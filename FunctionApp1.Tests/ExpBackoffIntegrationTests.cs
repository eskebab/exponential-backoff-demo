using Azure.Storage.Queues;
using Azure.Storage.Queues.Models;
using ProducerApp;
using Microsoft.Extensions.Logging;
using Moq;
using System.Text.Json;

namespace FunctionApp1.Tests;

public class ExpBackoffIntegrationTests : IAsyncLifetime
{
    private const string ConnectionString = "UseDevelopmentStorage=true";
    private const string QueueName = "test-myqueue-items";
    private const string PoisonQueueName = "test-myqueue-items-poison";

    private QueueClient _queueClient;
    private QueueClient _poisonQueueClient;
    private readonly JsonSerializerOptions _jsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = false
    };

    public async Task InitializeAsync()
    {
        // Set up test queues
        var options = new QueueClientOptions
        {
            MessageEncoding = QueueMessageEncoding.Base64
        };

        _queueClient = new QueueClient(ConnectionString, QueueName, options);
        _poisonQueueClient = new QueueClient(ConnectionString, PoisonQueueName, options);

        // Ensure queues exist and are empty
        await _queueClient.CreateIfNotExistsAsync();
        await _poisonQueueClient.CreateIfNotExistsAsync();

        await ClearQueue(_queueClient);
        await ClearQueue(_poisonQueueClient);
    }

    public async Task DisposeAsync()
    {
        // Clean up
        await _queueClient.DeleteIfExistsAsync();
        await _poisonQueueClient.DeleteIfExistsAsync();
    }

    [Fact]
    public async Task Message_Should_Be_Requeued_With_Incremented_DequeueCount()
    {
        // Arrange
        var logger = new Mock<ILogger<ExpBackoffDemo>>();
        var demo = new TestableExpBackoffDemo(logger.Object, QueueName, PoisonQueueName);

        var initialMessage = new MessageBodyWrapper
        {
            Message = "Test message",
            DequeueCount = 0
        };

        // Act: Send message and process it (will fail due to 100% failure rate)
        await SendMessage(initialMessage);

        // Get the message and process it
        QueueMessage? queueMessage = await GetMessage();
        Assert.NotNull(queueMessage);

        string messageContent = queueMessage.Body.ToString();
        await demo.TestProcessMessage(messageContent);

        // Assert: Check that message was requeued with incremented count
        await Task.Delay(2000); // Wait for visibility timeout
        queueMessage = await GetMessage();
        Assert.NotNull(queueMessage);

        var requeuedMessage = JsonSerializer.Deserialize<MessageBodyWrapper>(
            queueMessage.Body.ToString(), _jsonOptions);

        Assert.NotNull(requeuedMessage);
        Assert.Equal(1, requeuedMessage.DequeueCount);
        Assert.Equal("Test message", requeuedMessage.Message);
    }

    [Fact]
    public async Task Message_Exceeding_Max_Retries_Should_Go_To_Poison_Queue()
    {
        // Arrange
        var logger = new Mock<ILogger<ExpBackoffDemo>>();
        var demo = new TestableExpBackoffDemo(logger.Object, QueueName, PoisonQueueName);

        var initialMessage = new MessageBodyWrapper
        {
            Message = "Max retry test",
            DequeueCount = 12 // right on the edge of max retries
        };

        // Act: Send message and process it (will fail and be moved to poison)
        await SendMessage(initialMessage);

        QueueMessage? queueMessage = await GetMessage();
        Assert.NotNull(queueMessage);

        string messageContent = queueMessage.Body.ToString();
        await demo.TestProcessMessage(messageContent);

        // Assert: Check that message was moved to poison queue
        await Task.Delay(2000);

        // Original queue should be empty
        queueMessage = await GetMessage();
        Assert.Null(queueMessage);

        // Message should be in poison queue
        QueueMessage? poisonMessage = await GetPoisonMessage();
        Assert.NotNull(poisonMessage);

        var poisonMessageDto = JsonSerializer.Deserialize<MessageBodyWrapper>(
            poisonMessage.Body.ToString(), _jsonOptions);

        Assert.NotNull(poisonMessageDto);
        Assert.Equal(13, poisonMessageDto.DequeueCount);
        Assert.Equal("Max retry test", poisonMessageDto.Message);
    }

    [Fact]
    public async Task Backoff_Time_Should_Increase_With_Retry_Count()
    {
        // This test verifies visibility timeout increases
        // For this we need to check when messages become visible

        var logger = new Mock<ILogger<ExpBackoffDemo>>();
        var demo = new TestableExpBackoffDemo(logger.Object, QueueName, PoisonQueueName);

        // Send messages with different dequeue counts
        await SendMessage(new MessageBodyWrapper { Message = "Retry 1", DequeueCount = 1 });
        await SendMessage(new MessageBodyWrapper { Message = "Retry 5", DequeueCount = 5 });

        // Get and process first message (retry 1 -> should be visible in ~1 second)
        var msg1 = await GetMessage();
        await demo.TestProcessMessage(msg1.Body.ToString());

        // Get and process second message (retry 5 -> should be visible in ~5 seconds)
        var msg2 = await GetMessage();
        await demo.TestProcessMessage(msg2.Body.ToString());

        // First message should be visible before second one
        await Task.Delay(2000); // Wait for first message visibility timeout
        var firstVisible = await GetMessage();
        Assert.NotNull(firstVisible);
        var firstDto = JsonSerializer.Deserialize<MessageBodyWrapper>(
            firstVisible.Body.ToString(), _jsonOptions);
        Assert.Equal(2, firstDto.DequeueCount); // Now at retry 2

        // Put it back
        //await demo.TestProcessMessage(firstVisible.Body.ToString());

        // Second message should still not be visible yet
        await Task.Delay(1000);
        var msgCheck = await GetMessage();
        Assert.Null(msgCheck); // No messages visible yet

        // Wait longer for second message
        await Task.Delay(3000);
        var secondVisible = await GetMessage();
        Assert.NotNull(secondVisible);
        var secondDto = JsonSerializer.Deserialize<MessageBodyWrapper>(
            secondVisible.Body.ToString(), _jsonOptions);
        Assert.Equal(6, secondDto.DequeueCount); // Now at retry 6
    }

    [Fact]
    public async Task CorrelationId_Should_Persist_Across_Multiple_Retries()
    {
        // Arrange
        var logger = new Mock<ILogger<ExpBackoffDemo>>();
        var demo = new TestableExpBackoffDemo(logger.Object, QueueName, PoisonQueueName);
        
        // Create message with specific CorrelationId
        string testCorrelationId = "test-correlation-" + Guid.NewGuid().ToString();
        var initialMessage = new MessageBodyWrapper
        {
            Message = "Correlation test message",
            DequeueCount = 0,
            CorrelationId = testCorrelationId,
            FirstProcessedAt = DateTime.UtcNow
        };
        
        // Act: Send initial message
        await SendMessage(initialMessage);
        
        // Process and track the message through multiple retry cycles
        var observedDequeueCounts = new List<int>();
        var observedCorrelationIds = new List<string>();
        
        // Track message through 3 retry cycles
        for (int i = 0; i < 3; i++)
        {
            // Wait for message to become visible
            await Task.Delay(i == 0 ? 500 : 3000);
            
            // Get the message
            QueueMessage? queueMessage = await GetMessage();
            Assert.NotNull(queueMessage);
            
            // Extract and record details
            var messageDto = JsonSerializer.Deserialize<MessageBodyWrapper>(
                queueMessage.Body.ToString(), _jsonOptions);
            
            Assert.NotNull(messageDto);
            observedDequeueCounts.Add(messageDto.DequeueCount);
            observedCorrelationIds.Add(messageDto.CorrelationId ?? string.Empty);
            
            // Process message (will fail and requeue)
            await demo.TestProcessMessage(queueMessage.Body.ToString());
        }
        
        // Assert
        
        // 1. Verify DequeueCount increments with each retry
        // First should be 0, then 1, then 2
        Assert.Equal(0, observedDequeueCounts[0]);
        Assert.Equal(1, observedDequeueCounts[1]);
        Assert.Equal(2, observedDequeueCounts[2]);
        
        // 2. Verify CorrelationId remains unchanged across all retries
        Assert.All(observedCorrelationIds, id => Assert.Equal(testCorrelationId, id));
        
        // 3. Verify FirstProcessedAt timestamp is preserved
        // Get one more message to check
        await Task.Delay(3000);
        var finalMessage = await GetMessage();
        Assert.NotNull(finalMessage);
        
        var finalMessageDto = JsonSerializer.Deserialize<MessageBodyWrapper>(
            finalMessage.Body.ToString(), _jsonOptions);
        Assert.NotNull(finalMessageDto);
        
        // FirstProcessedAt should be close to our original timestamp
        Assert.Equal(
            initialMessage.FirstProcessedAt.ToUniversalTime().ToString("yyyy-MM-dd HH:mm"),
            finalMessageDto.FirstProcessedAt.ToUniversalTime().ToString("yyyy-MM-dd HH:mm")
        );
    }

    private async Task SendMessage(MessageBodyWrapper message)
    {
        string serialized = JsonSerializer.Serialize(message, _jsonOptions);
        await _queueClient.SendMessageAsync(serialized);
    }

    private async Task<QueueMessage?> GetMessage()
    {
        QueueMessage[] messages = await _queueClient.ReceiveMessagesAsync(1);
        return messages.Length > 0 ? messages[0] : null;
    }

    private async Task<QueueMessage?> GetPoisonMessage()
    {
        QueueMessage[] messages = await _poisonQueueClient.ReceiveMessagesAsync(1);
        return messages.Length > 0 ? messages[0] : null;
    }

    private static async Task ClearQueue(QueueClient queueClient)
    {
        while (true)
        {
            var messages = await queueClient.ReceiveMessagesAsync(32);
            if (messages.Value.Length == 0)
                break;

            foreach (var message in messages.Value)
            {
                await queueClient.DeleteMessageAsync(message.MessageId, message.PopReceipt);
            }
        }
    }
}

// Test-friendly version of the ExpBackoffDemo class
public class TestableExpBackoffDemo : ExpBackoffDemo
{
    public TestableExpBackoffDemo(
        ILogger<ExpBackoffDemo> logger,
        string queueName = "myqueue-items",
        string poisonQueueName = "myqueue-items-poison")
        : base(logger, queueName, poisonQueueName, 12)
    {
    }

    // Expose method to process a message directly
    public async Task TestProcessMessage(string message)
    {
        await Run(message);
    }

    public override TimeSpan GetVisibilityTimeout(int deQueueCount)
    {
        var visibilityTimeout = deQueueCount switch
        {
            1 => TimeSpan.FromSeconds(1),
            2 => TimeSpan.FromSeconds(2),
            3 => TimeSpan.FromSeconds(3),
            4 => TimeSpan.FromSeconds(4),
            5 => TimeSpan.FromSeconds(5),
            6 => TimeSpan.FromSeconds(6),
            7 => TimeSpan.FromSeconds(7),
            8 => TimeSpan.FromSeconds(8),
            9 => TimeSpan.FromSeconds(9),
            10 => TimeSpan.FromSeconds(10),
            11 => TimeSpan.FromSeconds(11),
            _ => TimeSpan.FromSeconds(12),
        };

        return visibilityTimeout;
    }
}
