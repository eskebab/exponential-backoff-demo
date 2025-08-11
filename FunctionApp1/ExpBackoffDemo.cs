using Azure.Storage.Queues;
using ConsoleApp1;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using System.Text.Json;

namespace FunctionApp1;

public class ExpBackoffDemo
{
    private readonly ILogger<ExpBackoffDemo> _logger;
    private readonly Random _random = new();
    private readonly QueueClient _queueClient;
    private readonly QueueClient _poisonQueueClient;
    const string connectionString = "UseDevelopmentStorage=true";
    const string queueName = "myqueue-items";
    const string poisonQueueName = "myqueue-items-poison";
    const int maxDequeueCount = 12; // Maximum number of times a message can be dequeued

    private static readonly JsonSerializerOptions _jsonOptions = new JsonSerializerOptions
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = false
    };

    public ExpBackoffDemo(ILogger<ExpBackoffDemo> logger)
    {
        _logger = logger;

        // Initialize the queue client with explicit Base64 encoding
        QueueClientOptions options = new()
        {
            MessageEncoding = QueueMessageEncoding.Base64
        };
        _queueClient = new(connectionString, queueName, options);
        _poisonQueueClient = new(connectionString, poisonQueueName, options);
    }

    [Function(nameof(ExpBackoffDemo))]
    public async Task Run([QueueTrigger("myqueue-items", Connection = "AzureWebJobsStorage")] string message)
    {
        // Declare messageDto outside the try block so it's accessible in the catch block
        MessageDto? messageDto = null;

        try
        {
            messageDto = JsonSerializer.Deserialize<MessageDto>(message, _jsonOptions);
            _logger.LogInformation("Dequeue Count: {dequeueCount}", messageDto?.DequeueCount);

            ProcessMessageWithRandomFailures(messageDto);
        }
        catch (Exception ex)
        {
            if (messageDto != null)
            {
                await RequeueWithBackoff(messageDto);
            }
            else
            {
                // Handle case where deserialization failed
                _logger.LogError("Failed to deserialize message: {message}", message);
                _logger.LogError(ex, "Error processing queue message");
            }

        }
    }

    [Function("Poison-queue")]
    public void RunPoison([QueueTrigger("myqueue-items-poison", Connection = "AzureWebJobsStorage")] string queueMessage)
    {
        _logger.LogInformation("⚡️ CONSUMER TRIGGERED! Received message: '{message}'", queueMessage);
    }

    private void ProcessMessageWithRandomFailures(MessageDto messageDto)
    {
        // Generate a random number between 0-9
        int randomValue = _random.Next(10);

        // If the value is 0 or bigger (100% chance), throw an exception
        if (randomValue >= 0)
        {
            _logger.LogWarning("🎲 Random failure triggered (10/10 chance)");
            throw new Exception($"Simulated random failure processing message: '{messageDto.Message}'");
        }

        // Otherwise, do nothing (successful processing)
        _logger.LogInformation("🎲 Random success (0/10 chance)");
    }

    private async Task RequeueWithBackoff(MessageDto message)
    {
        var newMessage = message with
        {
            DequeueCount = message.DequeueCount + 1 // Increment the dequeue count
        };

        // Serialize the metadata and original message
        string newMessageSerialized = JsonSerializer.Serialize(newMessage, _jsonOptions);

        if (newMessage.DequeueCount > maxDequeueCount)
        {
            // If the message has been dequeued too many times, send it to the poison queue
            _logger.LogWarning("Message '{message}' has exceeded maximum dequeue count. Sending to poison queue.", newMessage.Message);
            await _poisonQueueClient.SendMessageAsync(newMessageSerialized, TimeSpan.FromSeconds(0), TimeSpan.FromDays(7));
            return;
        }
        else
        {
            // Add the message back to the queue with visibility timeout
            await _queueClient.SendMessageAsync(newMessageSerialized,
                GetVisibilityTimeout(newMessage.DequeueCount), // Visibility timeout (delay before processing)
                TimeSpan.FromDays(7)); // Time-to-live
        }
    }

    private static TimeSpan GetVisibilityTimeout(int deQueueCount)
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