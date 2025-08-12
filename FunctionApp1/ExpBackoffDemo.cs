using Azure.Storage.Queues;
using ProducerApp;
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
    private readonly int _maxDequeueCount = 12; // Maximum number of times a message can be dequeued
    const string connectionString = "UseDevelopmentStorage=true";

    private static readonly JsonSerializerOptions _jsonOptions = new JsonSerializerOptions
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = false
    };

    // Modify constructor to accept queue names for testing
    public ExpBackoffDemo(
        ILogger<ExpBackoffDemo> logger,
        string queueName = "myqueue-items",
        string poisonQueueName = "myqueue-items-poison",
        int maxRetryCount = 12)
    {
        _logger = logger;
        _maxDequeueCount = maxRetryCount;

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
        MessageBodyWrapper? messageDto = null;

        try
        {
            messageDto = JsonSerializer.Deserialize<MessageBodyWrapper>(message, _jsonOptions);
            _logger.LogInformation("Dequeue Count: {dequeueCount}", messageDto?.DequeueCount);

            if (messageDto == null)
            {
                throw new JsonException("Failed to deserialize message to MessageDto");
            }

            ProcessMessageWithRandomFailures(messageDto);
        }
        catch (Exception ex)
        {
            if (messageDto != null)
            {
                await RequeueWithBackoff(messageDto, ex);
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

    private void ProcessMessageWithRandomFailures(MessageBodyWrapper messageDto)
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

    private async Task RequeueWithBackoff(MessageBodyWrapper message, Exception exception)
    {
        // Don't retry on certain exceptions
        if (exception is JsonException || exception is ArgumentException)
        {
            _logger.LogWarning("Permanent error detected - sending to poison queue immediately");
            await SendToPoisonQueue(message);
            return;
        }

        // For transient errors, apply backoff strategy
        var newMessage = message with
        {
            DequeueCount = message.DequeueCount + 1 // Increment the dequeue count
        };

        if (newMessage.DequeueCount > _maxDequeueCount)
        {
            // If the message has been dequeued too many times, send it to the poison queue
            _logger.LogWarning("Message '{message}' has exceeded maximum dequeue count. Sending to poison queue.", newMessage.Message);
            await SendToPoisonQueue(newMessage);
            return;
        }
        else
        {
            // Serialize the metadata and original message
            string newMessageSerialized = JsonSerializer.Serialize(newMessage, _jsonOptions);

            // Add the message back to the queue with visibility timeout
            await _queueClient.SendMessageAsync(newMessageSerialized,
                GetVisibilityTimeout(newMessage.DequeueCount), // Visibility timeout (delay before processing)
                TimeSpan.FromDays(7)); // Time-to-live
        }
    }

    private async Task SendToPoisonQueue(MessageBodyWrapper message)
    {
        // Serialize the message for the poison queue
        string messageSerialized = JsonSerializer.Serialize(message, _jsonOptions);
        await _poisonQueueClient.SendMessageAsync(messageSerialized, TimeSpan.FromSeconds(0), TimeSpan.FromDays(7));
    }

    public virtual TimeSpan GetVisibilityTimeout(int deQueueCount)
    {
        var visibilityTimeout = deQueueCount switch
        {
            1 => TimeSpan.FromSeconds(10),
            2 => TimeSpan.FromSeconds(30),
            3 => TimeSpan.FromMinutes(1),
            4 => TimeSpan.FromMinutes(5),
            5 => TimeSpan.FromMinutes(10),
            6 => TimeSpan.FromMinutes(30),
            7 => TimeSpan.FromHours(1),
            8 => TimeSpan.FromHours(3),
            9 => TimeSpan.FromHours(6),
            10 => TimeSpan.FromHours(12),
            11 => TimeSpan.FromHours(12),
            _ => TimeSpan.FromHours(12),
        };

        return visibilityTimeout;
    }
}