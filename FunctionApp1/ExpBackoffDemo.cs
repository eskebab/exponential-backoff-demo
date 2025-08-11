using Azure.Storage.Queues;
using ConsoleApp1;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using System.Text.Json;

namespace FunctionApp1;

public class ExpBackoffDemo
{
    private readonly ILogger<ExpBackoffDemo> _logger;
    private readonly Random _random = new Random();
    const string connectionString = "UseDevelopmentStorage=true";
    const string queueName = "myqueue-items";

    public ExpBackoffDemo(ILogger<ExpBackoffDemo> logger)
    {
        _logger = logger;
    }

    [Function(nameof(ExpBackoffDemo))]
    public async Task Run([QueueTrigger("myqueue-items", Connection = "AzureWebJobsStorage")] string message)
    {
        // Declare messageDto outside the try block so it's accessible in the catch block
        MessageDto? messageDto = null;
        
        try
        {
            messageDto = JsonSerializer.Deserialize<MessageDto>(message);
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

    private static async Task RequeueWithBackoff(MessageDto message)
    {
        // Create a queue client with explicit Base64 encoding
        QueueClientOptions options = new QueueClientOptions
        {
            MessageEncoding = QueueMessageEncoding.Base64
        };
        QueueClient queueClient = new(connectionString, queueName, options);

        var newMessage = message with
        {
            DequeueCount = message.DequeueCount + 1 // Increment the dequeue count
        };

        // Serialize the metadata and original message
        string newMessageSerialized = JsonSerializer.Serialize(newMessage);

        // Add the message back to the queue with visibility timeout
        await queueClient.SendMessageAsync(newMessageSerialized,
            GetVisibilityTimeout(newMessage.DequeueCount), // Visibility timeout (delay before processing)
            TimeSpan.FromDays(7)); // Time-to-live
    }

    private static TimeSpan GetVisibilityTimeout(int deQueueCount)
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