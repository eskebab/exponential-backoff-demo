using Azure.Storage.Queues;
using ProducerApp;
using System.Text.Json;

Console.WriteLine("⚙️ Console Producer App started.");
Console.WriteLine("---------------------------------");

// This is the "magic string" to connect to the local Azurite emulator
const string connectionString = "UseDevelopmentStorage=true";
const string queueName = "myqueue-items";

// Get a client object for the queue
QueueClientOptions options = new QueueClientOptions
{
    MessageEncoding = QueueMessageEncoding.Base64
};
QueueClient queueClient = new QueueClient(connectionString, queueName, options);

// Create the queue if it doesn't already exist
await queueClient.CreateIfNotExistsAsync();
Console.WriteLine($"Queue '{queueName}' is ready.");
Console.WriteLine("Press 'Enter' to send a message, or 'q' to quit.");

int messageCount = 0;
while (true)
{
    ConsoleKeyInfo key = Console.ReadKey();
    if (key.Key == ConsoleKey.Q)
    {
        break;
    }

    if (key.Key == ConsoleKey.Enter)
    {
        messageCount++;
        string message = $"Message #{messageCount} from the console app at {DateTime.Now:O}";

        var messageDto = new MessageBodyWrapper
        {
            Message = message,
            DequeueCount = 0 // Initial dequeue count
        };

        // Use the same JSON options
        var jsonOptions = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            WriteIndented = false
        };

        // Send the message
        Console.WriteLine($"Sending: {message}");
        await queueClient.SendMessageAsync(JsonSerializer.Serialize(messageDto, jsonOptions));
        Console.WriteLine($"-> Sent: '{message}'");
    }
}

Console.WriteLine("\nProducer app shutting down.");