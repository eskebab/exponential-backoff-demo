using Azure.Storage.Queues;
using System;
using System.Threading.Tasks;

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

        // Send the message
        Console.WriteLine($"Sending: {message}");
        await queueClient.SendMessageAsync(message);
        Console.WriteLine($"-> Sent: '{message}'");
    }
}

Console.WriteLine("\nProducer app shutting down.");