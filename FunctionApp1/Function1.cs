using Azure.Storage.Queues.Models;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace FunctionApp1
{
    public class Function1
    {
        private readonly ILogger<Function1> _logger;

        public Function1(ILogger<Function1> logger)
        {
            _logger = logger;
        }
        
        [Function(nameof(Function1))]
        public void Run([QueueTrigger("myqueue-items", Connection = "AzureWebJobsStorage")] QueueMessage queueMessage)
        {
            try
            {
                string messageText = queueMessage.MessageText;
                _logger.LogInformation("⚡️ CONSUMER TRIGGERED! Received message: '{message}'", messageText);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing queue message");
                throw;
            }
        }

        [Function("test")]
        public void RunPoison([QueueTrigger("myqueue-items-poison", Connection = "AzureWebJobsStorage")] QueueMessage queueMessage)
        {
            _logger.LogInformation("⚡️ CONSUMER TRIGGERED! Received message: '{message}'", queueMessage.MessageText);
        }
    }
}
