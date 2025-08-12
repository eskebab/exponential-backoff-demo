namespace ProducerApp;

public record MessageBodyWrapper
{
    public required string Message { get; set; }
    public int DequeueCount { get; set; }
    public string? CorrelationId { get; set; } = Guid.NewGuid().ToString();
    public DateTime FirstProcessedAt { get; set; } = DateTime.UtcNow;
}
