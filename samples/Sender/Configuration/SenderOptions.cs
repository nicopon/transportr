using TransportR.Models;

namespace Sender.Configuration;

public class SenderOptions
{
    public string SourceFilePath { get; set; } = "data/orders.bin";
    public string HubUrl { get; set; } = "http://localhost:5252";
    public int BatchSize { get; set; } = 5000;
    public int BatchTimeoutMs { get; set; } = 500;

    /// <summary>
    /// Caps how many batches may be in flight before the producer is made to wait.
    /// Null (the default) keeps the historical unbounded behaviour.
    /// </summary>
    public int? MaxInFlightBatches { get; set; }

    /// <summary>
    /// What may carry the write stream. Null keeps the client default.
    /// </summary>
    public WriteTransportPolicy? WriteTransport { get; set; }

    /// <summary>
    /// Stops the transfer after this many items read from the source. Null sends the whole file.
    /// </summary>
    public int? MaxItems { get; set; }

    /// <summary>
    /// Selects the byte-stream mode and sets the size, in bytes, at which a batch is cut: the source file
    /// is copied as it is, with no serializer and no item. Null keeps the item mode.
    /// </summary>
    public int? ByteStreamBatchBytes { get; set; }

    /// <summary>
    /// Seconds between the opening of one transfer and the next, when several run in parallel. Zero
    /// (the default) opens them all at once.
    /// </summary>
    public int TransferStaggerSeconds { get; set; }

    public bool EnableAuth { get; set; } = false;
    public string OutputDirectory { get; set; } = "output";
    public string Group { get; set; } = "senders";
    public string? AccessToken { get; set; } = null;
    
    /// <summary>
    /// Auth configuration for automatic token acquisition.
    /// </summary>
    public AuthOptions? Auth { get; set; }
}

public class AuthOptions
{
    public string TokenEndpoint { get; set; } = string.Empty;
    public string ClientId { get; set; } = string.Empty;
    public string ClientSecret { get; set; } = string.Empty;
    public string? Scope { get; set; }
    
    public bool IsConfigured => 
        !string.IsNullOrEmpty(TokenEndpoint) && 
        !string.IsNullOrEmpty(ClientId) && 
        !string.IsNullOrEmpty(ClientSecret);
}
