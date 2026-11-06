using TransportR.Models;

namespace Receiver.Configuration;

public class ReceiverOptions
{
    public string HubUrl { get; set; } = "http://localhost:5252";
    public string OutputDirectory { get; set; } = "output";
    public bool EnableAuth { get; set; } = false;
    public int BatchSize { get; set; } = 5000;
    public string Group { get; set; } = "receivers";
    public string? AccessToken { get; set; } = null;
    
    /// <summary>
    /// Overrides the receive buffer's item bound. Null keeps the client default.
    /// </summary>
    public int? ReceiveMaxItems { get; set; }

    /// <summary>
    /// Overrides the receive buffer's wire-byte bound. Null keeps the client default.
    /// </summary>
    public long? ReceiveMaxBytes { get; set; }

    /// <summary>
    /// What may carry the write stream (the acknowledgements). Null keeps the client default.
    /// </summary>
    public WriteTransportPolicy? WriteTransport { get; set; }

    /// <summary>
    /// Caps how fast items are consumed, standing in for a consumer that cannot keep up. Zero
    /// (the default) consumes as fast as it can.
    /// </summary>
    /// <remarks>
    /// Throttling the consumer rather than the source is the whole point: a slow source proves
    /// nothing about what the receive buffer holds, because the buffer never fills.
    /// </remarks>
    public int ConsumeItemsPerSecond { get; set; } = 0;

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
