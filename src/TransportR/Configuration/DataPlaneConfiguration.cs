namespace TransportR.Configuration;

/// <summary>
/// Configuration for data plane timeouts and behavior.
/// </summary>
public class DataPlaneConfiguration
{
    /// <summary>
    /// Timeout in seconds for inactive bridges waiting for reconnection.
    /// After this timeout, the bridge is cancelled and CancelTransfer is sent to both parties.
    /// Default: 60 seconds.
    /// </summary>
    public int BridgeTimeoutSeconds { get; set; } = 300;
}
