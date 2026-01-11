namespace TransportR.Hub.SignalR.Configuration;

/// <summary>
/// Configuration options for TransportR hub.
/// </summary>
public class HubOptions
{
    /// <summary>
    /// Interval for checking and cleaning up stale bridges. Default: 5 seconds.
    /// </summary>
    public TimeSpan BridgeCleanupInterval { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Delay before starting bridge cleanup on startup. Default: 10 seconds.
    /// </summary>
    public TimeSpan BridgeCleanupStartupDelay { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Maximum time a bridge can be idle before being cleaned up. Default: 5 minutes.
    /// </summary>
    public TimeSpan BridgeIdleTimeout { get; set; } = TimeSpan.FromMinutes(5);
}
