namespace TransportR.Client.SignalR.Configuration;

/// <summary>
/// Configuration options for TransportR client.
/// </summary>
public class ClientOptions
{
    /// <summary>
    /// Initial delay before retrying a failed connection. Default: 500ms.
    /// </summary>
    public TimeSpan InitialRetryDelay { get; set; } = TimeSpan.FromMilliseconds(500);

    /// <summary>
    /// Maximum retry delay (backoff ceiling). Default: 5 seconds.
    /// </summary>
    public TimeSpan MaxRetryDelay { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Timeout for Pong response during reconnection. Default: 10 seconds.
    /// </summary>
    public TimeSpan PongTimeout { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Buffer size for HTTP stream writes. Default: 512KB.
    /// </summary>
    public int StreamBufferSize { get; set; } = 512 * 1024;

    /// <summary>
    /// Interval for heartbeat reporting. Default: 1 second.
    /// </summary>
    public TimeSpan HeartbeatInterval { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Debounce duration for reconnection attempts. Default: 2 seconds.
    /// </summary>
    public TimeSpan ReconnectDebounce { get; set; } = TimeSpan.FromSeconds(2);

    /// <summary>
    /// Threshold before token expiry to trigger refresh. Default: 5 minutes.
    /// </summary>
    public TimeSpan TokenRefreshThreshold { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Timeout for batch accumulation in milliseconds. Default: 1000ms.
    /// </summary>
    public int BatchFlushIntervalMs { get; set; } = 1000;
}
