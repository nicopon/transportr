namespace TransportR.Configuration;

/// <summary>
/// Configuration for data plane timeouts and behavior.
/// </summary>
public class DataPlaneConfiguration
{
    /// <summary>
    /// Timeout in seconds for inactive bridges waiting for reconnection.
    /// After this timeout the bridge is cancelled and its transfer ends: the hub drops the
    /// transfer's state and bridges and tells each participant still registered to cancel.
    /// Default: 300 seconds. Zero or less is refused at startup.
    /// </summary>
    /// <remarks>
    /// This governs <c>BridgeCleanupService</c> only, and only for bridges that are NOT
    /// <see cref="Models.BridgeState.Active"/> — an active bridge is left alone whatever its idle
    /// time. A transfer that stops making progress is a separate concern, handled by
    /// <c>TransferCleanupService</c> against its own thresholds.
    /// </remarks>
    public int BridgeTimeoutSeconds { get; set; } = 300;

    /// <summary>
    /// Timeout in seconds before a transfer showing no activity at all is terminated.
    /// Default: 60 seconds. Zero or less is refused at startup.
    /// </summary>
    /// <remarks>
    /// Activity means anything that reaches the hub for this transfer: heartbeats, Ping
    /// keep-alives during reconnection, and the bytes that pass on either of its streams, counted
    /// at the hub and never read (<c>CountingStream</c>). A peer that is connected but making no
    /// progress keeps refreshing this, on purpose — it is what stops a network glitch from killing
    /// a healthy transfer. Use <see cref="StalledProgressTimeoutSeconds"/> to bound that case as
    /// well.
    /// </remarks>
    public int TransferInactivityTimeoutSeconds { get; set; } = 60;

    /// <summary>
    /// Optional timeout in seconds before a transfer that is alive but not advancing is
    /// terminated. <c>null</c> (the default) disables the check.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Progress means <c>ItemsSent</c>, <c>AcksReceived</c>, <c>ItemsReceived</c> or
    /// <c>AcksSent</c> moving forward. Without this, a deadlocked-but-connected peer is never
    /// terminated: its heartbeat keeps the inactivity check happy indefinitely.
    /// </para>
    /// <para>
    /// <b>Opt-in, and deliberately so.</b> No progress is also what a reconnection looks like from
    /// the hub, so a value at or below <see cref="BridgeTimeoutSeconds"/> would kill transfers that
    /// would have recovered, and is refused at startup. Set it comfortably above the longest outage
    /// the deployment intends to survive; if you cannot name that number, leave it off.
    /// </para>
    /// </remarks>
    public int? StalledProgressTimeoutSeconds { get; set; }
}
