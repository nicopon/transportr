namespace TransportR.Models;

/// <summary>
/// Bridge state for lifecycle management.
/// Tracks which of the two participants currently hold a stream on the bridge.
/// </summary>
public enum BridgeState
{
    /// <summary>No one connected.</summary>
    Idle,
    /// <summary>Sender connected, waiting for receiver.</summary>
    WaitingForReceiver,
    /// <summary>Receiver connected, waiting for sender.</summary>
    WaitingForSender,
    /// <summary>Both sender and receiver are connected.</summary>
    Active,
    /// <summary>Bridge has been cancelled due to timeout or explicit cancellation.</summary>
    Cancelled
}
