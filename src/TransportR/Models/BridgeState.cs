namespace TransportR.Models;

/// <summary>
/// Bridge state for lifecycle management.
/// Tracks connection status and domino completion sequence.
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
    
    // Domino Completion States (Hub only detects EOF on streams)
    
    /// <summary>Step 5: Sender POST ended with clean EOF.</summary>
    SenderPostCompleted,
    /// <summary>Step 6: Hub closed Receiver GET.</summary>
    ReceiverGetCompleted,
    /// <summary>Step 7: Receiver POST ended with clean EOF.</summary>
    ReceiverPostCompleted,
    /// <summary>Step 8: Hub closed Sender GET - Terminal success state.</summary>
    SenderGetCompleted,
    
    /// <summary>Bridge has been cancelled due to timeout or explicit cancellation.</summary>
    Cancelled,
    /// <summary>A rupture (network failure) was detected.</summary>
    Faulted
}

