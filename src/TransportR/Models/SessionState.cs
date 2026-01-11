namespace TransportR.Models;

/// <summary>
/// Represents the high-level state of a DataSession.
/// Includes granular domino states for tracking the 8-step completion sequence.
/// </summary>
public enum SessionState
{
    /// <summary>
    /// Initial state before starting.
    /// </summary>
    Idle,
    
    /// <summary>
    /// Establishing initial connection/streams.
    /// </summary>
    Starting,
    
    /// <summary>
    /// Normal operation (Data + Acks flowing).
    /// </summary>
    Streaming,
    
    /// <summary>
    /// Stream broken or initial connection failed, attempting SignalR Probe + Handshake.
    /// </summary>
    Reconnecting,
    
    // ===== SENDER DOMINO STATES =====
    
    /// <summary>
    /// [Sender] Step 1: Sent Complete chunk, waiting for CompleteAck.
    /// </summary>
    SenderCompleteSent,
    
    /// <summary>
    /// [Sender] Step 4: Got CompleteAck from receiver.
    /// </summary>
    SenderAckReceived,
    
    /// <summary>
    /// [Sender] Step 5: Closing POST (Data) stream.
    /// </summary>
    SenderTerminatingDataPost,
    
    /// <summary>
    /// [Sender] Step 8: Waiting for GET (Control) to receive EOF from hub.
    /// </summary>
    SenderAwaitingControlGetEOF,
    
    // ===== RECEIVER DOMINO STATES =====
    
    /// <summary>
    /// [Receiver] Step 2: Got Complete chunk from sender.
    /// </summary>
    ReceiverCompleteReceived,
    
    /// <summary>
    /// [Receiver] Step 3: Sent CompleteAck.
    /// </summary>
    ReceiverAckSent,
    
    /// <summary>
    /// [Receiver] Step 6: Waiting for GET (Data) to receive EOF from hub.
    /// </summary>
    ReceiverAwaitingDataGetEOF,
    
    /// <summary>
    /// [Receiver] Step 7: Closing POST (Control) stream.
    /// </summary>
    ReceiverTerminatingControlPost,
    
    // ===== TERMINAL STATES =====
    
    /// <summary>
    /// Transfer successfully completed.
    /// </summary>
    Completed,
    
    /// <summary>
    /// Fatal error occurred, transfer aborted.
    /// </summary>
    Failed,
    
    /// <summary>
    /// Session disposed.
    /// </summary>
    Disposed
}

