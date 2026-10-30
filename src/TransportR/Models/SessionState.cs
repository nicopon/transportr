namespace TransportR.Models;

/// <summary>
/// Represents the high-level state of a DataSession.
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

    /// <summary>
    /// [Receiver] Accepted Complete and queued its acknowledgement: every batch is in, and what is
    /// left is the hub reading that acknowledgement. A sender has no such state; it goes from
    /// <see cref="Streaming"/> to <see cref="Completed"/> on the acknowledgement.
    /// </summary>
    ReceiverCompleteReceived,

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
