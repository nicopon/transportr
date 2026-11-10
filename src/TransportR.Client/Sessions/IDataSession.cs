using TransportR.Abstractions;
using TransportR.Models;

namespace TransportR.Client.Sessions;

/// <summary>
/// Interface for a data transfer session.
/// Encapsulates the state and logic for a single bidirectional data transfer, whatever it carries.
/// </summary>
internal interface IDataSession : IAsyncDisposable
{
    /// <summary>
    /// The unique transfer identifier.
    /// </summary>
    string TransferId { get; }

    /// <summary>
    /// The batch size for this session.
    /// </summary>
    int BatchSize { get; }

    /// <summary>
    /// The data plane URL for HTTP streams.
    /// </summary>
    string DataUrl { get; }

    /// <summary>
    /// True if this session is sending data, false if receiving.
    /// </summary>
    bool IsSender { get; }

    /// <summary>
    /// Where the session currently stands. Terminal states are
    /// <see cref="SessionState.Completed"/> and <see cref="SessionState.Failed"/>; anything else
    /// means the transfer is still under way.
    /// </summary>
    SessionState State { get; }

    /// <summary>What carries this session's write stream; null before it first opens.</summary>
    WriteTransport? WriteTransport { get; }

    /// <summary>Why this session does not use a WebSocket, once it has given up on one.</summary>
    string? WriteTransportFallback { get; }


    /// <summary>
    /// Number of items processed (sent or received).
    /// </summary>
    long ItemsProcessed { get; }

    /// <summary>
    /// Serialized payload bytes that crossed the wire, resends included.
    /// </summary>
    long BytesTransferred { get; }

    /// <summary>
    /// Number of ACKs received (sender) or sent (receiver).
    /// </summary>
    long AcksReceived { get; }

    /// <summary>
    /// Number of ACKs sent by the receiver.
    /// </summary>
    long AcksSent { get; }

    /// <summary>
    /// When the session was started.
    /// </summary>
    DateTime StartedAt { get; }

    /// <summary>
    /// Start the session as a sender.
    /// </summary>
    Task StartSenderAsync();

    /// <summary>
    /// Start the session as a receiver.
    /// </summary>
    Task StartReceiverAsync();

    /// <summary>
    /// What a send made once <see cref="CompleteAsync"/> has begun is answered with, or null when the
    /// send may go on. Asked before the send reaches the source, which would drop it without a word.
    /// </summary>
    Task? RefusedAsCompleted();

    /// <summary>
    /// Stands for a send already made while the transfer is under way: a send that cannot finish
    /// because the transfer failed ends in the <see cref="TransferFailedException"/>, not in what
    /// the source met.
    /// </summary>
    Task WhileSending(Task send, CancellationToken cancellationToken);

    /// <summary>
    /// Complete the send operation and wait for all ACKs.
    /// </summary>
    Task CompleteAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Trigger a reconnection attempt.
    /// </summary>
    void TriggerReconnect();


    /// <summary>
    /// Waits for the transfer to be fully completed (including shutdown handshake).
    /// </summary>
    Task WaitForCompletionAsync(CancellationToken cancellationToken);

    /// <summary>
    /// The hub cancelled the transfer: fails it with the hub as the reason, which releases whoever is
    /// parked on it, then releases the session. Not <see cref="IAsyncDisposable.DisposeAsync"/>, which
    /// is the host letting go and tells the peer so.
    /// </summary>
    Task EndByHubAsync();
}
