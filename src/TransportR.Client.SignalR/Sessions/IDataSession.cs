namespace TransportR.Client.SignalR.Sessions;

/// <summary>
/// Interface for a data transfer session.
/// Encapsulates the state and logic for a single bidirectional data transfer.
/// </summary>
/// <typeparam name="T">The type of items being transferred.</typeparam>
public interface IDataSession<T> : IAsyncDisposable
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
    /// True if the transfer has completed.
    /// </summary>
    bool IsCompleted { get; }
    
    /// <summary>
    /// Number of items processed (sent or received).
    /// </summary>
    long ItemsProcessed { get; }
    
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
    /// Send a single item.
    /// </summary>
    Task SendAsync(T item, CancellationToken cancellationToken);
    
    /// <summary>
    /// Send multiple items.
    /// </summary>
    Task SendRangeAsync(IEnumerable<T> items, CancellationToken cancellationToken);
    
    /// <summary>
    /// Complete the send operation and wait for all ACKs.
    /// </summary>
    Task CompleteAsync(CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Receive items as an async enumerable.
    /// </summary>
    IAsyncEnumerable<T> ReceiveAsync(CancellationToken cancellationToken);
    
    /// <summary>
    /// Trigger a reconnection attempt.
    /// </summary>
    void TriggerReconnect();
    
    /// <summary>
    /// Called when a Pong is received from the hub.
    /// </summary>
    void OnPongReceived();


    /// <summary>
    /// Waits for the transfer to be fully completed (including shutdown handshake).
    /// </summary>
    Task WaitForCompletionAsync(CancellationToken cancellationToken);
}
