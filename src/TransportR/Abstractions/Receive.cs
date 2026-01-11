namespace TransportR.Abstractions;

/// <summary>
/// Handle for receiving items from a transfer.
/// </summary>
/// <typeparam name="T">The type of items to receive.</typeparam>
public abstract class Receive<T> : Transfer
{
    /// <summary>
    /// Gets the batch size for this transfer.
    /// </summary>
    public int BatchSize { get; }
    
    /// <summary>
    /// Creates a new receive handle.
    /// </summary>
    /// <param name="transferId">The unique transfer identifier.</param>
    /// <param name="batchSize">The batch size for this transfer.</param>
    protected Receive(string transferId, int batchSize) : base(transferId) 
    {
        BatchSize = batchSize;
    }

    /// <summary>
    /// Asynchronously enumerates all received items.
    /// </summary>
    /// <param name="cancellationToken">Token to cancel the receive operation.</param>
    /// <returns>An async enumerable of received items.</returns>
    public abstract IAsyncEnumerable<T> ReceiveAsync(CancellationToken cancellationToken = default);
    /// <summary>
    /// Waits for the transfer to be fully completed (including shutdown handshake).
    /// </summary>
    public virtual Task WaitForCompletionAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;
}
