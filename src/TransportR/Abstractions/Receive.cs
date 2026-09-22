namespace TransportR.Abstractions;

/// <summary>
/// Handle for receiving items from a transfer.
/// </summary>
/// <typeparam name="T">The type of items to receive.</typeparam>
/// <remarks>
/// The handler given to <c>OnTransferStarted</c> owns the handle and must dispose it: until it does,
/// the session stays registered with its client, which is what keeps its outcome reportable.
/// Dispose it once <see cref="WaitForCompletionAsync"/> has returned, not before — disposing a
/// transfer that has not ended abandons it, and every wait on that handle, pending or later, then
/// throws <see cref="TransferFailedException"/> rather than reporting a completion that never
/// happened. Disposing twice does nothing.
/// </remarks>
public abstract class Receive<T> : Transfer, IAsyncDisposable
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
    /// <exception cref="TransferFailedException">The transfer ended without delivering its data.</exception>
    public virtual Task WaitForCompletionAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;

    /// <summary>
    /// Releases the transfer and everything the client holds on its behalf.
    /// </summary>
    public ValueTask DisposeAsync()
    {
        GC.SuppressFinalize(this);
        return DisposeAsyncCore();
    }

    /// <summary>
    /// Override to provide custom dispose logic.
    /// </summary>
    protected virtual ValueTask DisposeAsyncCore() => ValueTask.CompletedTask;
}
