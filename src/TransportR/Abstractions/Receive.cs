namespace TransportR.Abstractions;

/// <summary>
/// Handle for receiving items from a transfer.
/// </summary>
/// <typeparam name="T">The type of items to receive.</typeparam>
/// <remarks>
/// The handler given to <c>OnTransferStarted</c> owns the handle and must dispose it: until it does,
/// the session stays registered with its client, which is what keeps its outcome reportable.
/// <see cref="ReceiveAsync"/> itself does not return until the transfer has reached an outcome, so
/// disposing right after the loop is the correct, evident form; disposing before that — a <c>break</c>,
/// or a release from another task while the loop is still running — abandons the transfer, and every
/// wait on the handle, pending or later, then throws <see cref="TransferFailedException"/> rather than
/// reporting a completion that never happened. Disposing twice does nothing.
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
    /// Asynchronously enumerates all received items, ending only once the transfer has reached its
    /// outcome.
    /// </summary>
    /// <param name="cancellationToken">Token to cancel the receive operation.</param>
    /// <returns>An async enumerable of received items.</returns>
    /// <remarks>
    /// The last item is not the last word: the enumeration keeps running past it until the transfer is
    /// acknowledged and closed, which is what makes <c>await using (this) { await foreach (...) { } }</c>
    /// the correct form rather than one that quietly abandons the transfer. It throws
    /// <see cref="TransferFailedException"/> if the transfer fails or the handle is released while it is
    /// still under way, and also if the closing exchange itself never finishes, after every item has
    /// already been delivered — so an exception from here no longer means data is missing. Breaking out
    /// of the loop early releases the enumerator without waiting for any of this.
    /// </remarks>
    /// <exception cref="TransferFailedException">The transfer failed, or the handle was released before it reached an outcome.</exception>
    public abstract IAsyncEnumerable<T> ReceiveAsync(CancellationToken cancellationToken = default);
    /// <summary>
    /// Waits for the transfer to be fully completed (including shutdown handshake).
    /// </summary>
    /// <remarks>
    /// <see cref="ReceiveAsync"/> already waits for this on its own; calling this afterward returns at
    /// once. What this is for is awaiting it in parallel with <see cref="ReceiveAsync"/> — to wake a
    /// consumer blocked on its own output as soon as the transfer fails — or waiting on a handle that is
    /// never enumerated at all. It throws within a fraction of a second of the transfer failing, whatever
    /// the enumeration has not yet delivered. It returns once the sender has been acknowledged, which
    /// says nothing of what the consumer has taken: items already received stay in the enumeration, and
    /// a host that stops consuming when the wait returns loses them.
    /// </remarks>
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
