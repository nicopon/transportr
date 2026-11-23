namespace TransportR.Client;

/// <summary>
/// Handle for sending items through a transfer.
/// </summary>
/// <typeparam name="T">The type of items to send.</typeparam>
public abstract class Send<T> : Transfer, IAsyncDisposable
{
    /// <summary>
    /// Gets the receiving client's identifier for this transfer.
    /// </summary>
    public Guid ReceiverId { get; }

    /// <summary>
    /// Creates a new send handle.
    /// </summary>
    /// <param name="transferId">The unique transfer identifier.</param>
    /// <param name="receiverId">The receiving client's identifier.</param>
    protected Send(string transferId, Guid receiverId = default) : base(transferId)
    {
        ReceiverId = receiverId;
    }

    /// <summary>
    /// Sends a single item through the transfer.
    /// </summary>
    /// <param name="item">The item to send.</param>
    /// <param name="cancellationToken">Token to cancel the send operation.</param>
    /// <exception cref="TransferFailedException">
    /// The transfer has ended without completing: the send window was full and the receiver
    /// acknowledged nothing for the client's acknowledgement timeout
    /// (<see cref="TransportRClientBuilder{T}.WithAcknowledgementTimeout"/>, 60 s by default), the
    /// peer or the hub ended it, or this handle was released. That holds for a call made after the
    /// ending as for one parked when it came. The handle is dead; dispose it.
    /// </exception>
    /// <exception cref="InvalidOperationException">
    /// <see cref="CompleteAsync"/> has been called: a completed transfer carries nothing more, and
    /// the item is not sent.
    /// </exception>
    public abstract Task SendAsync(T item, CancellationToken cancellationToken = default);

    /// <summary>
    /// Sends multiple items through the transfer.
    /// </summary>
    /// <param name="items">The items to send.</param>
    /// <param name="cancellationToken">Token to cancel the send operation.</param>
    public virtual async Task SendAsync(IEnumerable<T> items, CancellationToken cancellationToken = default)
    {
        foreach (var item in items)
        {
            await SendAsync(item, cancellationToken);
        }
    }

    /// <summary>
    /// Sends an asynchronous sequence of items through the transfer without buffering the whole sequence.
    /// </summary>
    /// <param name="items">The async sequence of items to send.</param>
    /// <param name="cancellationToken">Token to cancel the send operation.</param>
    public virtual async Task SendAsync(IAsyncEnumerable<T> items, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(items);

        await foreach (var item in items.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            await SendAsync(item, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Signals that all items have been sent and waits for the receiver's acknowledgment, which says the
    /// batches are in its buffer, not that its consumer has taken them.
    /// </summary>
    /// <param name="cancellationToken">Token to cancel the completion wait.</param>
    /// <remarks>
    /// Returning normally is the success signal, and the only one: there is no counter a caller can
    /// compare after the fact to learn whether the receiver got everything.
    /// </remarks>
    /// <exception cref="TransferFailedException">
    /// The receiver stopped acknowledging for the client's acknowledgement timeout
    /// (<see cref="TransportRClientBuilder{T}.WithAcknowledgementTimeout"/>, 60 s by default), or the
    /// session failed. The handle is dead; dispose it.
    /// </exception>
    public abstract Task CompleteAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Releases the transfer and everything the client holds on its behalf. Dispose once
    /// <see cref="CompleteAsync"/> has returned, not before — disposing a transfer that has not
    /// ended abandons it, and <see cref="CompleteAsync"/> then throws
    /// <see cref="TransferFailedException"/> instead of waiting on a session that is gone.
    /// Disposing twice does nothing.
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
