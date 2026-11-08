namespace TransportR.Abstractions;

/// <summary>
/// Handle for receiving the stream of bytes of a transfer.
/// </summary>
/// <remarks>
/// <para>
/// The handler given to <c>OnTransferStarted</c> owns the handle and must dispose it: until it does,
/// the session stays registered with its client, which is what keeps its outcome reportable. A read
/// that returns 0 and a <see cref="CopyToAsync"/> that returns both come only once the transfer has
/// reached its outcome, so disposing right after them is the correct form; disposing before that
/// abandons the transfer, and every wait on the handle, pending or later, then throws
/// <see cref="TransferFailedException"/>. Disposing twice does nothing.
/// </para>
/// <para>
/// One consumer at a time: <see cref="ReadAsync"/> and <see cref="CopyToAsync"/> take turns, and a call
/// made while another is under way throws <see cref="InvalidOperationException"/>.
/// </para>
/// </remarks>
public abstract class ByteReceive : Transfer, IAsyncDisposable
{
    /// <summary>
    /// Creates a new receive handle.
    /// </summary>
    /// <param name="transferId">The unique transfer identifier.</param>
    protected ByteReceive(string transferId) : base(transferId) { }

    /// <summary>
    /// Reads up to <paramref name="buffer"/>'s length of the bytes the transfer carries, waiting until
    /// at least one is there.
    /// </summary>
    /// <param name="buffer">Where the bytes go. Not empty: a result of 0 means the transfer is over.</param>
    /// <param name="cancellationToken">Token to cancel the wait; no byte is lost to it.</param>
    /// <returns>
    /// The number of bytes read, or 0 once every byte has been read and the transfer has reached its
    /// outcome: the acknowledgement of the last batch may still be on its way when the last byte
    /// arrives, and a caller that stops there can abandon a transfer it in fact received in full.
    /// </returns>
    /// <exception cref="ArgumentException"><paramref name="buffer"/> is empty.</exception>
    /// <exception cref="InvalidOperationException">Another read or copy of this handle is under way.</exception>
    /// <exception cref="TransferFailedException">
    /// The transfer failed, or the handle was released before it reached its outcome. Bytes admitted
    /// before the failure are still returned first.
    /// </exception>
    public abstract ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default);

    /// <summary>
    /// Writes what the transfer carries to <paramref name="destination"/> until the transfer has ended,
    /// and returns without disposing the handle.
    /// </summary>
    /// <param name="destination">The stream to write. It is not closed.</param>
    /// <param name="cancellationToken">Stops the copy.</param>
    /// <remarks>
    /// Each batch is written as it was received, with no copy of it in between. What was written before
    /// a failure stays written, and the bytes of a write that failed or was cancelled are not retried.
    /// Bytes an earlier <see cref="ReadAsync"/> left in its batch are written first.
    /// </remarks>
    /// <exception cref="InvalidOperationException">Another read or copy of this handle is under way.</exception>
    /// <exception cref="TransferFailedException">The transfer failed, or the handle was released before it reached its outcome.</exception>
    public abstract Task CopyToAsync(Stream destination, CancellationToken cancellationToken = default);

    /// <summary>
    /// Waits for the transfer to be fully completed (including shutdown handshake).
    /// </summary>
    /// <remarks>
    /// <see cref="ReadAsync"/> and <see cref="CopyToAsync"/> already wait for this on their own. What
    /// this is for is awaiting it in parallel with them, to wake a consumer blocked on its own output
    /// as soon as the transfer fails, or waiting on a handle that is never read at all. It returns once
    /// the sender has been acknowledged, which says nothing of what the consumer has taken.
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
