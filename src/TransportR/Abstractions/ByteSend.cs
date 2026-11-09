using System.Buffers;

namespace TransportR.Abstractions;

/// <summary>
/// Handle for sending a stream of bytes through a transfer.
/// </summary>
/// <remarks>
/// No boundary survives: neither the one between two calls nor the one between two batches. Nothing is
/// compressed, and the receiving end has to be a <see cref="ByteReceive"/> as well: a receiver that is
/// not refuses the transfer, which ends it on both sides.
/// </remarks>
public abstract class ByteSend : Transfer, IAsyncDisposable
{
    // The default of Stream.CopyToAsync, under the threshold at which an array goes to the large object heap.
    private const int ReadBufferBytes = 81920;

    /// <summary>
    /// Creates a new send handle.
    /// </summary>
    /// <param name="transferId">The unique transfer identifier.</param>
    protected ByteSend(string transferId) : base(transferId) { }

    /// <summary>
    /// Appends <paramref name="bytes"/> to the stream of the transfer. They are copied before this
    /// returns, so the caller may reuse its buffer at once.
    /// </summary>
    /// <param name="bytes">The bytes to send.</param>
    /// <param name="cancellationToken">Token to cancel the send operation.</param>
    /// <remarks>
    /// The wait for room in the send window is that of <see cref="Send{T}.SendAsync(T, CancellationToken)"/>:
    /// a receiver that is behind slows the caller, and one that went away fails the call.
    /// </remarks>
    /// <exception cref="TransferFailedException">
    /// The transfer has ended without completing: the send window was full and the receiver
    /// acknowledged nothing for the client's completion stall bound, the peer or the hub ended it, or
    /// this handle was released. That holds for a call made after the ending as for one parked when it
    /// came. The handle is dead; dispose it.
    /// </exception>
    /// <exception cref="InvalidOperationException">
    /// <see cref="CompleteAsync"/> has been called: a completed transfer carries nothing more, and the
    /// bytes are not sent.
    /// </exception>
    public abstract Task WriteAsync(ReadOnlyMemory<byte> bytes, CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads <paramref name="source"/> to its end into the transfer, and returns without completing or
    /// disposing the handle, as <see cref="Stream.CopyToAsync(Stream)"/> closes nothing.
    /// </summary>
    /// <param name="source">The stream to read. It is not closed.</param>
    /// <param name="cancellationToken">Stops the copy; bytes already appended stay in the transfer.</param>
    /// <remarks>
    /// The caller completes the transfer with <see cref="CompleteAsync"/> once this returns. Waits and
    /// failures are those of <see cref="WriteAsync"/>. Uses a default buffer of 80 KB.
    /// </remarks>
    public Task CopyFromAsync(Stream source, CancellationToken cancellationToken = default) =>
        CopyFromAsync(source, ReadBufferBytes, cancellationToken);

    /// <summary>
    /// Reads <paramref name="source"/> to its end into the transfer with the specified buffer size, and
    /// returns without completing or disposing the handle, as <see cref="Stream.CopyToAsync(Stream)"/> closes nothing.
    /// </summary>
    /// <param name="source">The stream to read. It is not closed.</param>
    /// <param name="bufferSize">Rented read buffer size in bytes. Must be greater than zero.</param>
    /// <param name="cancellationToken">Stops the copy; bytes already appended stay in the transfer.</param>
    /// <remarks>
    /// The caller completes the transfer with <see cref="CompleteAsync"/> once this returns. Waits and
    /// failures are those of <see cref="WriteAsync"/>.
    /// </remarks>
    public async Task CopyFromAsync(Stream source, int bufferSize, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(bufferSize, 0, nameof(bufferSize));

        var buffer = ArrayPool<byte>.Shared.Rent(bufferSize);
        var writing = false;
        try
        {
            int read;
            while ((read = await source.ReadAsync(buffer.AsMemory(0, bufferSize), cancellationToken).ConfigureAwait(false)) > 0)
            {
                writing = true;
                await WriteAsync(buffer.AsMemory(0, read), cancellationToken).ConfigureAwait(false);
                writing = false;
            }
        }
        finally
        {
            // A derived class may return from a failed write while it still reads the buffer (the
            // client's own does not). Returning the buffer then would hand memory being read to
            // another renter, so it is left to the garbage collector.
            if (!writing) ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    /// <summary>
    /// Signals that all bytes have been sent and waits for acknowledgment.
    /// </summary>
    /// <param name="cancellationToken">Token to cancel the completion wait.</param>
    /// <remarks>
    /// Returning normally is the success signal, and the only one: there is no counter a caller can
    /// compare after the fact to learn whether the receiver got everything.
    /// </remarks>
    /// <exception cref="TransferFailedException">
    /// The receiver stopped acknowledging for the client's completion stall bound, or the session
    /// failed. The handle is dead; dispose it.
    /// </exception>
    public abstract Task CompleteAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Releases the transfer and everything the client holds on its behalf. Dispose once
    /// <see cref="CompleteAsync"/> has returned, not before: disposing a transfer that has not ended
    /// abandons it, and <see cref="CompleteAsync"/> then throws <see cref="TransferFailedException"/>
    /// instead of waiting on a session that is gone. Disposing twice does nothing.
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
