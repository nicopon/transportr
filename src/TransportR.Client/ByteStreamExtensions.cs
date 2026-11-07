using System.Buffers;
using TransportR.Abstractions;
using TransportR.Client.Internal;

namespace TransportR.Client;

/// <summary>
/// The byte-stream mode of a <c>byte[]</c> client: the transfer carries one sequence of bytes, with no
/// serializer between the caller and the wire.
/// </summary>
public static class ByteStreamExtensions
{
    /// <summary>
    /// Builds a client that transfers a stream of bytes instead of serialized items.
    /// </summary>
    /// <remarks>
    /// <para>
    /// No boundary survives: neither the one between two calls of the sender nor the one between two
    /// batches. Both ends of a transfer must be in this mode, and a receiver that is not refuses the
    /// transfer, which ends it on both sides rather than delivering bytes it cannot read.
    /// </para>
    /// <para>
    /// Nothing is compressed. The mode cannot be combined with <c>WithSerializer</c>,
    /// <c>WithBatchSize</c>, or a receive capacity bounded by items; <c>Build</c> refuses each, in any
    /// order of the calls.
    /// </para>
    /// </remarks>
    /// <param name="builder">The builder of a client carrying <c>byte[]</c>.</param>
    /// <param name="batchBytes">
    /// The size, in bytes, at which a batch is cut. It sets what an acknowledgement, a send window slot
    /// and a resend cover, so the sender holds at most this much times <c>WithMaxInFlightBatches</c>. There
    /// is no default: the right figure depends on the use.
    /// </param>
    public static TransportRClientBuilder<byte[]> WithByteStream(this TransportRClientBuilder<byte[]> builder, int batchBytes)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(batchBytes, 0, nameof(batchBytes));
        builder.ByteStreamBatchBytes = batchBytes;
        return builder;
    }

    // The default of Stream.CopyToAsync, under the threshold at which an array goes to the large object heap.
    private const int ReadBufferBytes = 81920;

    /// <summary>
    /// Reads <paramref name="source"/> to its end into the transfer, and returns without completing or
    /// disposing the handle, as <see cref="Stream.CopyToAsync(Stream)"/> closes nothing.
    /// </summary>
    /// <remarks>
    /// <para>
    /// On a handle of a client in the byte-stream mode the bytes are appended to the stream of the
    /// transfer, and no boundary survives: a read of the source is not an item. On any other handle each
    /// read is sent as one item of exactly the bytes read, which is the only thing such a handle can do.
    /// </para>
    /// <para>
    /// The wait for room in the send window is the same as for <see cref="Send{T}.SendAsync(T, CancellationToken)"/>:
    /// a receiver that is behind slows the read of the source, and one that went away fails the call with
    /// <see cref="TransferFailedException"/>. The caller completes the transfer with
    /// <see cref="Send{T}.CompleteAsync"/> once this returns.
    /// </para>
    /// </remarks>
    /// <param name="send">The handle of the transfer.</param>
    /// <param name="source">The stream to read. It is not closed.</param>
    /// <param name="cancellationToken">Stops the copy; bytes already appended stay in the transfer.</param>
    public static async Task CopyFromAsync(this Send<byte[]> send, Stream source, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(send);
        ArgumentNullException.ThrowIfNull(source);

        var buffer = ArrayPool<byte>.Shared.Rent(ReadBufferBytes);
        var appending = false;
        try
        {
            int read;
            if (send is HttpSend<byte[]> { IsByteStream: true } direct)
            {
                while ((read = await source.ReadAsync(buffer.AsMemory(0, ReadBufferBytes), cancellationToken).ConfigureAwait(false)) > 0)
                {
                    appending = true;
                    await direct.SendBytesAsync(buffer.AsMemory(0, read), cancellationToken).ConfigureAwait(false);
                    appending = false;
                }
            }
            else
            {
                while ((read = await source.ReadAsync(buffer.AsMemory(0, ReadBufferBytes), cancellationToken).ConfigureAwait(false)) > 0)
                {
                    // A copy: the sender keeps an item until its batch is flushed, so the buffer cannot be reused under it.
                    await send.SendAsync(buffer.AsSpan(0, read).ToArray(), cancellationToken).ConfigureAwait(false);
                }
            }
        }
        finally
        {
            // An append that ended in an exception may still be copying from the buffer: a cancelled
            // caller is let go before the producer it was waiting on. Returning it then would hand
            // memory being read to another renter, so it is left to the garbage collector.
            if (!appending) ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    /// <summary>
    /// Writes what the transfer carries to <paramref name="destination"/> until the transfer has ended,
    /// and returns without disposing the handle.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The intended way to read a transfer in the byte-stream mode: each batch is written to the
    /// destination as it was received and handed back, where <see cref="Receive{T}.ReceiveAsync"/>
    /// allocates a <c>byte[]</c> per batch, on the large object heap past 85,000 bytes. On a handle of
    /// any other client each item is written in turn.
    /// </para>
    /// <para>
    /// Returns once the transfer has completed, as the enumeration of <see cref="Receive{T}.ReceiveAsync"/>
    /// does, and throws <see cref="TransferFailedException"/> when it failed. The destination is not
    /// closed, and what was written before a failure stays written.
    /// </para>
    /// </remarks>
    /// <param name="receive">The handle of the transfer.</param>
    /// <param name="destination">The stream to write.</param>
    /// <param name="cancellationToken">Stops the copy.</param>
    public static async Task CopyToAsync(this Receive<byte[]> receive, Stream destination, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(receive);
        ArgumentNullException.ThrowIfNull(destination);

        if (receive is HttpReceive<byte[]> { IsByteStream: true } direct)
        {
            await foreach (var chunk in direct.ReceiveChunksAsync(cancellationToken).ConfigureAwait(false))
            {
                // The chunk is ours from the moment it is yielded, so a write that throws or is
                // cancelled must still give its pooled memory back.
                try
                {
                    await destination.WriteAsync(chunk.Memory, cancellationToken).ConfigureAwait(false);
                }
                finally
                {
                    chunk.Dispose();
                }
            }

            return;
        }

        await foreach (var batch in receive.ReceiveAsync(cancellationToken).ConfigureAwait(false))
        {
            await destination.WriteAsync(batch, cancellationToken).ConfigureAwait(false);
        }
    }
}
