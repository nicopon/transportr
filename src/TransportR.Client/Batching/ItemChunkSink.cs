using System.Buffers;
using System.Runtime.InteropServices;
using TransportR.Client.Protocol;
using TransportR.Interfaces;

namespace TransportR.Client.Batching;

/// <summary>
/// Decodes each batch body into items and hands them to a <see cref="ReceiveBuffer{T}"/>.
/// </summary>
internal sealed class ItemChunkSink<T>(ReceiveBuffer<T> buffer, IBatchSerializer<T> serializer) : IReceiveSink<T>
{
    public ChunkType BatchType => ChunkType.Batch;

    /// <summary>
    /// Hands a chunk to the receive buffer, which charges it against the configured capacity
    /// before it is decoded and parks here for as long as the consumer is behind.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The chunk's payload length is what gets charged — the wire size, which is the only size
    /// available without knowing what <typeparamref name="T"/> costs on the heap. The sender
    /// strips the item-count prefix before writing, so this is already the serialized body alone.
    /// </para>
    /// <para>
    /// The token cancels the wait for capacity and the hand-off, not the decoding between them:
    /// nothing under the serializer interface takes one. Always <c>false</c>: the items are
    /// decoded out of the body, so the memory is the caller's to return.
    /// </para>
    /// </remarks>
    public async Task<bool> WriteAsync(IMemoryOwner<byte> owner, ReadOnlyMemory<byte> body, CancellationToken ct)
    {
        await buffer.WriteChunkAsync(
            body.Length,
            () => ReadBatchBodyFromMemoryAsync(serializer, body),
            ct).ConfigureAwait(false);
        return false;
    }

    public void Complete() => buffer.Complete();

    public void Fail(Exception error) => buffer.Complete(error);

    /// <remarks>
    /// Nothing to return: decoded items hold no pooled memory, and they stay the consumer's to take.
    /// </remarks>
    public void DiscardUnread() { }

    public IAsyncEnumerable<T> ReadItemsAsync(CancellationToken ct) => buffer.ReadAllAsync(ct);

    /// <summary>
    /// Decodes a batch body straight out of the memory it was read into.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Internal rather than private because the serializer tests enter through here: how the body
    /// reaches the serializer is part of what they check — the stream wraps the received memory
    /// instead of copying it, which is what lets a reader allocate through a pool.
    /// </para>
    /// <para>
    /// Takes no cancellation token, because there is nowhere to honour one:
    /// <see cref="IBatchSerializer{T}.DeserializeBatchAsync"/> does not accept one, and the source
    /// is memory already read off the wire. A token accepted here and passed nowhere would tell a
    /// caller that decoding is interruptible when it is not.
    /// </para>
    /// </remarks>
    internal static async Task<List<T>?> ReadBatchBodyFromMemoryAsync(
        IBatchSerializer<T> serializer, ReadOnlyMemory<byte> bodyBytes)
    {
        // An array-backed body is wrapped rather than copied; only a body that is not can force
        // the copy in the other branch, which does not happen on the read path.
        Stream source = MemoryMarshal.TryGetArray(bodyBytes, out var segment)
            ? new MemoryStream(segment.Array!, segment.Offset, segment.Count, writable: false)
            : new MemoryStream(bodyBytes.ToArray());

        using (source)
        {
            try
            {
                return await serializer.DeserializeBatchAsync(source);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                // The body arrived whole, framed by its header, so a resend after a reconnection
                // decodes no better: left retryable, this reconnects forever on the same batch.
                throw new UndecodableBatchException(
                    $"A batch body could not be decoded by {serializer.GetType().Name}: {ex.Message}", ex);
            }
        }
    }
}
