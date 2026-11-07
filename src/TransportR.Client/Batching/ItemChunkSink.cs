using System.Buffers;
using TransportR.Interfaces;

namespace TransportR.Client.Batching;

/// <summary>
/// Decodes each batch body into items and hands them to a <see cref="ReceiveBuffer{T}"/>.
/// </summary>
internal sealed class ItemChunkSink<T>(ReceiveBuffer<T> buffer, IBatchSerializer<T> serializer) : IChunkSink
{
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
            () => ReceiverState<T>.ReadBatchBodyFromMemoryAsync(serializer, body),
            ct).ConfigureAwait(false);
        return false;
    }

    public void Complete() => buffer.Complete();

    public void Fail(Exception error) => buffer.Complete(error);

    /// <remarks>
    /// Nothing to return: decoded items hold no pooled memory, and they stay the consumer's to take.
    /// </remarks>
    public void DiscardUnread() { }
}
