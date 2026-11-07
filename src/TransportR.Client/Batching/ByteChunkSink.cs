using System.Buffers;

namespace TransportR.Client.Batching;

/// <summary>
/// Hands each batch body to the consumer as it is, as one <see cref="ByteChunk"/>, without decoding it.
/// </summary>
internal sealed class ByteChunkSink(ReceiveBuffer<ByteChunk> buffer) : IChunkSink
{
    /// <remarks>
    /// One entry per chunk, charged its wire size. Taken only once the entry is in the buffer: a wait
    /// for room that is cancelled, or a buffer completed under the write, leaves the memory with the
    /// caller, which is what keeps it returned exactly once.
    /// </remarks>
    public async Task<bool> WriteAsync(IMemoryOwner<byte> owner, ReadOnlyMemory<byte> body, CancellationToken ct)
    {
        var written = await buffer.WriteChunkAsync(
            body.Length,
            () => Task.FromResult<List<ByteChunk>?>([new ByteChunk(owner, body.Length)]),
            ct).ConfigureAwait(false);

        return written > 0;
    }

    public void Complete() => buffer.Complete();

    public void Fail(Exception error) => buffer.Complete(error);

    /// <summary>
    /// The chunks in the order they were admitted. Each is the reader's to dispose.
    /// </summary>
    public IAsyncEnumerable<ByteChunk> ReadAllAsync(CancellationToken ct) => buffer.ReadAllAsync(ct);

    public void DiscardUnread() => buffer.Drain(chunk => chunk.Dispose());
}
