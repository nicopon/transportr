using System.Buffers;

namespace TransportR.Client.Batching;

/// <summary>
/// One received batch of a byte-stream transfer: pooled memory handed over to the consumer, who
/// returns it.
/// </summary>
/// <remarks>
/// Holds the memory the chunk's payload was read into, which the receiver state handed over.
/// <see cref="Dispose"/> is idempotent because the buffer behind it is pooled, and returning one twice
/// hands it to two renters.
/// </remarks>
internal sealed class ByteChunk(IMemoryOwner<byte> owner, int length) : IDisposable
{
    private IMemoryOwner<byte>? _owner = owner;

    public int Length { get; } = length;

    /// <exception cref="ObjectDisposedException">The chunk has been disposed.</exception>
    public ReadOnlyMemory<byte> Memory
        => (Volatile.Read(ref _owner) ?? throw new ObjectDisposedException(nameof(ByteChunk))).Memory[..Length];

    public void Dispose() => Interlocked.Exchange(ref _owner, null)?.Dispose();
}
