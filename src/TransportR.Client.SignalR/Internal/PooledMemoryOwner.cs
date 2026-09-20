using System.Buffers;

namespace TransportR.Client.SignalR.Internal;

/// <summary>
/// Memory owner that wraps a pooled memory segment for efficient buffer management.
/// </summary>
internal class PooledMemoryOwner : IMemoryOwner<byte>
{
    private readonly ReadOnlyMemory<byte> _memory;
    private byte[]? _array;

    public PooledMemoryOwner(ReadOnlyMemory<byte> memory)
    {
        _array = ArrayPool<byte>.Shared.Rent(memory.Length);
        memory.CopyTo(_array);
        _memory = _array.AsMemory(0, memory.Length);
    }

    public Memory<byte> Memory
    {
        get
        {
            // Read once: testing the field and then indexing it lets a concurrent disposal turn an
            // ObjectDisposedException into a NullReferenceException.
            var array = _array;
            return array is not null
                ? array.AsMemory(0, _memory.Length)
                : throw new ObjectDisposedException(nameof(PooledMemoryOwner));
        }
    }

    /// <remarks>
    /// Idempotent, and it has to be: the buffer comes from a pool, so returning it twice hands one
    /// array to two renters. That corrupts unrelated data and throws nothing here.
    /// </remarks>
    public void Dispose()
    {
        var array = Interlocked.Exchange(ref _array, null);
        if (array is not null) ArrayPool<byte>.Shared.Return(array);
    }
}
