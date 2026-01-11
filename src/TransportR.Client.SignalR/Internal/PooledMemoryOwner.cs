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

    public Memory<byte> Memory => _array != null 
        ? _array.AsMemory(0, _memory.Length) 
        : throw new ObjectDisposedException(nameof(PooledMemoryOwner));

    public void Dispose()
    {
        if (_array != null)
        {
            ArrayPool<byte>.Shared.Return(_array);
            _array = null;
        }
    }
}
