using System.Buffers;

namespace TransportR.Client.Protocol;

/// <summary>
/// The memory a chunk's payload is read into: an array rented from <see cref="ArrayPool{T}.Shared"/>,
/// whose <see cref="Memory"/> is exactly the payload's length.
/// </summary>
/// <remarks>
/// <see cref="Dispose"/> is idempotent and atomic, and it has to be: the array goes back to a pool, so
/// returning it twice hands one array to two renters, which corrupts unrelated data and throws
/// nothing here. A receiver can reach it from two threads, its read loop and its release.
/// </remarks>
internal sealed class PooledPayload : IMemoryOwner<byte>
{
    private readonly int _length;
    private byte[]? _array;

    public PooledPayload(int length)
    {
        _array = ArrayPool<byte>.Shared.Rent(length);
        _length = length;
    }

    public Memory<byte> Memory
    {
        get
        {
            // Read once: testing the field and then indexing it lets a concurrent disposal turn an
            // ObjectDisposedException into a NullReferenceException.
            var array = _array;
            return array is not null
                ? array.AsMemory(0, _length)
                : throw new ObjectDisposedException(nameof(PooledPayload));
        }
    }

    public void Dispose()
    {
        var array = Interlocked.Exchange(ref _array, null);
        if (array is not null) ArrayPool<byte>.Shared.Return(array);
    }
}
