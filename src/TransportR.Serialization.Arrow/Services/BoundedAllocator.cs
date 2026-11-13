using System.Buffers;
using Apache.Arrow.Memory;
using TransportR.Client.Internal;

namespace TransportR.Serialization.Arrow.Services;

/// <summary>
/// Serves the buffers of one batch from the allocator of the host and refuses the request that takes
/// the batch past a limit, so a batch is refused before the buffer that would break it exists.
/// </summary>
/// <remarks>
/// One instance per batch: the count is not reset. The sizes requested are the ones the batch
/// announces for its buffers, so they are read before anything is allocated for them.
/// </remarks>
internal sealed class BoundedAllocator(MemoryAllocator inner, long limit) : MemoryAllocator
{
    private long _requested;

    protected override IMemoryOwner<byte> AllocateInternal(int length, out int bytesAllocated)
    {
        if (Interlocked.Add(ref _requested, length) > limit) throw DecompressionBound.Exceeded(limit);

        var owner = inner.Allocate(length);
        bytesAllocated = owner.Memory.Length;
        return owner;
    }
}
