using Apache.Arrow.Memory;
using TransportR.Client.SignalR;
using TransportR.Serialization.Arrow.Services;

namespace TransportR.Serialization.Arrow;

public static class TransportRClientBuilderArrowExtensions
{
    /// <summary>
    /// Configures the client to carry Apache Arrow record batches.
    /// </summary>
    /// <param name="builder">The client builder.</param>
    /// <param name="allocator">
    /// Where deserialized buffers come from. Supplying one makes received batches observable by a
    /// tracking pool; the default is Arrow's native allocator.
    /// </param>
    /// <param name="compress">Compress record batch bodies (LZ4 frame). On by default.</param>
    public static TransportRClientBuilder WithArrowSerialization(
        this TransportRClientBuilder builder,
        MemoryAllocator? allocator = null,
        bool compress = true)
        => builder.WithSerializer(new ArrowBatchSerializer(allocator, compress));
}
