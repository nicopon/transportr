using Apache.Arrow;
using Apache.Arrow.Memory;
using TransportR.Client;
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
    /// <remarks>
    /// Only offered on a builder carrying <see cref="RecordBatch"/>. That is the point: on any
    /// other item type this method does not exist, so the mismatch is a compile error instead of
    /// a client that builds and dies on its first batch.
    /// </remarks>
    public static TransportRClientBuilder<RecordBatch> WithArrowSerialization(
        this TransportRClientBuilder<RecordBatch> builder,
        MemoryAllocator? allocator = null,
        bool compress = true)
        => builder.WithSerializer(new ArrowBatchSerializer(allocator, compress));
}
