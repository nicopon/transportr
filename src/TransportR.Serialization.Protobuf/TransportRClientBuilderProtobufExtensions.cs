using TransportR.Client;
using TransportR.Serialization.Protobuf.Services;

namespace TransportR.Serialization.Protobuf;

public static class TransportRClientBuilderProtobufExtensions
{
    /// <summary>
    /// Configures the client to use Protocol Buffers serialization with Zstd compression.
    /// </summary>
    /// <param name="builder">The client builder.</param>
    /// <param name="compressionLevel">Zstd compression level (default -1).</param>
    /// <returns>The builder instance.</returns>
    public static TransportRClientBuilder<T> WithProtobufSerialization<T>(this TransportRClientBuilder<T> builder, int compressionLevel = -1)
    {
        return builder.WithSerializer(new ProtobufBatchSerializer<T>(compressionLevel));
    }
}
