using Microsoft.Extensions.DependencyInjection;
using TransportR.Client.SignalR;
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
    public static TransportRClientBuilder WithProtobufSerialization(this TransportRClientBuilder builder, int compressionLevel = -1)
    {
        return builder.WithSerializer(new ProtobufBatchSerializer(compressionLevel));
    }
}
