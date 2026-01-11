using System.Text.Json;
using TransportR.Client.SignalR;
using TransportR.Serialization.Json.Services;

namespace TransportR.Serialization.Json;

public static class TransportRClientBuilderJsonExtensions
{
    /// <summary>
    /// Configures the client to use System.Text.Json for serialization.
    /// </summary>
    /// <param name="builder">The client builder.</param>
    /// <param name="options">Optional custom JsonSerializerOptions.</param>
    /// <param name="compressionLevel">Zstd compression level (default -1).</param>
    /// <returns>The builder instance.</returns>
    public static TransportRClientBuilder WithJsonSerialization(this TransportRClientBuilder builder, JsonSerializerOptions? options = null, int compressionLevel = -1)
    {
        return builder.WithSerializer(new JsonBatchSerializer(options, compressionLevel));
    }
}
