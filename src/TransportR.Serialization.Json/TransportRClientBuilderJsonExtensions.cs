using System.Text.Json;
using TransportR.Client;
using TransportR.Serialization.Json.Services;

namespace TransportR.Serialization.Json;

public static class TransportRClientBuilderJsonExtensions
{
    /// <summary>
    /// Configures the client to use System.Text.Json for serialization.
    /// </summary>
    /// <param name="builder">The client builder.</param>
    /// <param name="options">Optional custom JsonSerializerOptions.</param>
    /// <param name="compressionLevel">Zstd compression level (default -1). Ignored when <paramref name="compress"/> is false.</param>
    /// <param name="compress">
    /// Whether to compress batch bodies with Zstd. On by default; turn it off to read the wire as
    /// plain JSON. Both ends of a transfer must use the same setting.
    /// </param>
    /// <returns>The builder instance.</returns>
    public static TransportRClientBuilder<T> WithJsonSerialization<T>(this TransportRClientBuilder<T> builder, JsonSerializerOptions? options = null, int compressionLevel = -1, bool compress = true)
    {
        return builder.WithSerializerFactory(maxBatchBytes => new JsonBatchSerializer<T>(options, compressionLevel, compress, maxBatchBytes));
    }
}
