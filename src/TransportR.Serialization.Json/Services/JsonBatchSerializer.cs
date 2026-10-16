using System.Text.Json;
using TransportR.Interfaces;
using ZstdSharp;

namespace TransportR.Serialization.Json.Services;

/// <summary>
/// Serializes batches with System.Text.Json, Zstd-compressed unless told otherwise.
/// </summary>
/// <remarks>
/// Both ends of a transfer must agree on <c>compress</c>: the wire carries no marker saying which
/// one was used. A mismatch fails the transfer at its first batch, on both ends.
/// </remarks>
public class JsonBatchSerializer<T> : IBatchSerializer<T>
{
    private readonly JsonSerializerOptions _options;
    private readonly int _compressionLevel;
    private readonly bool _compress;

    /// <param name="options">Serializer options; compact, case-insensitive property names by default.</param>
    /// <param name="compressionLevel">Zstd compression level. Ignored when <paramref name="compress"/> is false.</param>
    /// <param name="compress">
    /// Whether to compress batch bodies with Zstd. On by default. Off, a captured body is plain
    /// UTF-8 JSON, which is the reason to turn it off: reading the wire while debugging.
    /// </param>
    public JsonBatchSerializer(JsonSerializerOptions? options = null, int compressionLevel = -1, bool compress = true)
    {
        _options = options ?? new JsonSerializerOptions
        {
            PropertyNameCaseInsensitive = true,
            WriteIndented = false // Compact by default for network efficiency
        };
        _compressionLevel = compressionLevel;
        _compress = compress;
    }

    public async Task SerializeBatchAsync(Stream destination, List<T> batch)
    {
        if (!_compress)
        {
            await JsonSerializer.SerializeAsync(destination, batch, _options);
            return;
        }

        using var compressionStream = new CompressionStream(destination, level: _compressionLevel, leaveOpen: true);
        await JsonSerializer.SerializeAsync(compressionStream, batch, _options);
        // Implicit Dispose flushes the Zstd frame
    }

    // A body that does not decode throws rather than returning null: the receive path drops a null
    // batch as empty, so a swallowed error here would acknowledge items nobody ever received.
    public async Task<List<T>?> DeserializeBatchAsync(Stream source)
    {
        if (!_compress)
        {
            return await JsonSerializer.DeserializeAsync<List<T>>(source, _options);
        }

        using var decompressionStream = new DecompressionStream(source, leaveOpen: true);
        return await JsonSerializer.DeserializeAsync<List<T>>(decompressionStream, _options);
    }
}
