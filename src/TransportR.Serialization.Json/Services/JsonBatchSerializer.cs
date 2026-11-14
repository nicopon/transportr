using System.Text.Json;
using TransportR.Client;
using TransportR.Client.Configuration;
using TransportR.Client.Internal;
using ZstdSharp;

namespace TransportR.Serialization.Json.Services;

/// <summary>
/// Serializes batches with System.Text.Json, Zstd-compressed unless told otherwise.
/// </summary>
/// <remarks>
/// <para>
/// Both ends of a transfer must agree on <c>compress</c>: the wire carries no marker saying which
/// one was used. A mismatch fails the transfer at its first batch, on both ends.
/// </para>
/// <para>
/// A compressed batch that expands to more than the bound given at construction is refused after at
/// most one read past it, without decompressing the rest, and one that is more than that bound
/// before compression is refused at serialization, so a receiver at the same bound takes whatever
/// this sends. A serializer built through the public constructor, which is the one a host passes
/// to <c>WithSerializer</c>, keeps the 256 MiB default
/// and not the figure of <c>WithMaxBatchBytes</c>: <c>WithJsonSerialization</c> is the one that
/// carries that figure.
/// </para>
/// </remarks>
public class JsonBatchSerializer<T> : IBatchSerializer<T>
{
    private readonly JsonSerializerOptions _options;
    private readonly int _compressionLevel;
    private readonly bool _compress;
    private readonly int _maxDecompressedBytes;

    /// <param name="options">Serializer options; compact, case-insensitive property names by default.</param>
    /// <param name="compressionLevel">Zstd compression level. Ignored when <paramref name="compress"/> is false.</param>
    /// <param name="compress">
    /// Whether to compress batch bodies with Zstd. On by default. Off, a captured body is plain
    /// UTF-8 JSON, which is the reason to turn it off: reading the wire while debugging.
    /// </param>
    public JsonBatchSerializer(JsonSerializerOptions? options = null, int compressionLevel = -1, bool compress = true)
        : this(options, compressionLevel, compress, ClientOptions.DefaultMaxBatchBytes)
    {
    }

    internal JsonBatchSerializer(JsonSerializerOptions? options, int compressionLevel, bool compress, int maxDecompressedBytes)
    {
        _options = options ?? new JsonSerializerOptions
        {
            PropertyNameCaseInsensitive = true,
            WriteIndented = false // Compact by default for network efficiency
        };
        _compressionLevel = compressionLevel;
        _compress = compress;
        _maxDecompressedBytes = maxDecompressedBytes;
    }

    public async Task SerializeBatchAsync(Stream destination, List<T> batch)
    {
        if (!_compress)
        {
            await JsonSerializer.SerializeAsync(destination, batch, _options);
            return;
        }

        using var compressionStream = new CompressionStream(destination, level: _compressionLevel, leaveOpen: true);
        using var bounded = new BoundedWriteStream(compressionStream, _maxDecompressedBytes);
        await JsonSerializer.SerializeAsync(bounded, batch, _options);
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
        using var bounded = new BoundedReadStream(decompressionStream, _maxDecompressedBytes);
        return await JsonSerializer.DeserializeAsync<List<T>>(bounded, _options);
    }
}
