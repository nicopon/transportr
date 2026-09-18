using System.Text.Json;
using TransportR.Interfaces;
using ZstdSharp;

namespace TransportR.Serialization.Json.Services;

public class JsonBatchSerializer<T> : IBatchSerializer<T>
{
    private readonly JsonSerializerOptions _options;
    private readonly int _compressionLevel;

    public JsonBatchSerializer(JsonSerializerOptions? options = null, int compressionLevel = -1)
    {
        _options = options ?? new JsonSerializerOptions
        {
            PropertyNameCaseInsensitive = true,
            WriteIndented = false // Compact by default for network efficiency
        };
        _compressionLevel = compressionLevel;
    }

    public async Task SerializeBatchAsync(Stream destination, List<T> batch)
    {
        // Wrap with Zstd Compression
        using var compressionStream = new CompressionStream(destination, level: _compressionLevel, leaveOpen: true);
        await JsonSerializer.SerializeAsync(compressionStream, batch, _options);
        // Implicit Dispose flushes the Zstd frame
    }

    public async Task<List<T>?> DeserializeBatchAsync(Stream source)
    {
        try 
        {
            // Wrap with Zstd Decompression
            using var decompressionStream = new DecompressionStream(source, leaveOpen: true);
            return await JsonSerializer.DeserializeAsync<List<T>>(decompressionStream, _options);
        }
        catch (JsonException)
        {
            return null;
        }
        catch (IOException) // To catch potential end-of-stream or decompression errors more gracefully if needed
        {
            return null;
        }
    }
}
