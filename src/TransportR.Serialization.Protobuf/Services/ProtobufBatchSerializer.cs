using TransportR.Interfaces;
using ProtoBuf;
using ZstdSharp;

namespace TransportR.Serialization.Protobuf.Services;

/// <summary>
/// Serializes batches using Protocol Buffers and compresses them with Zstd.
/// </summary>
public class ProtobufBatchSerializer<T> : IBatchSerializer<T>
{
    private readonly int _compressionLevel;

    public ProtobufBatchSerializer(int compressionLevel = -1)
    {
        _compressionLevel = compressionLevel;
    }

    public async Task SerializeBatchAsync(Stream destination, List<T> batch)
    {
        // Use ZstdCompressionStream to wrap the destination stream
        // 'leaveOpen: true' is crucial because the destination stream (NetworkStream/Pipe)
        // must remain open for subsequent batches.
        using var compressionStream = new CompressionStream(destination, level: _compressionLevel, leaveOpen: true);

        await Task.Run(() => Serializer.Serialize(compressionStream, batch));
        
        // Implicit Dispose flushes the Zstd frame.
    }

    public async Task<List<T>?> DeserializeBatchAsync(Stream source)
    {
        try 
        {
            // Wrap the source stream with ZstdDecompressionStream
            // 'leaveOpen: true' to keep the source stream open for the next batch
            using var decompressionStream = new DecompressionStream(source, leaveOpen: true);

            return await Task.Run(() => Serializer.Deserialize<List<T>>(decompressionStream));
        }
        catch (EndOfStreamException)
        {
            return null;
        }
    }
}
