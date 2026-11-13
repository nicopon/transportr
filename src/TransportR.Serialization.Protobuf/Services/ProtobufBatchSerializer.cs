using ProtoBuf;
using TransportR.Client.Configuration;
using TransportR.Client.Internal;
using TransportR.Interfaces;
using ZstdSharp;

namespace TransportR.Serialization.Protobuf.Services;

/// <summary>
/// Serializes batches using Protocol Buffers and compresses them with Zstd.
/// </summary>
/// <remarks>
/// <para>
/// A body that ends early or does not decode throws, and the transfer fails on both ends.
/// </para>
/// <para>
/// A batch that expands to more than the bound given at construction is refused after at most one
/// read past it, without decompressing the rest, and one that is more than that bound before
/// compression is refused at serialization, so a receiver at the same bound takes whatever this
/// sends. A serializer built through the public constructor,
/// which is the one a host passes to <c>WithSerializer</c>, keeps the 256 MiB default and not the
/// figure of <c>WithMaxBatchBytes</c>: <c>WithProtobufSerialization</c> is the one that carries that
/// figure.
/// </para>
/// </remarks>
public class ProtobufBatchSerializer<T> : IBatchSerializer<T>
{
    private readonly int _compressionLevel;
    private readonly int _maxDecompressedBytes;

    public ProtobufBatchSerializer(int compressionLevel = -1)
        : this(compressionLevel, ClientOptions.DefaultMaxBatchBytes)
    {
    }

    internal ProtobufBatchSerializer(int compressionLevel, int maxDecompressedBytes)
    {
        _compressionLevel = compressionLevel;
        _maxDecompressedBytes = maxDecompressedBytes;
    }

    public async Task SerializeBatchAsync(Stream destination, List<T> batch)
    {
        // Use ZstdCompressionStream to wrap the destination stream
        // 'leaveOpen: true' is crucial because the destination stream (NetworkStream/Pipe)
        // must remain open for subsequent batches.
        using var compressionStream = new CompressionStream(destination, level: _compressionLevel, leaveOpen: true);
        using var bounded = new BoundedWriteStream(compressionStream, _maxDecompressedBytes);

        await Task.Run(() => Serializer.Serialize(bounded, batch));

        // Implicit Dispose flushes the Zstd frame.
    }

    public async Task<List<T>?> DeserializeBatchAsync(Stream source)
    {
        // 'leaveOpen: true' to keep the source stream open for the next batch
        using var decompressionStream = new DecompressionStream(source, leaveOpen: true);
        using var bounded = new BoundedReadStream(decompressionStream, _maxDecompressedBytes);

        return await Task.Run(() => Serializer.Deserialize<List<T>>(bounded));
    }
}
