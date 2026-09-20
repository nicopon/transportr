using System.Buffers;

namespace TransportR.Protocol;

/// <summary>
/// Chunk I/O operations for reading and writing protocol messages.
/// Replaces ProtoProtocol with zero external dependencies.
/// </summary>
public static class ChunkIO
{
    /// <summary>
    /// Writes a batch chunk (header + payload) to the stream.
    /// </summary>
    public static async Task WriteBatchAsync(
        Stream stream, 
        Guid transferId, 
        int batchId, 
        int count, 
        ReadOnlyMemory<byte> payload,
        CancellationToken ct = default)
    {
        var header = ChunkHeader.CreateBatch(transferId, batchId, count, payload.Length);
        await WriteChunkAsync(stream, header, payload, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Writes a control chunk (BatchAck, Complete, CompleteAck, Ping or Pong) to the stream.
    /// </summary>
    public static async Task WriteControlAsync(
        Stream stream, 
        Guid transferId, 
        int batchId, 
        ChunkType type,
        CancellationToken ct = default)
    {
        var header = ChunkHeader.CreateControl(transferId, batchId, type);
        await WriteHeaderOnlyAsync(stream, header, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Reads a chunk from the stream.
    /// </summary>
    /// <returns>Header and payload (payload is empty for control messages), or null if stream ended.</returns>
    public static async Task<ChunkData?> ReadChunkAsync(Stream stream, CancellationToken ct = default)
    {
        var header = await ChunkHeader.ReadFromStreamAsync(stream, ct).ConfigureAwait(false);
        if (header == null) return null;

        if (header.Value.PayloadLength > 0)
        {
            // Rent buffer from pool for payload
            var payloadOwner = MemoryPool<byte>.Shared.Rent(header.Value.PayloadLength);
            var payloadMemory = payloadOwner.Memory[..header.Value.PayloadLength];
            
            int totalRead = 0;
            while (totalRead < header.Value.PayloadLength)
            {
                int read = await stream.ReadAsync(payloadMemory[totalRead..], ct).ConfigureAwait(false);
                if (read == 0)
                {
                    payloadOwner.Dispose();
                    throw new EndOfStreamException($"Unexpected end of stream while reading payload (read {totalRead}/{header.Value.PayloadLength} bytes)");
                }
                totalRead += read;
            }

            return new ChunkData(header.Value, payloadOwner, header.Value.PayloadLength);
        }

        return new ChunkData(header.Value, null, 0);
    }

    private static async Task WriteChunkAsync(
        Stream stream, 
        ChunkHeader header, 
        ReadOnlyMemory<byte> payload,
        CancellationToken ct)
    {
        // Rent buffer for header + payload
        int totalSize = ChunkHeader.Size + payload.Length;
        byte[] buffer = ArrayPool<byte>.Shared.Rent(totalSize);
        
        try
        {
            // Write header to buffer
            header.WriteTo(buffer.AsSpan());
            
            // Copy payload to buffer
            payload.Span.CopyTo(buffer.AsSpan(ChunkHeader.Size));
            
            // Write entire buffer to stream
            await stream.WriteAsync(buffer.AsMemory(0, totalSize), ct).ConfigureAwait(false);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private static async Task WriteHeaderOnlyAsync(
        Stream stream, 
        ChunkHeader header,
        CancellationToken ct)
    {
        byte[] buffer = ArrayPool<byte>.Shared.Rent(ChunkHeader.Size);
        
        try
        {
            header.WriteTo(buffer.AsSpan());
            await stream.WriteAsync(buffer.AsMemory(0, ChunkHeader.Size), ct).ConfigureAwait(false);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }
}

/// <summary>
/// Represents a chunk read from the stream.
/// </summary>
public readonly struct ChunkData : IDisposable
{
    /// <summary>Chunk header.</summary>
    public ChunkHeader Header { get; }
    
    /// <summary>Payload memory owner (null for control messages).</summary>
    private readonly IMemoryOwner<byte>? _payloadOwner;
    
    /// <summary>Actual payload length.</summary>
    private readonly int _payloadLength;

    public ChunkData(ChunkHeader header, IMemoryOwner<byte>? payloadOwner, int payloadLength)
    {
        Header = header;
        _payloadOwner = payloadOwner;
        _payloadLength = payloadLength;
    }

    /// <summary>Payload data (empty for control messages).</summary>
    public ReadOnlyMemory<byte> Payload => _payloadOwner?.Memory[.._payloadLength] ?? ReadOnlyMemory<byte>.Empty;

    /// <summary>Returns the payload memory to the pool.</summary>
    public void Dispose()
    {
        _payloadOwner?.Dispose();
    }
}
