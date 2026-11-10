using System.Buffers;
using System.Buffers.Binary;

namespace TransportR.Protocol;

/// <summary>
/// Binary chunk header for wire protocol.
/// Fixed 30-byte header using native BinaryPrimitives (zero external dependencies).
/// </summary>
public readonly struct ChunkHeader
{
    /// <summary>Fixed header size in bytes.</summary>
    public const int Size = 30;

    /// <summary>Current protocol version.</summary>
    public const byte CurrentVersion = 2;

    /// <summary>Protocol version (1 byte).</summary>
    public byte Version { get; init; }

    /// <summary>Chunk type (1 byte).</summary>
    public ChunkType Type { get; init; }

    /// <summary>Payload length in bytes (4 bytes). 0 for control messages.</summary>
    public int PayloadLength { get; init; }

    /// <summary>Batch identifier (4 bytes).</summary>
    public int BatchId { get; init; }

    /// <summary>Item count in batch (4 bytes). 0 for control messages.</summary>
    public int Count { get; init; }

    /// <summary>Transfer identifier (16 bytes).</summary>
    public Guid TransferId { get; init; }

    /// <summary>
    /// Writes the header to the specified buffer.
    /// </summary>
    /// <param name="buffer">Target buffer (must be at least 30 bytes).</param>
    public void WriteTo(Span<byte> buffer)
    {
        buffer[0] = Version;
        buffer[1] = (byte)Type;
        BinaryPrimitives.WriteInt32LittleEndian(buffer[2..], PayloadLength);
        BinaryPrimitives.WriteInt32LittleEndian(buffer[6..], BatchId);
        BinaryPrimitives.WriteInt32LittleEndian(buffer[10..], Count);
        TransferId.TryWriteBytes(buffer[14..]);
    }

    /// <summary>
    /// Reads a header from the specified buffer.
    /// </summary>
    /// <param name="buffer">Source buffer (must be at least 30 bytes).</param>
    /// <returns>Parsed header.</returns>
    public static ChunkHeader ReadFrom(ReadOnlySpan<byte> buffer)
    {
        return new ChunkHeader
        {
            Version = buffer[0],
            Type = (ChunkType)buffer[1],
            PayloadLength = BinaryPrimitives.ReadInt32LittleEndian(buffer[2..]),
            BatchId = BinaryPrimitives.ReadInt32LittleEndian(buffer[6..]),
            Count = BinaryPrimitives.ReadInt32LittleEndian(buffer[10..]),
            TransferId = new Guid(buffer[14..30])
        };
    }

    /// <summary>
    /// Asynchronously reads a header from a stream.
    /// </summary>
    /// <param name="stream">Source stream.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>Parsed header, or null if stream ended.</returns>
    public static async Task<ChunkHeader?> ReadFromStreamAsync(Stream stream, CancellationToken ct = default)
    {
        byte[] buffer = ArrayPool<byte>.Shared.Rent(Size);

        try
        {
            int totalRead = 0;

            while (totalRead < Size)
            {
                int read = await stream.ReadAsync(buffer, totalRead, Size - totalRead, ct).ConfigureAwait(false);
                if (read == 0)
                {
                    // EOF before reading any data = normal end
                    if (totalRead == 0) return null;
                    // EOF mid-header = error
                    throw new EndOfStreamException($"Unexpected end of stream while reading chunk header (read {totalRead}/{Size} bytes)");
                }
                totalRead += read;
            }

            var header = ReadFrom(buffer.AsSpan(0, Size));

            // Version validation
            if (header.Version != CurrentVersion)
            {
                throw new WireProtocolViolationException($"Unsupported protocol version: {header.Version} (expected {CurrentVersion})");
            }

            return header;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    /// <summary>
    /// Creates a batch header.
    /// </summary>
    public static ChunkHeader CreateBatch(Guid transferId, int batchId, int count, int payloadLength)
    {
        return new ChunkHeader
        {
            Version = CurrentVersion,
            Type = ChunkType.Batch,
            PayloadLength = payloadLength,
            BatchId = batchId,
            Count = count,
            TransferId = transferId
        };
    }

    /// <summary>
    /// Creates a control header (BatchAck, Complete, CompleteAck, Ping or Pong).
    /// </summary>
    public static ChunkHeader CreateControl(Guid transferId, int batchId, ChunkType type)
    {
        return new ChunkHeader
        {
            Version = CurrentVersion,
            Type = type,
            PayloadLength = 0,
            BatchId = batchId,
            Count = 0,
            TransferId = transferId
        };
    }
}
