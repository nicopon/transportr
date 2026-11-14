using System.Buffers;

namespace TransportR.Client.Protocol;

/// <summary>
/// Chunk I/O operations for reading and writing protocol messages.
/// Replaces ProtoProtocol with zero external dependencies.
/// </summary>
internal static class ChunkIO
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
        await WriteBatchAsync(stream, transferId, ChunkType.Batch, batchId, count, payload, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Writes a batch chunk of the given type (<see cref="ChunkType.Batch"/> or
    /// <see cref="ChunkType.StreamBatch"/>) to the stream.
    /// </summary>
    /// <remarks>
    /// The type tells the receiver how the payload is to be read. A receiver expecting the other one
    /// refuses the chunk and aborts the transfer, so the type is the sender's to get right.
    /// </remarks>
    public static async Task WriteBatchAsync(
        Stream stream,
        Guid transferId,
        ChunkType type,
        int batchId,
        int count,
        ReadOnlyMemory<byte> payload,
        CancellationToken ct = default)
    {
        if (type is not (ChunkType.Batch or ChunkType.StreamBatch))
            throw new ArgumentOutOfRangeException(nameof(type), type, "Not a batch chunk type.");

        var header = ChunkHeader.CreateBatch(transferId, batchId, count, payload.Length) with { Type = type };
        await WriteChunkAsync(stream, header, payload, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Writes a batch chunk of the given type whose payload is a sequence, the bytes on the wire being
    /// those of the overload that takes the payload in one block.
    /// </summary>
    /// <remarks>
    /// The header and each segment go to <paramref name="stream"/> as writes of their own, with no
    /// buffer to join them: for a stream that buffers what it is given, as each write stream of the
    /// client does, and sends it on a flush or a full buffer, never per write.
    /// </remarks>
    internal static async Task WriteBatchAsync(
        Stream stream,
        Guid transferId,
        ChunkType type,
        int batchId,
        int count,
        ReadOnlySequence<byte> payload,
        CancellationToken ct = default)
    {
        if (type is not (ChunkType.Batch or ChunkType.StreamBatch))
            throw new ArgumentOutOfRangeException(nameof(type), type, "Not a batch chunk type.");

        var header = ChunkHeader.CreateBatch(transferId, batchId, count, checked((int)payload.Length)) with { Type = type };
        await WriteHeaderOnlyAsync(stream, header, ct).ConfigureAwait(false);

        foreach (var segment in payload)
        {
            await stream.WriteAsync(segment, ct).ConfigureAwait(false);
        }
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
    /// Reads a chunk from the stream, and hands the memory its payload was read into to the caller,
    /// as the caller's own.
    /// </summary>
    /// <param name="maxPayloadLength">
    /// The largest payload accepted. The length comes off the wire, so it is checked before any memory
    /// is rented for it: a negative length, or one above this, is a <see cref="WireProtocolViolationException"/>.
    /// </param>
    /// <param name="allowedBy">What sets that bound, as the refusal names it.</param>
    /// <returns>
    /// The header, and the payload's memory, exactly its length, or <c>null</c> when it has none; or
    /// <c>null</c> if the stream ended. The owner is handed over bare, not wrapped in a struct that
    /// can be copied, so that no copy of it can return that memory: whoever the caller hands the owner
    /// to returns it, once.
    /// </returns>
    internal static async ValueTask<(ChunkHeader Header, IMemoryOwner<byte>? Payload)?> ReadOwnedChunkAsync(
        Stream stream, int maxPayloadLength, string allowedBy, CancellationToken ct)
    {
        var read = await ChunkHeader.ReadFromStreamAsync(stream, ct).ConfigureAwait(false);
        if (read is not { } header) return null;
        if (header.PayloadLength < 0)
        {
            throw new WireProtocolViolationException($"A chunk announced a payload of {header.PayloadLength} bytes.");
        }

        if (header.PayloadLength == 0) return (header, null);
        if (header.PayloadLength > maxPayloadLength)
        {
            throw new WireProtocolViolationException(
                $"A chunk announced a payload of {header.PayloadLength} bytes, over the {maxPayloadLength} bytes {allowedBy}.");
        }

        var payload = new PooledPayload(header.PayloadLength);
        try
        {
            var memory = payload.Memory;
            int totalRead = 0;
            while (totalRead < header.PayloadLength)
            {
                int count = await stream.ReadAsync(memory[totalRead..], ct).ConfigureAwait(false);
                if (count == 0)
                {
                    throw new EndOfStreamException($"Unexpected end of stream while reading payload (read {totalRead}/{header.PayloadLength} bytes)");
                }
                totalRead += count;
            }

            return (header, payload);
        }
        catch
        {
            payload.Dispose();
            throw;
        }
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
internal readonly struct ChunkData : IDisposable
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
