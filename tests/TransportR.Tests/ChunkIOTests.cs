using System.Buffers;
using TransportR.Protocol;

namespace POC_DataProxy.Tests;

/// <summary>
/// Unit tests for <see cref="ChunkIO"/> and <see cref="ChunkHeader"/>.
/// </summary>
public class ChunkIOTests
{
    [Fact]
    public void ChunkHeader_CreateBatch_CreatesValidHeader()
    {
        // Arrange
        var transferId = Guid.Parse("11111111-2222-3333-4444-555555555555");
        var batchId = 42;
        var count = 100;
        var payloadLength = 256;

        // Act
        var header = ChunkHeader.CreateBatch(transferId, batchId, count, payloadLength);

        // Assert
        Assert.Equal(ChunkHeader.CurrentVersion, header.Version);
        Assert.Equal(ChunkType.Batch, header.Type);
        Assert.Equal(transferId, header.TransferId);
        Assert.Equal(batchId, header.BatchId);
        Assert.Equal(count, header.Count);
        Assert.Equal(payloadLength, header.PayloadLength);
    }

    [Fact]
    public void ChunkHeader_CreateControl_Ack_CreatesValidHeader()
    {
        // Arrange
        var transferId = Guid.Parse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee");
        var batchId = 10;

        // Act
        var header = ChunkHeader.CreateControl(transferId, batchId, ChunkType.BatchAck);

        // Assert
        Assert.Equal(ChunkHeader.CurrentVersion, header.Version);
        Assert.Equal(ChunkType.BatchAck, header.Type);
        Assert.Equal(batchId, header.BatchId);
        Assert.Equal(0, header.PayloadLength);
        Assert.Equal(0, header.Count);
    }

    [Fact]
    public void ChunkHeader_CreateControl_Complete_CreatesValidHeader()
    {
        // Complete is now a control chunk that requires CompleteAck
        var transferId = Guid.Parse("12345678-1234-5678-1234-567812345678");

        // Act
        var header = ChunkHeader.CreateControl(transferId, 0, ChunkType.Complete);

        // Assert
        Assert.Equal(ChunkHeader.CurrentVersion, header.Version);
        Assert.Equal(ChunkType.Complete, header.Type);
        Assert.Equal(transferId, header.TransferId);
        Assert.Equal(0, header.BatchId);
        Assert.Equal(0, header.PayloadLength);
    }

    [Fact]
    public void ChunkHeader_CreateControl_CompleteAck_CreatesValidHeader()
    {
        // CompleteAck confirms receipt of Complete
        var transferId = Guid.Parse("12345678-1234-5678-1234-567812345678");

        // Act
        var header = ChunkHeader.CreateControl(transferId, 0, ChunkType.CompleteAck);

        // Assert
        Assert.Equal(ChunkHeader.CurrentVersion, header.Version);
        Assert.Equal(ChunkType.CompleteAck, header.Type);
        Assert.Equal(transferId, header.TransferId);
    }

    [Fact]
    public void ChunkHeader_WriteTo_And_ReadFrom_RoundTrip()
    {
        // Arrange
        var transferId = Guid.NewGuid();
        var original = ChunkHeader.CreateBatch(transferId, 99, 50, 1024);
        byte[] buffer = new byte[ChunkHeader.Size];

        // Act
        original.WriteTo(buffer);
        var read = ChunkHeader.ReadFrom(buffer);

        // Assert
        Assert.Equal(original.Version, read.Version);
        Assert.Equal(original.Type, read.Type);
        Assert.Equal(original.TransferId, read.TransferId);
        Assert.Equal(original.BatchId, read.BatchId);
        Assert.Equal(original.Count, read.Count);
        Assert.Equal(original.PayloadLength, read.PayloadLength);
    }

    [Fact]
    public async Task ChunkIO_WriteBatch_And_ReadChunk_RoundTrip()
    {
        // Arrange
        var transferId = Guid.NewGuid();
        var batchId = 42;
        var count = 10;
        var payload = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        
        using var ms = new MemoryStream();

        // Act - Write
        await ChunkIO.WriteBatchAsync(ms, transferId, batchId, count, payload);
        ms.Position = 0;
        
        // Act - Read
        using var chunk = await ChunkIO.ReadChunkAsync(ms);

        // Assert
        Assert.NotNull(chunk);
        Assert.Equal(ChunkType.Batch, chunk.Value.Header.Type);
        Assert.Equal(transferId, chunk.Value.Header.TransferId);
        Assert.Equal(batchId, chunk.Value.Header.BatchId);
        Assert.Equal(count, chunk.Value.Header.Count);
        Assert.Equal(payload, chunk.Value.Payload.ToArray());
    }

    [Fact]
    public async Task ChunkIO_WriteControl_And_ReadChunk_RoundTrip()
    {
        // Arrange
        var transferId = Guid.NewGuid();
        var batchId = 99;
        
        using var ms = new MemoryStream();

        // Act - Write
        await ChunkIO.WriteControlAsync(ms, transferId, batchId, ChunkType.BatchAck);
        ms.Position = 0;
        
        // Act - Read
        using var chunk = await ChunkIO.ReadChunkAsync(ms);

        // Assert
        Assert.NotNull(chunk);
        Assert.Equal(ChunkType.BatchAck, chunk.Value.Header.Type);
        Assert.Equal(transferId, chunk.Value.Header.TransferId);
        Assert.Equal(batchId, chunk.Value.Header.BatchId);
        Assert.True(chunk.Value.Payload.IsEmpty);
    }

    [Fact]
    public async Task ChunkIO_WriteComplete_And_ReadChunk_RoundTrip()
    {
        // Arrange
        var transferId = Guid.NewGuid();
        
        using var ms = new MemoryStream();

        // Act - Write Complete (control chunk)
        await ChunkIO.WriteControlAsync(ms, transferId, 0, ChunkType.Complete);
        ms.Position = 0;
        
        // Act - Read
        using var chunk = await ChunkIO.ReadChunkAsync(ms);

        // Assert
        Assert.NotNull(chunk);
        Assert.Equal(ChunkType.Complete, chunk.Value.Header.Type);
        Assert.Equal(transferId, chunk.Value.Header.TransferId);
        Assert.True(chunk.Value.Payload.IsEmpty);
    }

    [Fact]
    public async Task ChunkIO_ReadChunkAsync_EmptyStream_ReturnsNull()
    {
        // Arrange
        using var ms = new MemoryStream();

        // Act
        var result = await ChunkIO.ReadChunkAsync(ms);

        // Assert
        Assert.Null(result);
    }

    [Fact]
    public async Task ChunkIO_MultipleChunks_PreservesAll()
    {
        // Arrange
        var transferId = Guid.NewGuid();
        var payload1 = new byte[] { 1, 2, 3 };
        
        using var ms = new MemoryStream();

        // Act - Write all (batch, ack, complete sequence)
        await ChunkIO.WriteBatchAsync(ms, transferId, 0, 3, payload1);
        await ChunkIO.WriteControlAsync(ms, transferId, 0, ChunkType.BatchAck);
        await ChunkIO.WriteControlAsync(ms, transferId, 0, ChunkType.Complete);
        await ChunkIO.WriteControlAsync(ms, transferId, 0, ChunkType.CompleteAck);
        
        ms.Position = 0;

        // Read all
        using var chunk1 = await ChunkIO.ReadChunkAsync(ms);
        using var chunk2 = await ChunkIO.ReadChunkAsync(ms);
        using var chunk3 = await ChunkIO.ReadChunkAsync(ms);
        using var chunk4 = await ChunkIO.ReadChunkAsync(ms);
        var chunk5 = await ChunkIO.ReadChunkAsync(ms); // Should be null (EOF)

        // Assert
        Assert.Equal(ChunkType.Batch, chunk1!.Value.Header.Type);
        Assert.Equal(ChunkType.BatchAck, chunk2!.Value.Header.Type);
        Assert.Equal(ChunkType.Complete, chunk3!.Value.Header.Type);
        Assert.Equal(ChunkType.CompleteAck, chunk4!.Value.Header.Type);
        Assert.Null(chunk5);
    }

    [Fact]
    public void ChunkHeader_Size_Is30Bytes()
    {
        // Verify the header size is exactly 30 bytes as documented
        Assert.Equal(30, ChunkHeader.Size);
    }

    [Fact]
    public void ChunkHeader_CurrentVersion_Is2()
    {
        // Verify current version is 2
        Assert.Equal(2, ChunkHeader.CurrentVersion);
    }

    [Fact]
    public async Task ReadFromStreamAsync_UnsupportedVersion_ThrowsWireProtocolViolation()
    {
        // Arrange: a header written with a version this build no longer speaks.
        var header = ChunkHeader.CreateBatch(Guid.NewGuid(), 0, 1, 0) with { Version = 1 };
        byte[] buffer = new byte[ChunkHeader.Size];
        header.WriteTo(buffer);
        using var ms = new MemoryStream(buffer);

        // Act & Assert: fatal, not the generic exception a network hiccup would raise.
        await Assert.ThrowsAsync<WireProtocolViolationException>(() => ChunkHeader.ReadFromStreamAsync(ms));
    }

    [Fact]
    public void ReadFromStreamAsync_AllocatesNoBufferForTheHeader()
    {
        // Arrange: headers back to back in a memory stream, which completes every read at once, so a
        // read never leaves this thread and the counter below sees all it allocates.
        const int reads = 1_000;
        var header = new byte[ChunkHeader.Size];
        ChunkHeader.CreateControl(Guid.NewGuid(), 0, ChunkType.Ping).WriteTo(header);
        var stream = new MemoryStream();
        for (var i = 0; i < reads; i++) stream.Write(header);

        // A first pass leaves nothing to set up in the second: the code is compiled, the stream holds
        // the task it hands back for a read of a header's length, the pool has an array on this thread.
        ReadHeaders(stream, reads);

        // Act
        var before = GC.GetAllocatedBytesForCurrentThread();
        ReadHeaders(stream, reads);
        var perRead = (GC.GetAllocatedBytesForCurrentThread() - before) / reads;

        // Assert
        Assert.InRange(perRead, 0, MaxBytesPerHeaderRead);
    }

    /// <summary>
    /// What a read of a header may allocate: its task, and in a Debug build its state machine, which
    /// the compiler makes a class there. Halfway between that and the same plus an array of a
    /// header's size, so that one such array per read goes over it.
    /// </summary>
#if DEBUG
    private const long MaxBytesPerHeaderRead = 244;
#else
    private const long MaxBytesPerHeaderRead = 132;
#endif

    /// <summary>
    /// Reads <paramref name="count"/> headers from the start of <paramref name="stream"/>, allocating
    /// nothing besides what each read does.
    /// </summary>
    private static void ReadHeaders(MemoryStream stream, int count)
    {
        stream.Position = 0;
        for (var i = 0; i < count; i++)
        {
            var read = ChunkHeader.ReadFromStreamAsync(stream);
            Assert.True(read.IsCompletedSuccessfully);
            Assert.True(read.Result.HasValue);
        }
    }

    [Fact]
    public void ChunkType_HasExpectedValues()
    {
        // Verify chunk type values match protocol specification
        Assert.Equal(0x01, (byte)ChunkType.Batch);
        Assert.Equal(0x02, (byte)ChunkType.BatchAck);
        Assert.Equal(0x03, (byte)ChunkType.Complete);
        Assert.Equal(0x04, (byte)ChunkType.CompleteAck);
        Assert.Equal(0x05, (byte)ChunkType.Ping);
        Assert.Equal(0x06, (byte)ChunkType.Pong);
        Assert.Equal(0x07, (byte)ChunkType.Abort);
        Assert.Equal(0x08, (byte)ChunkType.StreamBatch);
    }

    [Fact]
    public async Task WriteBatchAsync_WithStreamBatch_RoundTripsTheTypeAndThePayload()
    {
        // Arrange
        var transferId = Guid.NewGuid();
        byte[] payload = [9, 8, 7, 6, 5];
        using var ms = new MemoryStream();

        // Act
        await ChunkIO.WriteBatchAsync(ms, transferId, ChunkType.StreamBatch, batchId: 4, count: payload.Length, payload);
        ms.Position = 0;
        using var chunk = await ChunkIO.ReadChunkAsync(ms);

        // Assert
        Assert.NotNull(chunk);
        Assert.Equal(ChunkType.StreamBatch, chunk.Value.Header.Type);
        Assert.Equal(ChunkHeader.CurrentVersion, chunk.Value.Header.Version);
        Assert.Equal(4, chunk.Value.Header.BatchId);
        Assert.Equal(payload.Length, chunk.Value.Header.Count);
        Assert.Equal(transferId, chunk.Value.Header.TransferId);
        Assert.Equal(payload, chunk.Value.Payload.ToArray());
    }

    [Fact]
    public async Task WriteBatchAsync_WithoutAType_StillWritesABatch()
    {
        using var ms = new MemoryStream();

        await ChunkIO.WriteBatchAsync(ms, Guid.NewGuid(), batchId: 0, count: 1, new byte[] { 1 });
        ms.Position = 0;
        using var chunk = await ChunkIO.ReadChunkAsync(ms);

        Assert.Equal(ChunkType.Batch, chunk!.Value.Header.Type);
    }

    [Theory]
    [InlineData(ChunkType.BatchAck)]
    [InlineData(ChunkType.Complete)]
    [InlineData(ChunkType.Abort)]
    [InlineData((ChunkType)0xFF)]
    public async Task WriteBatchAsync_WithANonBatchType_IsRefused(ChunkType type)
    {
        using var ms = new MemoryStream();

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            () => ChunkIO.WriteBatchAsync(ms, Guid.NewGuid(), type, batchId: 0, count: 1, new byte[] { 1 }));

        Assert.Equal(0, ms.Length);
    }

    [Fact]
    public async Task WriteBatchAsync_APayloadInSegments_PutsTheSameBytesOnTheWireAsInOneBlock()
    {
        var transferId = Guid.NewGuid();
        byte[] payload = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        var first = new Segment(payload[..3]);
        var last = first.Append(payload[3..4]).Append(payload[4..]);
        var segmented = new ReadOnlySequence<byte>(first, 0, last, last.Memory.Length);
        Assert.False(segmented.IsSingleSegment);

        using var inOneBlock = new MemoryStream();
        await ChunkIO.WriteBatchAsync(inOneBlock, transferId, ChunkType.StreamBatch, batchId: 7, count: payload.Length, payload);
        using var inSegments = new MemoryStream();
        await ChunkIO.WriteBatchAsync(inSegments, transferId, ChunkType.StreamBatch, batchId: 7, count: payload.Length, segmented);

        Assert.Equal(inOneBlock.ToArray(), inSegments.ToArray());
    }

    private sealed class Segment : ReadOnlySequenceSegment<byte>
    {
        public Segment(ReadOnlyMemory<byte> memory, long runningIndex = 0)
        {
            Memory = memory;
            RunningIndex = runningIndex;
        }

        public Segment Append(ReadOnlyMemory<byte> memory)
        {
            var next = new Segment(memory, RunningIndex + Memory.Length);
            Next = next;
            return next;
        }
    }
}
