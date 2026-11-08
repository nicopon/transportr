using System.Buffers;
using System.Buffers.Binary;
using System.Runtime.InteropServices;
using TransportR.Client.Batching;
using TransportR.Client.Sessions;
using TransportR.Protocol;
using TransportR.Tests.TestHelpers;

namespace TransportR.Tests;

/// <summary>
/// The memory a chunk is read into goes from the read to the processor, and on to the receiver state,
/// without a copy, and every way it can end returns it exactly once: a buffer returned twice is handed
/// to two renters, and nothing throws when it happens. What happens past the receiver state is
/// <see cref="ReceiverStateOwnershipTests"/>'.
/// </summary>
public class ReceivedChunkOwnershipTests
{
    private readonly Guid _transferId = Guid.NewGuid();
    private readonly StubSessionContext _context = new();

    private TestByteBatchProcessor ByteProcessor() =>
        new(_context, _transferId.ToString(),
            new ByteBatchSource(1024, _context.Options.BatchFlushIntervalMs, _context.StreamManager),
            new ByteReceivePort(_context.Options.ReceiveBufferCapacity));

    private TestBatchProcessor<string> ItemProcessor()
    {
        var (send, receive) = _context.CreatePorts(batchSize: 100);
        return new TestBatchProcessor<string>(_context, _transferId.ToString(), send, receive);
    }

    private ChunkHeader Batch(int batchId, ChunkType type, int length) =>
        ChunkHeader.CreateBatch(_transferId, batchId, count: 1, length) with { Type = type };

    [Fact]
    public async Task ReadOwnedChunk_HandsOverMemoryOfExactlyThePayloadsLength()
    {
        byte[] payload = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        using var stream = new MemoryStream();
        await ChunkIO.WriteBatchAsync(stream, _transferId, ChunkType.StreamBatch, batchId: 3, count: payload.Length, payload);
        stream.Position = 0;

        var read = await ChunkIO.ReadOwnedChunkAsync(stream, CancellationToken.None);

        Assert.True(read.HasValue);
        var (header, owner) = read.Value;
        Assert.NotNull(owner);
        using (owner)
        {
            Assert.Equal(3, header.BatchId);
            Assert.Equal(payload, owner.Memory.ToArray());
        }
    }

    [Fact]
    public void ReadOwnedChunk_EndingInThePayload_ReturnsItsMemoryToThePool()
    {
        // Arrange: a header announcing more than the stream holds. The pool hands this thread back the
        // last array it returned, so the read rents the probe, and only a return hands it out again.
        var stream = new MemoryStream();
        var header = new byte[ChunkHeader.Size];
        ChunkHeader.CreateBatch(_transferId, 0, count: 1, payloadLength: 5000).WriteTo(header);
        stream.Write(header);
        stream.Write(new byte[10]);
        stream.Position = 0;
        var probe = ArrayPool<byte>.Shared.Rent(5000);
        ArrayPool<byte>.Shared.Return(probe);

        // Act: a memory stream completes every read at once, so nothing leaves this thread.
        var read = ChunkIO.ReadOwnedChunkAsync(stream, CancellationToken.None);

        // Assert
        Assert.True(read.IsCompleted);
        Assert.Throws<EndOfStreamException>(() => read.GetAwaiter().GetResult());
        Assert.Same(probe, ArrayPool<byte>.Shared.Rent(5000));
    }

    [Fact]
    public async Task ABatchAByteSinkTakes_ReachesTheConsumerInTheMemoryItWasReadInto_AndIsReturnedOnceByIt()
    {
        await using var processor = ByteProcessor();
        await processor.StartReceiverAsync();
        byte[] bytes = [7, 7, 7, 7];
        var owner = new CountingOwner(bytes);

        await processor.ProcessChunkAsync(Batch(0, ChunkType.StreamBatch, bytes.Length), owner);
        processor.CompleteReceiver();

        await foreach (var chunk in processor.ReceiveChunksAsync(CancellationToken.None))
        {
            Assert.True(MemoryMarshal.TryGetArray(chunk.Memory, out var segment));
            Assert.Same(bytes, segment.Array);
            Assert.Equal(0, owner.Disposals);
            chunk.Dispose();
        }

        Assert.Equal(1, owner.Disposals);
    }

    [Fact]
    public async Task ABatchAnItemSinkDecodes_IsReturnedOnceDecoded()
    {
        await using var processor = ItemProcessor();
        await processor.StartReceiverAsync();
        var body = new byte[sizeof(int) + 1];
        BinaryPrimitives.WriteInt32LittleEndian(body, 1);
        var owner = new CountingOwner(body);

        await processor.ProcessChunkAsync(Batch(0, ChunkType.Batch, body.Length), owner);

        Assert.Equal(1, owner.Disposals);
        processor.CompleteReceiver();
        var items = 0;
        await foreach (var _ in processor.ReceiveAsync(CancellationToken.None)) items++;
        Assert.Equal(1, items);
    }

    [Fact]
    public async Task ABatchOfTheWrongKind_IsReturnedOnce_AndNothingIsDelivered()
    {
        await using var processor = ByteProcessor();
        await processor.StartReceiverAsync();
        var owner = new CountingOwner([1, 2, 3, 4]);

        await Assert.ThrowsAsync<UndecodableBatchException>(
            () => processor.ProcessChunkAsync(Batch(0, ChunkType.Batch, 4), owner));

        Assert.Equal(1, owner.Disposals);
        processor.CompleteReceiver();
        await foreach (var _ in processor.ReceiveChunksAsync(CancellationToken.None)) Assert.Fail("Nothing should have been delivered.");
    }

    [Fact]
    public async Task ABatchBeforeTheReceiverStarts_IsReturnedOnce()
    {
        await using var processor = ByteProcessor();
        var owner = new CountingOwner([1, 2, 3, 4]);

        await processor.ProcessChunkAsync(Batch(0, ChunkType.StreamBatch, 4), owner);

        Assert.Equal(1, owner.Disposals);
    }

    [Theory]
    [InlineData(ChunkType.Pong)]
    [InlineData((ChunkType)0xFF)]
    public async Task AChunkThatIsNotABatch_HasItsPayloadReturnedOnce(ChunkType type)
    {
        await using var processor = ByteProcessor();
        await processor.StartReceiverAsync();
        var owner = new CountingOwner([1, 2, 3, 4]);
        var header = ChunkHeader.CreateControl(_transferId, 0, type) with { PayloadLength = 4 };

        try
        {
            await processor.ProcessChunkAsync(header, owner);
        }
        catch (WireProtocolViolationException) when (type != ChunkType.Pong)
        {
        }

        Assert.Equal(1, owner.Disposals);
    }

    [Fact]
    public async Task ABatchHeldOutOfOrder_IsReturnedOnceWhenTheProcessorIsDisposed()
    {
        var processor = ByteProcessor();
        await processor.StartReceiverAsync();
        var owner = new CountingOwner([1, 2, 3, 4]);

        await processor.ProcessChunkAsync(Batch(1, ChunkType.StreamBatch, 4), owner);
        Assert.Equal(0, owner.Disposals);

        await processor.DisposeAsync();

        Assert.Equal(1, owner.Disposals);
    }
}
