using System.Buffers;
using System.Buffers.Binary;
using System.Threading.Channels;
using System.Text;
using TransportR.Client.SignalR.Sessions;
using TransportR.Client.SignalR.Configuration;
using TransportR.Interfaces;
using TransportR.Protocol;
using Microsoft.Extensions.Logging;
using Microsoft.IO;
using Microsoft.AspNetCore.SignalR.Client;

namespace TransportR.Tests;

public class BatchProcessorTests
{
    private readonly StubSessionContext _stubContext;
    private readonly Guid _transferId = Guid.NewGuid();

    public BatchProcessorTests()
    {
        _stubContext = new StubSessionContext();
    }

    private BatchProcessor<string> CreateProcessor(int batchSize = 100)
    {
        return new BatchProcessor<string>(_stubContext, _transferId.ToString(), batchSize);
    }

    [Fact]
    public async Task ResendUnackedBatches_WhileBatchesAreStillBeingRegistered_DoesNotFail()
    {
        // A stream opens while the serialization worker is still registering batches, which is every
        // stream of a sender that has not finished producing. The resend walks the batches that are
        // there; one that fails on the others' arrival ends the stream, and a session released at that
        // moment has no stream left to say so on.
        await using var processor = CreateProcessor(batchSize: 1);
        await processor.StartSenderAsync(CancellationToken.None);

        const int batches = 3000;
        for (var i = 0; i < batches; i++)
        {
            await processor.SendAsync("item", CancellationToken.None);
        }

        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(10);
        while (processor.UnackedCount < batches)
        {
            Assert.True(DateTime.UtcNow < deadline, "The batches were not all registered in time.");
            await processor.ResendUnackedBatchesAsync(Stream.Null, _transferId, false, -1, CancellationToken.None);
        }
    }

    [Fact]
    public async Task HandleBatchAck_ShouldRemoveUnackedBatch()
    {
        // Arrange
        await using var processor = CreateProcessor();
        await processor.StartSenderAsync(CancellationToken.None);

        // Act: Send a batch
        await processor.SendAsync("item1", CancellationToken.None);
        await processor.CompleteSenderAsync(CancellationToken.None);

        // Wait for batch to appear in OutgoingMessages
        await processor.OutgoingMessages.WaitToReadAsync();
        processor.OutgoingMessages.TryRead(out var msg);
        
        // Assert Initial State
        Assert.True(processor.HasUnackedBatches);
        Assert.Equal(1, processor.UnackedCount);

        // Act: Simulate receiving ACK for Batch 0
        var ackHeader = ChunkHeader.CreateControl(_transferId, 0, ChunkType.BatchAck);
        var ackChunk = new ChunkData(ackHeader, null, 0);
        
        await processor.ProcessChunkAsync(ackChunk);

        // Assert Final State
        Assert.False(processor.HasUnackedBatches);
        Assert.Equal(0, processor.UnackedCount);
        Assert.Equal(1, processor.AcksReceived);
    }

    [Fact]
    public async Task ResendUnackedBatches_ShouldFilterAlreadyReceived()
    {
        // Arrange
        await using var processor = CreateProcessor();
        await processor.StartSenderAsync(CancellationToken.None);

        // Send 3 batches
        for (int i = 0; i < 300; i++) await processor.SendAsync($"item{i}", CancellationToken.None);
        await processor.CompleteSenderAsync(CancellationToken.None);

        // Wait for all 3 batches to be serialized and put in Unacked
        // We read from channel to ensure they are processed
        for(int i=0; i<3; i++)
        {
             await processor.OutgoingMessages.WaitToReadAsync();
             processor.OutgoingMessages.TryRead(out _);
        }
        
        // Ensure we have 3 unacked
        Assert.Equal(3, processor.UnackedCount);

        using var outputStream = new MemoryStream();
        
        // Act: Resend unacked batches, claiming Receiver NextExpected=1
        // Sender has 0, 1, 2. Receiver wants 1. So send 1, 2.
        await processor.ResendUnackedBatchesAsync(
            outputStream, 
            _transferId, 
            didPingPong: true, 
            nextExpectedFromPong: 1, 
            token: CancellationToken.None);

        // Assert
        Assert.Equal(2, processor.UnackedCount); // 0 removed
        Assert.True(outputStream.Length > 0);
        
        outputStream.Position = 0;
        
        // Read Chunk 1
        var chunk1 = await ChunkIO.ReadChunkAsync(outputStream);
        Assert.NotNull(chunk1);
        Assert.Equal(1, chunk1.Value.Header.BatchId);
        
        // Read Chunk 2
        var chunk2 = await ChunkIO.ReadChunkAsync(outputStream);
        Assert.NotNull(chunk2);
        Assert.Equal(2, chunk2.Value.Header.BatchId);
        
        // Should be end of stream
        var chunk3 = await ChunkIO.ReadChunkAsync(outputStream);
        Assert.Null(chunk3);
    }

    [Fact]
    public async Task CompleteSender_ShouldWaitForCompleteAck()
    {
        // Arrange
        await using var processor = CreateProcessor();
        await processor.SendCompleteSignalAsync();
        
        var completeTask = processor.WaitForCompleteAckAsync(CancellationToken.None);
        
        // Assert: Task should not be completed yet
        Assert.False(completeTask.IsCompleted);
        
        // Act: Simulate CompleteAck reception
        var ackHeader = ChunkHeader.CreateControl(_transferId, 0, ChunkType.CompleteAck);
        var ackChunk = new ChunkData(ackHeader, null, 0);
        
        await processor.ProcessChunkAsync(ackChunk);
        
        // Assert: Task should complete
        await completeTask;
        Assert.True(completeTask.IsCompletedSuccessfully);
    }

    [Fact]
    public async Task CompleteAck_ThatLandsBetweenTwoCompleteRetries_IsNotLost()
    {
        // The receiver acknowledges once and stops reading, so an acknowledgement that arrives after a
        // retry's wait has run out, but before the next Complete goes out, is the only one there is.
        await using var processor = CreateProcessor();
        await processor.SendCompleteSignalAsync();

        var ackHeader = ChunkHeader.CreateControl(_transferId, 0, ChunkType.CompleteAck);
        await processor.ProcessChunkAsync(new ChunkData(ackHeader, null, 0));

        await processor.SendCompleteSignalAsync();

        Assert.True(processor.WaitForCompleteAckAsync(CancellationToken.None).IsCompletedSuccessfully);
    }

    [Fact]
    public async Task ProcessChunk_UnknownChunkType_ThrowsWireProtocolViolation()
    {
        // Arrange
        await using var processor = CreateProcessor();
        await processor.StartReceiverAsync();

        var unknownHeader = ChunkHeader.CreateControl(_transferId, 0, (ChunkType)0xFF);
        var unknownChunk = new ChunkData(unknownHeader, null, 0);

        // Act & Assert: an unrecognized type is fatal, silently ignoring it would leave the peer
        // waiting on a reply this side never sends.
        await Assert.ThrowsAsync<WireProtocolViolationException>(() => processor.ProcessChunkAsync(unknownChunk));
    }

    // ---- Receive capacity (ClientOptions.ReceiveBufferCapacity) ----

    /// <summary>
    /// Builds the chunk the stub serializer expects: a 4-byte count, then ten bytes per item.
    /// </summary>
    private ChunkData CreateBatchChunk(int batchId, int itemCount)
    {
        var payload = new byte[4 + 10 * itemCount];
        BinaryPrimitives.WriteInt32LittleEndian(payload, itemCount);
        var owner = new ArrayMemoryOwner(payload);
        return new ChunkData(
            ChunkHeader.CreateBatch(_transferId, batchId, itemCount, payload.Length), owner, payload.Length);
    }

    [Fact]
    public async Task ProcessChunk_WhenTheByteBudgetIsFull_ParksTheReadLoop()
    {
        // Arrange: a budget of one chunk. The item count is nowhere near its own bound, which is
        // the whole point - this is the case ten thousand items never caught.
        _stubContext.Options.ReceiveBufferCapacity = BufferCapacity.ItemsAndBytes(10_000, 24);
        await using var processor = CreateProcessor();
        await processor.StartReceiverAsync();

        await processor.ProcessChunkAsync(CreateBatchChunk(batchId: 0, itemCount: 2));

        // Act: the second chunk has no room.
        var blocked = processor.ProcessChunkAsync(CreateBatchChunk(batchId: 1, itemCount: 2));

        // Assert: parked. This is what stops the read loop draining the HTTP body, which is what
        // pushes back to the sender.
        Assert.NotSame(blocked, await Task.WhenAny(blocked, Task.Delay(300)));

        // Act: the consumer takes the first chunk's items.
        var enumerator = processor.ReceiveAsync(CancellationToken.None).GetAsyncEnumerator();
        await enumerator.MoveNextAsync();
        await enumerator.MoveNextAsync();
        await enumerator.MoveNextAsync();

        // Assert: the read loop resumes.
        await blocked.WaitAsync(TimeSpan.FromSeconds(5));
        await enumerator.DisposeAsync();
    }

    [Fact]
    public void ReceiveCapacity_IsUnboundedByDefault()
    {
        // The default is no bound at all, matching MaxInFlightBatches at the other end. A
        // default that silently bounded would be a guess, and this is the test that keeps one
        // from creeping back in.
        Assert.Null(new ClientOptions().ReceiveBufferCapacity);
    }

    [Fact]
    public async Task ProcessChunk_WithNoCapacityConfigured_NeverParksTheReadLoop()
    {
        // Arrange: the default, which bounds nothing.
        await using var processor = CreateProcessor();
        await processor.StartReceiverAsync();

        // Act: far more than any bound, consumed by nobody.
        var pump = Task.Run(async () =>
        {
            for (var i = 0; i < 500; i++)
                await processor.ProcessChunkAsync(CreateBatchChunk(batchId: i, itemCount: 50));
        });

        // Assert
        Assert.Same(pump, await Task.WhenAny(pump, Task.Delay(TimeSpan.FromSeconds(5))));
        await pump;
        Assert.Equal(25000, processor.ItemsReceived);
    }

    [Fact]
    public async Task ProcessChunk_WithinTheBudget_DoesNotParkTheReadLoop()
    {
        // Arrange: a capacity no test-sized chunk comes near.
        _stubContext.Options.ReceiveBufferCapacity = BufferCapacity.ItemsAndBytes(10_000, 1 << 20);
        await using var processor = CreateProcessor();
        await processor.StartReceiverAsync();

        // Act
        var pump = Task.Run(async () =>
        {
            for (var i = 0; i < 20; i++)
                await processor.ProcessChunkAsync(CreateBatchChunk(batchId: i, itemCount: 5));
        });

        // Assert
        Assert.Same(pump, await Task.WhenAny(pump, Task.Delay(TimeSpan.FromSeconds(5))));
        await pump;
        Assert.Equal(100, processor.ItemsReceived);
    }

    [Fact]
    public async Task DisposeAsync_ReleasesAReadLoopParkedOnTheByteGate()
    {
        // Arrange: a budget of one chunk, filled.
        _stubContext.Options.ReceiveBufferCapacity = BufferCapacity.ItemsAndBytes(10_000, 24);
        var processor = CreateProcessor();
        await processor.StartReceiverAsync();
        await processor.ProcessChunkAsync(CreateBatchChunk(batchId: 0, itemCount: 2));

        var blocked = processor.ProcessChunkAsync(CreateBatchChunk(batchId: 1, itemCount: 2));
        Assert.NotSame(blocked, await Task.WhenAny(blocked, Task.Delay(300)));

        // Act: the session goes away while the consumer is behind. The parked loop cannot reach
        // the finally that would otherwise complete the buffer, so disposal has to do it.
        await processor.DisposeAsync();

        // Assert
        await Assert.ThrowsAsync<ChannelClosedException>(() => blocked.WaitAsync(TimeSpan.FromSeconds(5)));
    }

    private sealed class ArrayMemoryOwner(byte[] data) : IMemoryOwner<byte>
    {
        public Memory<byte> Memory => data;
        public void Dispose() { }
    }

    // ========== STUBS ==========

    private class StubSessionContext : ISessionContext<string>
    {
        public ILogger Logger { get; } = new StubLogger();
        public IBatchSerializer<string> Serializer { get; } = new StubBatchSerializer();
        // Both null: BatchProcessor reaches neither. The transport lives behind
        // HttpStreamManager, which these tests do not start.
        public HttpClient HttpClient => null!;
        public HubConnection HubConnection => null!;
        public int BatchFlushIntervalMs => 50;
        public RecyclableMemoryStreamManager StreamManager { get; } = new RecyclableMemoryStreamManager();
        public ClientOptions Options { get; } = new ClientOptions();
        public void RemoveSession(string transferId) { }

        public Task<string?> GetAccessTokenAsync(bool forceRefresh = false) => Task.FromResult<string?>(null);
    }

    private class StubLogger : ILogger, IDisposable
    {
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) { }
        public bool IsEnabled(LogLevel logLevel) => true;
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => this;
        public void Dispose() { }
    }

    private class StubBatchSerializer : IBatchSerializer<string>
    {
        public Task SerializeBatchAsync(Stream destination, List<string> batch)
        {
            // Simple string serialization: Count + Items
            // 4 bytes count, then just placeholder bytes for items
            using var writer = new BinaryWriter(destination, Encoding.UTF8, leaveOpen: true);
            writer.Write(batch.Count);
            // Just write dummy bytes for content
            writer.Write(new byte[10 * batch.Count]); 
            return Task.CompletedTask;
        }

        public Task<List<string>?> DeserializeBatchAsync(Stream source)
        {
            using var reader = new BinaryReader(source, Encoding.UTF8, leaveOpen: true);
            var count = reader.ReadInt32();
            var bytes = reader.ReadBytes(10 * count);
            var list = new List<string>(); 
            // Return empty items just to match count
            for(int i=0; i<count; i++) list.Add(default!);
            return Task.FromResult<List<string>?>(list);
        }
    }

    // ---- In-flight window (ClientOptions.MaxInFlightBatches) ----

    private BatchProcessor<string> CreateWindowedProcessor(int maxInFlight, int batchSize = 1)
    {
        _stubContext.Options.MaxInFlightBatches = maxInFlight;
        return CreateProcessor(batchSize);
    }

    [Fact]
    public async Task SendAsync_WhenWindowIsFull_ParksTheProducerUntilAcked()
    {
        // Arrange: one item per batch, window of two.
        await using var processor = CreateWindowedProcessor(maxInFlight: 2);
        await processor.StartSenderAsync(CancellationToken.None);

        await processor.SendAsync("a", CancellationToken.None);
        await processor.SendAsync("b", CancellationToken.None);

        // Act: the third batch has no slot.
        var blocked = processor.SendAsync("c", CancellationToken.None);

        // Assert: parked. Draining the outgoing channel is not what reopens the window - only an
        // acknowledgement is, which is the whole point of measuring in-flight rather than queued.
        while (processor.UnackedCount < 2) await Task.Delay(10);
        processor.OutgoingMessages.TryRead(out _);
        processor.OutgoingMessages.TryRead(out _);
        Assert.NotSame(blocked, await Task.WhenAny(blocked, Task.Delay(300)));

        // Act: acknowledge batch 0.
        await processor.ProcessChunkAsync(
            new ChunkData(ChunkHeader.CreateControl(_transferId, 0, ChunkType.BatchAck), null, 0));

        // Assert: the producer resumes.
        await blocked.WaitAsync(TimeSpan.FromSeconds(5));
    }

    [Fact]
    public async Task SendAsync_WithoutWindow_DoesNotParkTheProducer()
    {
        // Arrange: no window configured - the default.
        await using var processor = CreateProcessor(batchSize: 1);
        await processor.StartSenderAsync(CancellationToken.None);

        // Act: send far more than any window would allow, acknowledging nothing.
        var produce = Task.Run(async () =>
        {
            for (var i = 0; i < 50; i++)
                await processor.SendAsync($"item{i}", CancellationToken.None);
        });

        // Assert
        Assert.Same(produce, await Task.WhenAny(produce, Task.Delay(TimeSpan.FromSeconds(5))));
        await produce;
    }

    [Fact]
    public async Task ResendUnackedBatches_ImplicitAck_GivesTheSlotBack()
    {
        // Arrange: window of two, both taken.
        await using var processor = CreateWindowedProcessor(maxInFlight: 2);
        await processor.StartSenderAsync(CancellationToken.None);

        await processor.SendAsync("a", CancellationToken.None);
        await processor.SendAsync("b", CancellationToken.None);
        await ReadOutgoingAsync(processor, 2);

        var blocked = processor.SendAsync("c", CancellationToken.None);
        Assert.NotSame(blocked, await Task.WhenAny(blocked, Task.Delay(200)));

        // Act: after a cut, the Pong says the receiver already has batch 0. That is an implicit
        // ack - if it does not return its slot, the window shrinks by one on every resumed batch
        // and never reopens.
        using var outputStream = new MemoryStream();
        await processor.ResendUnackedBatchesAsync(
            outputStream,
            _transferId,
            didPingPong: true,
            nextExpectedFromPong: 1,
            token: CancellationToken.None);

        // Assert: the slot came back, and the batch parked on it was registered and handed to the
        // channel. The count is read after that and not before: the slot lets the parked producer
        // run, so the count is 1 until its batch registers and 2 after, and which one a read sees
        // is a matter of scheduling.
        await blocked.WaitAsync(TimeSpan.FromSeconds(5));
        await ReadOutgoingAsync(processor, 1);

        // Batch 0 went with the implicit ack; batches 1 and 2 are left.
        Assert.Equal(2, processor.UnackedCount);
    }

    // A batch reaches the outgoing channel after it is registered, so reading it is waiting for its
    // registration. Nothing else reads the channel in these tests.
    private static async Task ReadOutgoingAsync(BatchProcessor<string> processor, int count)
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        for (var i = 0; i < count; i++)
        {
            await processor.OutgoingMessages.ReadAsync(deadline.Token);
        }
    }
}
