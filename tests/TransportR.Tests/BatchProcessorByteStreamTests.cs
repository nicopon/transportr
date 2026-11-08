using System.Buffers;
using Microsoft.Extensions.Logging;
using Microsoft.IO;
using TransportR.Abstractions;
using TransportR.Client.Batching;
using TransportR.Client.Configuration;
using TransportR.Client.Internal;
using TransportR.Client.Sessions;
using TransportR.Interfaces;
using TransportR.Protocol;
using TransportR.Tests.TestHelpers;

namespace TransportR.Tests;

/// <summary>
/// The byte-stream mode through <see cref="BatchProcessor{T}"/>, receiving and sending. How the
/// session reacts to the refusals is <c>DataSession</c>'s, covered end to end with the public surface.
/// </summary>
public class BatchProcessorByteStreamTests
{
    private readonly ByteStreamSessionContext _context = new();
    private readonly Guid _transferId = Guid.NewGuid();

    private TestByteBatchProcessor CreateProcessor()
    {
        _context.BatchBytes = 1024;
        return _context.NewProcessor(_transferId.ToString());
    }

    private ChunkData CreateChunk(int batchId, byte[] payload, ChunkType type = ChunkType.StreamBatch)
    {
        var header = ChunkHeader.CreateBatch(_transferId, batchId, payload.Length, payload.Length) with { Type = type };
        return new ChunkData(header, new ArrayOwner(payload), payload.Length);
    }

    private static byte[] Bytes(byte fill, int length) => Enumerable.Repeat(fill, length).ToArray();

    private static async Task<T> Within<T>(Task<T> task) => await task.WaitAsync(TimeSpan.FromSeconds(5));

    [Fact]
    public async Task ReceiveAsync_ReturnsOneByteArrayPerBatch_InOrder()
    {
        await using var processor = CreateProcessor();
        await processor.StartReceiverAsync();

        await processor.ProcessChunkAsync(CreateChunk(0, Bytes(1, 10)));
        await processor.ProcessChunkAsync(CreateChunk(1, Bytes(2, 3)));
        processor.CompleteReceiver();

        var received = new List<byte[]>();
        await foreach (var batch in processor.ReceiveAsync(CancellationToken.None)) received.Add(batch);

        Assert.Equal(2, received.Count);
        Assert.Equal(Bytes(1, 10), received[0]);
        Assert.Equal(Bytes(2, 3), received[1]);
    }

    [Fact]
    public async Task ReceiveAsync_DeliversInOrderABatchThatArrivedEarly()
    {
        await using var processor = CreateProcessor();
        await processor.StartReceiverAsync();

        await processor.ProcessChunkAsync(CreateChunk(1, Bytes(2, 4)));
        await processor.ProcessChunkAsync(CreateChunk(0, Bytes(1, 4)));
        processor.CompleteReceiver();

        var received = new List<byte[]>();
        await foreach (var batch in processor.ReceiveAsync(CancellationToken.None)) received.Add(batch);

        Assert.Equal([Bytes(1, 4), Bytes(2, 4)], received);
    }

    [Fact]
    public async Task ReceiveChunksAsync_GivesTheChunksThemselves_ValidUntilTheConsumerDisposesThem()
    {
        await using var processor = CreateProcessor();
        await processor.StartReceiverAsync();
        await processor.ProcessChunkAsync(CreateChunk(0, Bytes(5, 8)));
        processor.CompleteReceiver();

        var chunks = new List<ByteChunk>();
        await foreach (var chunk in processor.ReceiveChunksAsync(CancellationToken.None)) chunks.Add(chunk);

        var only = Assert.Single(chunks);
        Assert.Equal(Bytes(5, 8), only.Memory.ToArray());
        only.Dispose();
        Assert.Throws<ObjectDisposedException>(() => only.Memory);
    }

    [Fact]
    public async Task ProcessChunk_AnItemBatchInByteStreamMode_IsRefusedAsUndecodable_AndNothingIsDelivered()
    {
        await using var processor = CreateProcessor();
        await processor.StartReceiverAsync();

        var refused = await Assert.ThrowsAsync<UndecodableBatchException>(
            () => processor.ProcessChunkAsync(CreateChunk(0, Bytes(1, 4), ChunkType.Batch)));

        // Generic: says nothing of the wire types or the serializer.
        Assert.DoesNotContain("StreamBatch", refused.Message);
        Assert.DoesNotContain("ChunkType", refused.Message);
        Assert.DoesNotContain("erializ", refused.Message);
        Assert.Null(refused.InnerException);
        processor.CompleteReceiver();
        await foreach (var _ in processor.ReceiveChunksAsync(CancellationToken.None)) Assert.Fail("Nothing should have been delivered.");
        Assert.Equal(0, processor.NextExpectedBatchId);
    }

    [Fact]
    public async Task StartReceiverAsync_AgainAfterAReconnection_KeepsFeedingTheConsumersEnumeration()
    {
        await using var processor = CreateProcessor();
        await processor.StartReceiverAsync();
        await using var enumerator = processor.ReceiveAsync(CancellationToken.None).GetAsyncEnumerator();
        var first = enumerator.MoveNextAsync().AsTask();
        await processor.ProcessChunkAsync(CreateChunk(0, Bytes(1, 4)));
        Assert.True(await Within(first));

        await processor.StartReceiverAsync();
        await processor.ProcessChunkAsync(CreateChunk(1, Bytes(2, 4)));

        Assert.True(await Within(enumerator.MoveNextAsync().AsTask()));
        Assert.Equal(Bytes(2, 4), enumerator.Current);
    }

    [Fact]
    public async Task ProcessChunk_WhenTheByteBudgetIsFull_ParksTheReadLoopUntilTheConsumerComesBack()
    {
        // Arrange: a budget of one 20-byte chunk.
        _context.Options.ReceiveBufferCapacity = BufferCapacity.ItemsAndBytes(10_000, 24);
        await using var processor = CreateProcessor();
        await processor.StartReceiverAsync();
        await processor.ProcessChunkAsync(CreateChunk(0, Bytes(1, 20)));

        var blocked = processor.ProcessChunkAsync(CreateChunk(1, Bytes(2, 20)));
        Assert.NotSame(blocked, await Task.WhenAny(blocked, Task.Delay(300)));

        // Act: the consumer takes the first batch, then asks for the next, which gives its room back.
        await using var enumerator = processor.ReceiveAsync(CancellationToken.None).GetAsyncEnumerator();
        Assert.True(await Within(enumerator.MoveNextAsync().AsTask()));
        var second = enumerator.MoveNextAsync().AsTask();

        // Assert
        await blocked.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.True(await Within(second));
        Assert.Equal(Bytes(2, 20), enumerator.Current);
    }

    [Fact]
    public async Task ProcessChunk_WhenTheParkedWaitIsCancelled_FailsTheWriteAndDeliversNothingOfIt()
    {
        _context.Options.ReceiveBufferCapacity = BufferCapacity.ItemsAndBytes(10_000, 24);
        await using var processor = CreateProcessor();
        await processor.StartReceiverAsync();
        await processor.ProcessChunkAsync(CreateChunk(0, Bytes(1, 20)));
        using var cts = new CancellationTokenSource();
        var blocked = processor.ProcessChunkAsync(CreateChunk(1, Bytes(2, 20)), cts.Token);
        Assert.NotSame(blocked, await Task.WhenAny(blocked, Task.Delay(200)));

        await cts.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => blocked);
        processor.CompleteReceiver();

        var received = new List<byte[]>();
        await foreach (var batch in processor.ReceiveAsync(CancellationToken.None)) received.Add(batch);
        Assert.Equal([Bytes(1, 20)], received);
        Assert.Equal(1, processor.NextExpectedBatchId);
    }

    [Fact]
    public async Task DisposeAsync_DropsWhatTheConsumerHasNotTaken_AndLeavesWhatItHolds()
    {
        // Arrange: three chunks admitted, the consumer holds the first.
        await using var processor = CreateProcessor();
        await processor.StartReceiverAsync();
        for (var i = 0; i < 3; i++) await processor.ProcessChunkAsync(CreateChunk(i, Bytes((byte)(i + 1), 4)));
        await using var chunks = processor.ReceiveChunksAsync(CancellationToken.None).GetAsyncEnumerator();
        Assert.True(await Within(chunks.MoveNextAsync().AsTask()));
        var held = chunks.Current;

        // Act: the session is released.
        await processor.DisposeAsync();

        // Assert: the entries nobody took are gone, and the one in hand is still readable.
        Assert.False(await Within(chunks.MoveNextAsync().AsTask()));
        Assert.Equal(Bytes(1, 4), held.Memory.ToArray());
        held.Dispose();
    }

    [Fact]
    public async Task CompleteReceiver_LeavesWhatWasAdmittedToTheConsumer()
    {
        await using var processor = CreateProcessor();
        await processor.StartReceiverAsync();
        await processor.ProcessChunkAsync(CreateChunk(0, Bytes(1, 4)));
        await processor.ProcessChunkAsync(CreateChunk(1, Bytes(2, 4)));

        processor.CompleteReceiver();

        var count = 0;
        await foreach (var batch in processor.ReceiveAsync(CancellationToken.None)) count += batch.Length;
        Assert.Equal(8, count);
    }

    [Fact]
    public async Task DisposeAsync_ReleasesAReadLoopParkedOnTheByteGate()
    {
        _context.Options.ReceiveBufferCapacity = BufferCapacity.ItemsAndBytes(10_000, 24);
        var processor = CreateProcessor();
        await processor.StartReceiverAsync();
        await processor.ProcessChunkAsync(CreateChunk(0, Bytes(1, 20)));
        var blocked = processor.ProcessChunkAsync(CreateChunk(1, Bytes(2, 20)));
        Assert.NotSame(blocked, await Task.WhenAny(blocked, Task.Delay(300)));

        await processor.DisposeAsync();

        await Assert.ThrowsAnyAsync<Exception>(() => blocked.WaitAsync(TimeSpan.FromSeconds(5)));
    }

    [Fact]
    public async Task ReceiveAsync_BeforeTheReceiverStarts_Throws()
    {
        await using var processor = CreateProcessor();

        Assert.Throws<InvalidOperationException>(() => processor.ReceiveAsync(CancellationToken.None));
        Assert.Throws<InvalidOperationException>(() => processor.ReceiveChunksAsync(CancellationToken.None));
    }

    // ---- Sending ----

    private async Task<TestByteBatchProcessor> StartSenderAsync(int budget = 4, int? window = null)
    {
        _context.BatchBytes = budget;
        _context.Options.MaxInFlightBatches = window;
        var processor = _context.NewProcessor(_transferId.ToString());
        await processor.StartSenderAsync(CancellationToken.None);
        return processor;
    }

    private static byte[] Sequence(int length, byte from = 1) =>
        Enumerable.Range(0, length).Select(i => (byte)(from + i)).ToArray();

    /// <summary>
    /// Writes the next <paramref name="count"/> batches the processor queued, as the write loop does,
    /// and reads them back off that wire.
    /// </summary>
    private async Task<List<(ChunkHeader Header, byte[] Body)>> WriteQueuedAsync(TestByteBatchProcessor processor, int count)
    {
        using var wire = new MemoryStream();
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        for (var i = 0; i < count; i++)
        {
            var message = Assert.IsType<BatchControlMessage>(await processor.OutgoingMessages.ReadAsync(deadline.Token));
            Assert.True(await processor.WriteBatchAsync(wire, _transferId, message.BatchId, message.Entry, deadline.Token));
        }
        return await ReadWireAsync(wire);
    }

    /// <summary>
    /// Writes the next <paramref name="count"/> batches the processor queued and then gives back the
    /// reference each message held, as the write loop does.
    /// </summary>
    private async Task WriteAsTheWriteLoopAsync(TestByteBatchProcessor processor, int count)
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        for (var i = 0; i < count; i++)
        {
            var message = Assert.IsType<BatchControlMessage>(await processor.OutgoingMessages.ReadAsync(deadline.Token));
            Assert.True(await processor.WriteBatchAsync(Stream.Null, _transferId, message.BatchId, message.Entry, deadline.Token));
            message.Entry.Release();
        }
    }

    private static async Task<List<(ChunkHeader Header, byte[] Body)>> ReadWireAsync(MemoryStream wire)
    {
        wire.Position = 0;
        var chunks = new List<(ChunkHeader, byte[])>();
        while (await ChunkIO.ReadChunkAsync(wire) is { } chunk)
        {
            using (chunk) chunks.Add((chunk.Header, chunk.Payload.ToArray()));
        }
        return chunks;
    }

    private static async Task WaitUntil(Func<bool> condition)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(5);
        while (!condition())
        {
            Assert.True(DateTime.UtcNow < deadline, "The condition was not met in time.");
            await Task.Delay(10);
        }
    }

    private Task AckAsync(TestByteBatchProcessor processor, int batchId) =>
        processor.ProcessChunkAsync(new ChunkData(ChunkHeader.CreateControl(_transferId, batchId, ChunkType.BatchAck), null, 0));

    [Fact]
    public async Task StartSenderAsync_InByteStreamMode_SendsWhatItIsGivenAsAStreamBatch()
    {
        await using var processor = await StartSenderAsync();

        await processor.SendAsync([7], CancellationToken.None);
        await processor.CompleteSenderAsync(CancellationToken.None);

        var chunk = Assert.Single(await WriteQueuedAsync(processor, 1));
        Assert.Equal(ChunkType.StreamBatch, chunk.Header.Type);
        Assert.Equal([7], chunk.Body);
    }

    [Fact]
    public async Task SendAsync_InByteStreamMode_SendsStreamBatchesOfAtMostTheBudget_CountedInBytes()
    {
        await using var processor = await StartSenderAsync(budget: 4);
        var bytes = Sequence(9);

        await processor.SendAsync(bytes[..3], CancellationToken.None);
        await processor.SendAsync(bytes[3..8], CancellationToken.None);
        await processor.SendAsync(bytes[8..], CancellationToken.None);
        await processor.CompleteSenderAsync(CancellationToken.None);

        var chunks = await WriteQueuedAsync(processor, 3);
        Assert.All(chunks, c => Assert.Equal(ChunkType.StreamBatch, c.Header.Type));
        Assert.Equal([4, 4, 1], chunks.Select(c => c.Header.Count));
        Assert.All(chunks, c => Assert.Equal(c.Header.Count, c.Header.PayloadLength));
        Assert.Equal([0, 1, 2], chunks.Select(c => c.Header.BatchId));
        Assert.Equal(bytes, chunks.SelectMany(c => c.Body));
        Assert.Equal(9, processor.ItemsSent);
    }

    [Fact]
    public async Task SendBytesAsync_AndSendRangeAsync_AppendToTheSameStream()
    {
        await using var processor = await StartSenderAsync(budget: 4);
        var bytes = Sequence(10);

        await processor.SendBytesAsync(bytes.AsMemory(0, 5), CancellationToken.None);
        await processor.SendRangeAsync([bytes[5..7], bytes[7..]], CancellationToken.None);
        await processor.CompleteSenderAsync(CancellationToken.None);

        var chunks = await WriteQueuedAsync(processor, 3);
        Assert.Equal(bytes, chunks.SelectMany(c => c.Body));
        Assert.Equal(10, processor.ItemsSent);
    }

    [Fact]
    public async Task SendAsync_BelowTheBudget_IsSentOnceTheFlushIntervalElapses()
    {
        await using var processor = await StartSenderAsync(budget: 1024);

        await processor.SendAsync(Sequence(3), CancellationToken.None);

        var chunk = Assert.Single(await WriteQueuedAsync(processor, 1));
        Assert.Equal(3, chunk.Header.Count);
        Assert.Equal(Sequence(3), chunk.Body);
    }

    [Fact]
    public async Task ResendUnackedBatches_InByteStreamMode_ResendsTheSameBytes_UnderTheSameTypeAndCount()
    {
        await using var processor = await StartSenderAsync(budget: 4);
        await processor.SendAsync(Sequence(10), CancellationToken.None);
        await processor.CompleteSenderAsync(CancellationToken.None);
        var sent = await WriteQueuedAsync(processor, 3);

        using var reopened = new MemoryStream();
        await processor.ResendUnackedBatchesAsync(reopened, _transferId, didPingPong: true, nextExpectedFromPong: 1, CancellationToken.None);

        var resent = await ReadWireAsync(reopened);
        Assert.Equal([1, 2], resent.Select(c => c.Header.BatchId));
        for (var i = 0; i < resent.Count; i++)
        {
            Assert.Equal(sent[i + 1].Header, resent[i].Header);
            Assert.Equal(sent[i + 1].Body, resent[i].Body);
        }
        Assert.All(resent, c => Assert.Equal(ChunkType.StreamBatch, c.Header.Type));
    }

    [Fact]
    public async Task AWrittenBatch_KeepsItsStreamThroughAResend_AndHasItDisposedOnceWithItsAck_ImplicitOrNot()
    {
        var streams = new StreamCounts(_context.StreamManager);
        await using var processor = await StartSenderAsync(budget: 4);
        await processor.SendAsync(Sequence(10), CancellationToken.None);
        await processor.CompleteSenderAsync(CancellationToken.None);
        await WriteAsTheWriteLoopAsync(processor, 3);
        Assert.Equal(0, streams.Disposed);

        // The receiver says it has batch 0: acknowledged without being resent, unlike 1 and 2.
        using var reopened = new MemoryStream();
        await processor.ResendUnackedBatchesAsync(reopened, _transferId, didPingPong: true, nextExpectedFromPong: 1, CancellationToken.None);
        Assert.Equal(1, streams.Disposed);
        Assert.Equal([1, 2], (await ReadWireAsync(reopened)).Select(c => c.Header.BatchId));

        await AckAsync(processor, 1);
        await AckAsync(processor, 2);

        Assert.Equal(3, streams.Disposed);
        Assert.Equal(0, streams.DisposedTwice);
    }

    [Fact]
    public async Task SendAsync_WhenTheWindowIsFull_ParksTheProducerBeforeRentingABatch_UntilAnAck()
    {
        var streams = new StreamCounts(_context.StreamManager);
        await using var processor = await StartSenderAsync(budget: 4, window: 2);
        await processor.SendAsync(Sequence(8), CancellationToken.None);
        await WaitUntil(() => processor.UnackedCount == 2);

        var parked = processor.SendAsync(Sequence(1), CancellationToken.None);

        Assert.NotSame(parked, await Task.WhenAny(parked, Task.Delay(300)));
        Assert.Equal(2, streams.Created);

        await AckAsync(processor, 0);

        await parked.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(3, streams.Created);
    }

    [Fact]
    public async Task Exit1_ARegisteredBatch_HasItsStreamDisposedOnce_AndItsSlotBackOnlyWithItsAck()
    {
        var logs = new Infrastructure.LogRecorder();
        _context.Logger = logs.CreateLogger("test");
        var streams = new StreamCounts(_context.StreamManager);
        await using var processor = await StartSenderAsync(budget: 4, window: 1);

        await processor.SendAsync(Sequence(4), CancellationToken.None);
        await WaitUntil(() => processor.UnackedCount == 1);
        await WriteAsTheWriteLoopAsync(processor, 1);

        var parked = processor.SendAsync(Sequence(1), CancellationToken.None);
        Assert.NotSame(parked, await Task.WhenAny(parked, Task.Delay(200)));
        await AckAsync(processor, 0);
        await parked.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Equal(1, streams.Disposed);
        Assert.Equal(0, streams.DisposedTwice);
        Assert.Empty(logs.MessagesFrom(LogLevel.Error));
    }

    [Fact]
    public async Task Exit3_AbandonSender_InByteStreamMode_ReleasesAProducerParkedOnTheWindow()
    {
        var streams = new StreamCounts(_context.StreamManager);
        await using var processor = await StartSenderAsync(budget: 4, window: 1);
        await processor.SendAsync(Sequence(4), CancellationToken.None);
        await WaitUntil(() => processor.UnackedCount == 1);
        var parked = processor.SendAsync(Sequence(1), CancellationToken.None);
        Assert.NotSame(parked, await Task.WhenAny(parked, Task.Delay(200)));

        processor.AbandonSender();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => parked.WaitAsync(TimeSpan.FromSeconds(5)));
        Assert.Equal(1, streams.Created);
    }

    [Fact]
    public async Task Exit4_DisposeAsync_InByteStreamMode_DisposesEveryStreamOnce()
    {
        var streams = new StreamCounts(_context.StreamManager);
        var processor = await StartSenderAsync(budget: 4, window: 3);
        await processor.SendAsync(Sequence(9), CancellationToken.None);
        await WaitUntil(() => processor.UnackedCount == 2);
        Assert.Equal(3, streams.Created);

        await processor.DisposeAsync();

        await WaitUntil(() => streams.Disposed == 3);
        await Task.Delay(50);
        Assert.Equal(3, streams.Disposed);
        Assert.Equal(0, streams.DisposedTwice);
    }

    [Fact]
    public async Task Exit6_ABatchThatFailsOnceRegistered_LeavesItsSlotToItsAck()
    {
        var logs = new Infrastructure.LogRecorder();
        _context.Logger = logs.CreateLogger("test");
        await using var processor = await StartSenderAsync(budget: 4, window: 1);
        var failed = new TaskCompletionSource<Exception>(TaskCreationOptions.RunContinuationsAsynchronously);
        processor.OnSerializationFailed += ex => failed.TrySetResult(ex);

        // The queue to the write loop is closed: the batch is registered, then cannot be queued.
        processor.CompleteSenderChannel();
        await processor.SendAsync(Sequence(4), CancellationToken.None);
        await failed.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(1, processor.UnackedCount);

        await AckAsync(processor, 0);

        // The ack gave the slot back once; a second release would have been logged as an over-release.
        Assert.False(logs.Has(LogLevel.Error, "over-released"));
        await processor.SendAsync(Sequence(1), CancellationToken.None).WaitAsync(TimeSpan.FromSeconds(5));
    }

    [Fact]
    public async Task ABatchThatFailsOnceRegistered_HasItsStreamDisposedOnce_WithItsAck()
    {
        var streams = new StreamCounts(_context.StreamManager);
        await using var processor = await StartSenderAsync(budget: 4, window: 1);
        var failed = new TaskCompletionSource<Exception>(TaskCreationOptions.RunContinuationsAsynchronously);
        processor.OnSerializationFailed += ex => failed.TrySetResult(ex);

        // The queue to the write loop is closed: the batch is registered, then cannot be queued.
        processor.CompleteSenderChannel();
        await processor.SendAsync(Sequence(4), CancellationToken.None);
        await failed.Task.WaitAsync(TimeSpan.FromSeconds(5));

        await AckAsync(processor, 0);

        Assert.Equal(1, streams.Disposed);
        Assert.Equal(0, streams.DisposedTwice);
    }

    public enum FailedBy { Fail, ReceivedAbort }

    private async Task EndTheTransferAsync(TestByteBatchProcessor processor, FailedBy how)
    {
        if (how == FailedBy.Fail)
        {
            processor.Fail("The transfer was ended.");
            return;
        }

        var abort = ChunkHeader.CreateControl(_transferId, (int)AbortReason.ReleasedBeforeCompletion, ChunkType.Abort);
        await processor.ProcessChunkAsync(new ChunkData(abort, new ArrayOwner([]), 0));
    }

    [Theory]
    [InlineData(FailedBy.Fail)]
    [InlineData(FailedBy.ReceivedAbort)]
    public async Task AFailedTransfer_ReleasesAConsumerOfReceiveChunksAsync_WithTheFailure_WithoutWaitingForDisposal(FailedBy how)
    {
        await using var processor = CreateProcessor();
        await processor.StartReceiverAsync();

        var consumer = Task.Run(async () =>
        {
            await foreach (var chunk in processor.ReceiveChunksAsync(CancellationToken.None)) chunk.Dispose();
        });

        await EndTheTransferAsync(processor, how);

        await Assert.ThrowsAsync<TransferFailedException>(() => consumer.WaitAsync(TimeSpan.FromSeconds(5)));
    }

    [Theory]
    [InlineData(FailedBy.Fail)]
    [InlineData(FailedBy.ReceivedAbort)]
    public async Task AFailedTransfer_ReleasesAConsumerOfReceiveAsync_WithTheFailure_WithoutWaitingForDisposal(FailedBy how)
    {
        await using var processor = CreateProcessor();
        await processor.StartReceiverAsync();

        var consumer = Task.Run(async () =>
        {
            await foreach (var _ in processor.ReceiveAsync(CancellationToken.None)) { }
        });

        await EndTheTransferAsync(processor, how);

        await Assert.ThrowsAsync<TransferFailedException>(() => consumer.WaitAsync(TimeSpan.FromSeconds(5)));
    }

    [Fact]
    public async Task AFailedTransfer_StillHandsTheConsumerWhatWasAdmittedBeforeTheFailure()
    {
        await using var processor = CreateProcessor();
        await processor.StartReceiverAsync();
        await processor.ProcessChunkAsync(CreateChunk(0, Bytes(7, 5)));

        processor.Fail("The transfer was ended.");

        var received = new List<byte[]>();
        var failure = await Assert.ThrowsAsync<TransferFailedException>(async () =>
        {
            await foreach (var chunk in processor.ReceiveChunksAsync(CancellationToken.None))
            {
                using (chunk) received.Add(chunk.Memory.ToArray());
            }
        }).WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Equal([Bytes(7, 5)], received);
        Assert.Contains("The transfer was ended.", failure.Message);
    }

    private sealed class StreamCounts
    {
        private int _created;
        private int _disposed;
        private int _disposedTwice;

        public StreamCounts(RecyclableMemoryStreamManager manager)
        {
            manager.StreamCreated += (_, _) => Interlocked.Increment(ref _created);
            manager.StreamDisposed += (_, _) => Interlocked.Increment(ref _disposed);
            manager.StreamDoubleDisposed += (_, _) => Interlocked.Increment(ref _disposedTwice);
        }

        public int Created => Volatile.Read(ref _created);
        public int Disposed => Volatile.Read(ref _disposed);
        public int DisposedTwice => Volatile.Read(ref _disposedTwice);
    }

    private sealed class ArrayOwner(byte[] data) : IMemoryOwner<byte>
    {
        public Memory<byte> Memory => data;
        public void Dispose() { }
    }

    private class ByteStreamSessionContext : ISessionContext
    {
        public ILogger Logger { get; set; } = new StubLogger();
        public HttpClient HttpClient => null!;
        public RecyclableMemoryStreamManager StreamManager { get; } = new();
        public ClientOptions Options { get; } = new() { BatchFlushIntervalMs = 50 };

        public int BatchBytes { get; set; } = 1024;

        public TestByteBatchProcessor NewProcessor(string transferId) =>
            new(this, transferId,
                new ByteBatchSource(BatchBytes, Options.BatchFlushIntervalMs, StreamManager),
                new ByteReceivePort(Options.ReceiveBufferCapacity));
        public void RemoveSession(string transferId) { }
        public Task<string?> GetAccessTokenAsync(bool forceRefresh = false) => Task.FromResult<string?>(null);
        public Task ReportTransferAsync(string transferId, string? affinity, long items, long acks, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task<bool> ProbeAsync(string transferId, string? affinity, CancellationToken cancellationToken) => Task.FromResult(false);
    }
}
