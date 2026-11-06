using System.Buffers;
using Microsoft.Extensions.Logging;
using Microsoft.IO;
using TransportR.Client.Batching;
using TransportR.Client.Configuration;
using TransportR.Client.Sessions;
using TransportR.Interfaces;
using TransportR.Protocol;
using TransportR.Tests.TestHelpers;

namespace TransportR.Tests;

/// <summary>
/// The receiving side of the byte-stream mode, through <see cref="BatchProcessor{T}"/>. How the
/// session reacts to the refusals is <c>DataSession</c>'s, covered end to end with the public surface.
/// </summary>
public class BatchProcessorByteStreamTests
{
    private readonly ByteStreamSessionContext _context = new();
    private readonly Guid _transferId = Guid.NewGuid();

    private BatchProcessor<byte[]> CreateProcessor()
    {
        _context.Options.ByteStreamBatchBytes = 1024;
        return new BatchProcessor<byte[]>(_context, _transferId.ToString(), batchSize: 1024);
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
    public async Task ProcessChunk_CopiesThePayload_SoTheChunkTheReadLoopDisposesIsNotTheOneHandedOn()
    {
        await using var processor = CreateProcessor();
        await processor.StartReceiverAsync();
        var payload = Bytes(3, 6);

        await processor.ProcessChunkAsync(CreateChunk(0, payload));
        Array.Clear(payload);
        processor.CompleteReceiver();

        await foreach (var batch in processor.ReceiveAsync(CancellationToken.None))
            Assert.Equal(Bytes(3, 6), batch);
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

    [Fact]
    public void ReceiveChunksAsync_OutsideTheByteStreamMode_Throws()
    {
        var context = new ItemSessionContext();
        var processor = new BatchProcessor<string>(context, _transferId.ToString(), 10);

        Assert.Throws<InvalidOperationException>(() => processor.ReceiveChunksAsync(CancellationToken.None));
    }

    [Fact]
    public void ItemMode_WithoutASerializer_RefusesToBeBuilt()
    {
        // The options carry no byte-stream size, so this is the item mode with nothing to encode it.
        var context = new ByteStreamSessionContext();

        Assert.Throws<InvalidOperationException>(() => new BatchProcessor<byte[]>(context, _transferId.ToString(), 10));
    }

    [Fact]
    public async Task StartSenderAsync_InByteStreamMode_IsRefused()
    {
        await using var processor = CreateProcessor();

        await Assert.ThrowsAsync<InvalidOperationException>(() => processor.StartSenderAsync(CancellationToken.None));
    }

    private sealed class ArrayOwner(byte[] data) : IMemoryOwner<byte>
    {
        public Memory<byte> Memory => data;
        public void Dispose() { }
    }

    private class ByteStreamSessionContext : ISessionContext<byte[]>
    {
        public ILogger Logger { get; } = new StubLogger();
        public IBatchSerializer<byte[]>? Serializer => null;
        public HttpClient HttpClient => null!;
        public int BatchFlushIntervalMs => 50;
        public RecyclableMemoryStreamManager StreamManager { get; } = new();
        public ClientOptions Options { get; } = new();
        public void RemoveSession(string transferId) { }
        public Task<string?> GetAccessTokenAsync(bool forceRefresh = false) => Task.FromResult<string?>(null);
        public Task ReportTransferAsync(string transferId, string? affinity, long items, long acks, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task<bool> ProbeAsync(string transferId, string? affinity, CancellationToken cancellationToken) => Task.FromResult(false);
    }

    private sealed class ItemSessionContext : StubSessionContext;
}
