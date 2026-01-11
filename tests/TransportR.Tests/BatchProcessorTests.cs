using System.Buffers;
using System.IO.Pipelines;
using System.Threading.Channels;
using System.Text;
using TransportR.Client.SignalR.Internal;
using TransportR.Client.SignalR.Sessions;
using TransportR.Client.SignalR.Configuration;
using TransportR.Interfaces;
using TransportR.Protocol;
using Microsoft.Extensions.Logging;
using Microsoft.IO;
using Microsoft.AspNetCore.SignalR.Client;
using Xunit;

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
        return new BatchProcessor<string>(_stubContext, _transferId.ToString(), batchSize, "http://test");
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
        await processor.SendCompleteSignalAsync(CancellationToken.None);
        
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

    // ========== STUBS ==========

    private class StubSessionContext : ISessionContext
    {
        public Guid ClientId { get; } = Guid.NewGuid();
        public ILogger Logger { get; } = new StubLogger();
        public IBatchSerializer Serializer { get; } = new StubBatchSerializer();
        public HttpClient HttpClient => null!; // Not used in BatchProcessor logic directly (only via HttpStreamManager which is mocked out or not used)
        // Wait, BatchProcessor doesn't use HttpClient directly.
        
        public HubConnection HubConnection => null!; // Not used in logic tested
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

    private class StubBatchSerializer : IBatchSerializer
    {
        public Task SerializeBatchAsync<T>(Stream destination, List<T> batch)
        {
            // Simple string serialization: Count + Items
            // 4 bytes count, then just placeholder bytes for items
            using var writer = new BinaryWriter(destination, Encoding.UTF8, leaveOpen: true);
            writer.Write(batch.Count);
            // Just write dummy bytes for content
            writer.Write(new byte[10 * batch.Count]); 
            return Task.CompletedTask;
        }

        public Task<List<T>?> DeserializeBatchAsync<T>(Stream source)
        {
            using var reader = new BinaryReader(source, Encoding.UTF8, leaveOpen: true);
            var count = reader.ReadInt32();
            var bytes = reader.ReadBytes(10 * count);
            var list = new List<T>(); 
            // Return empty items just to match count
            for(int i=0; i<count; i++) list.Add(default!);
            return Task.FromResult<List<T>?>(list);
        }
    }
}
