using System.Buffers;
using TransportR.Client.SignalR.Batching;
using TransportR.Configuration;
using TransportR.Interfaces;
using TransportR.Protocol;
using Xunit;

namespace POC_DataProxy.Tests;

/// <summary>
/// Unit tests for <see cref="ReceiverState{T}"/>.
/// </summary>
public class ReceiverStateTests
{
    private readonly ReceiveBuffer<int> _buffer;
    private readonly BatchProtocol<int> _protocol;
    private readonly ReceiverState<int> _receiverState;

    public ReceiverStateTests()
    {
        // Wide enough that nothing here parks on capacity - these tests are about ordering.
        _buffer = new ReceiveBuffer<int>(BufferCapacity.ItemsAndBytes(1000, 1024 * 1024));
        _protocol = new BatchProtocol<int>(new MockBatchSerializer());
        _receiverState = new ReceiverState<int>(_buffer, _protocol);
    }

    [Fact]
    public void Constructor_InitializesWithBatchIdZero()
    {
        // Assert
        Assert.Equal(0, _receiverState.NextExpectedBatchId);
    }

    [Fact]
    public async Task ProcessBatchAsync_InOrder_ReturnsAckAndProcesses()
    {
        // Arrange
        var payload = CreateMockPayload(new[] { 1, 2, 3 });
        var owner = new ArrayMemoryOwner(payload);

        // Act
        var result = await _receiverState.ProcessBatchAsync(0, owner, payload);

        // Assert
        Assert.True(result.IsProcessed);
        Assert.Equal(0, result.BatchIdToAck);
        Assert.Equal(1, _receiverState.NextExpectedBatchId);
    }

    [Fact]
    public async Task ProcessBatchAsync_OutOfOrder_BuffersAndReturnsAck()
    {
        // Arrange - Send batch 1 before batch 0
        var payload = CreateMockPayload(new[] { 4, 5, 6 });
        var owner = new ArrayMemoryOwner(payload);

        // Act
        var result = await _receiverState.ProcessBatchAsync(1, owner, payload);

        // Assert
        Assert.True(result.IsProcessed);
        Assert.Equal(1, result.BatchIdToAck);      // ACK the received batch (TCP will deliver missing later)
        Assert.Equal(0, _receiverState.NextExpectedBatchId); // Still waiting for 0
    }

    [Fact]
    public async Task ProcessBatchAsync_DuplicateBatch_DisposesOwnerAndReturnsAck()
    {
        // Arrange - First, receive batch 0
        var payload1 = CreateMockPayload(new[] { 1, 2, 3 });
        var owner1 = new ArrayMemoryOwner(payload1);
        await _receiverState.ProcessBatchAsync(0, owner1, payload1);

        // Send duplicate batch 0 again
        var payload2 = CreateMockPayload(new[] { 1, 2, 3 });
        var owner2 = new ArrayMemoryOwner(payload2);

        // Act
        var result = await _receiverState.ProcessBatchAsync(0, owner2, payload2);

        // Assert
        Assert.True(result.IsProcessed);
        Assert.Equal(0, result.BatchIdToAck); // ACK the duplicate (already processed)
        Assert.True(owner2.IsDisposed); // Owner should be disposed since batch was duplicate
    }

    [Fact]
    public async Task ProcessBatchAsync_BufferedBatchesProcessedWhenGapFilled()
    {
        // Arrange - Send batches out of order: 2, 1, 0
        var payload2 = CreateMockPayload(new[] { 7, 8, 9 });
        var owner2 = new ArrayMemoryOwner(payload2);
        await _receiverState.ProcessBatchAsync(2, owner2, payload2);
        Assert.Equal(0, _receiverState.NextExpectedBatchId);

        var payload1 = CreateMockPayload(new[] { 4, 5, 6 });
        var owner1 = new ArrayMemoryOwner(payload1);
        await _receiverState.ProcessBatchAsync(1, owner1, payload1);
        Assert.Equal(0, _receiverState.NextExpectedBatchId);

        // Now send batch 0 - should trigger processing of 0, 1, 2
        var payload0 = CreateMockPayload(new[] { 1, 2, 3 });
        var owner0 = new ArrayMemoryOwner(payload0);

        // Act
        var result = await _receiverState.ProcessBatchAsync(0, owner0, payload0);

        // Assert
        Assert.True(result.IsProcessed);
        Assert.Equal(0, result.BatchIdToAck);
        Assert.Equal(3, _receiverState.NextExpectedBatchId); // All 3 batches processed
    }

    [Fact]
    public async Task DiscardReordered_ReturnsTheMemoryOfChunksHeldOutOfOrder()
    {
        // Arrange: batch 1 arrives first and is held.
        var payload = CreateMockPayload(new[] { 4, 5, 6 });
        var owner = new ArrayMemoryOwner(payload);
        await _receiverState.ProcessBatchAsync(1, owner, payload);
        Assert.Equal(1, _receiverState.ReorderedChunks);
        Assert.False(owner.IsDisposed);

        // Act: a reconnection replaces this state.
        _receiverState.DiscardReordered();

        // Assert: pooled buffers go back, rather than away with the instance.
        Assert.Equal(0, _receiverState.ReorderedChunks);
        Assert.True(owner.IsDisposed);
    }

    [Fact]
    public async Task Complete_EndsTheConsumersEnumeration()
    {
        // Arrange
        var payload = CreateMockPayload(new[] { 1, 2, 3 });
        await _receiverState.ProcessBatchAsync(0, new ArrayMemoryOwner(payload), payload);

        // Act
        _receiverState.Complete();

        // Assert: what was already buffered is still delivered, then the enumeration ends.
        var received = new List<int>();
        await foreach (var item in _buffer.ReadAllAsync(CancellationToken.None))
            received.Add(item);

        Assert.Equal(new[] { 1, 2, 3 }, received);
    }

    [Fact]
    public async Task ProcessBatchAsync_OutOfOrder_IsCountedWhileHeld()
    {
        // Arrange
        Assert.Equal(0, _receiverState.ReorderedChunks);
        var payload1 = CreateMockPayload(new[] { 4, 5, 6 });

        // Act: batch 1 before batch 0.
        await _receiverState.ProcessBatchAsync(1, new ArrayMemoryOwner(payload1), payload1);

        // Assert: held, and visible as held - these bytes sit outside the receive buffer's budget.
        Assert.Equal(1, _receiverState.ReorderedChunks);

        // Act: the gap closes.
        var payload0 = CreateMockPayload(new[] { 1, 2, 3 });
        await _receiverState.ProcessBatchAsync(0, new ArrayMemoryOwner(payload0), payload0);

        // Assert
        Assert.Equal(0, _receiverState.ReorderedChunks);
    }

    private static byte[] CreateMockPayload(int[] items)
    {
        // Create a simple mock payload that our MockBatchSerializer can deserialize
        using var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms);
        writer.Write(items.Length);
        foreach (var item in items)
        {
            writer.Write(item);
        }
        return ms.ToArray();
    }

    /// <summary>
    /// Simple memory owner wrapper for testing.
    /// </summary>
    private class ArrayMemoryOwner : IMemoryOwner<byte>
    {
        private readonly byte[] _data;
        public bool IsDisposed { get; private set; }

        public ArrayMemoryOwner(byte[] data) => _data = data;
        
        public Memory<byte> Memory => _data;

        public void Dispose()
        {
            IsDisposed = true;
        }
    }

    /// <summary>
    /// Mock serializer for testing ReceiverState.
    /// </summary>
    private class MockBatchSerializer : IBatchSerializer<int>
    {
        public Task SerializeBatchAsync(Stream destination, List<int> batch)
        {
            using var writer = new BinaryWriter(destination, System.Text.Encoding.UTF8, leaveOpen: true);
            writer.Write(batch.Count);
            foreach (var item in batch)
            {
                writer.Write(item);
            }
            return Task.CompletedTask;
        }

        public Task<List<int>?> DeserializeBatchAsync(Stream source)
        {
            using var reader = new BinaryReader(source, System.Text.Encoding.UTF8, leaveOpen: true);
            var count = reader.ReadInt32();
            var result = new List<int>(count);
            for (int i = 0; i < count; i++)
            {
                result.Add(reader.ReadInt32());
            }
            return Task.FromResult<List<int>?>(result);
        }
    }
}
