using TransportR.Client.Batching;
using Xunit;

namespace POC_DataProxy.Tests;

/// <summary>
/// Unit tests for <see cref="BatchAccumulator{T}"/>.
/// </summary>
public class BatchAccumulatorTests
{
    [Fact]
    public void Constructor_WithInvalidBatchSize_ThrowsArgumentOutOfRangeException()
    {
        // Arrange & Act & Assert
        Assert.Throws<ArgumentOutOfRangeException>(() => new BatchAccumulator<int>(0, 1000));
        Assert.Throws<ArgumentOutOfRangeException>(() => new BatchAccumulator<int>(-1, 1000));
    }

    [Fact]
    public void Constructor_WithInvalidTimeoutMs_ThrowsArgumentOutOfRangeException()
    {
        // Arrange & Act & Assert
        Assert.Throws<ArgumentOutOfRangeException>(() => new BatchAccumulator<int>(100, 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => new BatchAccumulator<int>(100, -1));
    }

    [Fact]
    public void Constructor_WithValidParameters_CreatesInstance()
    {
        // Arrange & Act
        using var accumulator = new BatchAccumulator<int>(100, 1000);
        
        // Assert
        Assert.NotNull(accumulator);
        Assert.NotNull(accumulator.BatchReader);
    }

    [Fact]
    public void Dispose_CalledTwice_IsNotAnError()
    {
        var accumulator = new BatchAccumulator<int>(100, 1000);

        accumulator.Dispose();
        accumulator.Dispose();
    }

    [Fact]
    public async Task AddAsync_WhenBatchSizeReached_PublishesToChannel()
    {
        // Arrange
        const int batchSize = 3;
        using var accumulator = new BatchAccumulator<int>(batchSize, 60000);
        
        // Act
        await accumulator.AddAsync(1);
        await accumulator.AddAsync(2);
        await accumulator.AddAsync(3); // Should trigger batch
        await accumulator.CompleteAsync();

        // Assert - Read from channel
        var batches = new List<(List<int> Items, int BatchId)>();
        await foreach (var batch in accumulator.BatchReader.ReadAllAsync())
        {
            batches.Add(batch);
        }
        
        Assert.Single(batches);
        Assert.Equal(3, batches[0].Items.Count);
        Assert.Equal(0, batches[0].BatchId);
        Assert.Equal(new[] { 1, 2, 3 }, batches[0].Items);
    }

    [Fact]
    public async Task AddAsync_MultipleBatches_IncrementsBatchId()
    {
        // Arrange
        const int batchSize = 2;
        using var accumulator = new BatchAccumulator<int>(batchSize, 60000);
        
        // Act
        await accumulator.AddAsync(1);
        await accumulator.AddAsync(2); // Batch 0
        await accumulator.AddAsync(3);
        await accumulator.AddAsync(4); // Batch 1
        await accumulator.CompleteAsync();

        // Assert
        var batchIds = new List<int>();
        await foreach (var (_, batchId) in accumulator.BatchReader.ReadAllAsync())
        {
            batchIds.Add(batchId);
        }
        
        Assert.Equal(2, batchIds.Count);
        Assert.Equal(0, batchIds[0]);
        Assert.Equal(1, batchIds[1]);
    }

    [Fact]
    public async Task CompleteAsync_WithPendingItems_FlushesRemainingItems()
    {
        // Arrange
        const int batchSize = 10; // Large batch size
        using var accumulator = new BatchAccumulator<int>(batchSize, 60000);
        
        await accumulator.AddAsync(1);
        await accumulator.AddAsync(2);
        await accumulator.AddAsync(3);
        
        // No batch yet since batch size not reached
        Assert.False(accumulator.BatchReader.TryRead(out _));

        // Act
        await accumulator.CompleteAsync();

        // Assert
        var flushedItems = new List<int>();
        await foreach (var (items, _) in accumulator.BatchReader.ReadAllAsync())
        {
            flushedItems.AddRange(items);
        }
        
        Assert.Equal(3, flushedItems.Count);
        Assert.Equal(new[] { 1, 2, 3 }, flushedItems);
    }

    [Fact]
    public async Task CompleteAsync_WithNoItems_CompletesChannelWithNoBatches()
    {
        // Arrange
        using var accumulator = new BatchAccumulator<int>(10, 60000);

        // Act
        await accumulator.CompleteAsync();

        // Assert
        var batches = new List<(List<int>, int)>();
        await foreach (var batch in accumulator.BatchReader.ReadAllAsync())
        {
            batches.Add(batch);
        }
        
        Assert.Empty(batches);
    }

    [Fact]
    public async Task AddRangeAsync_AddsMultipleItems_FlushesWhenBatchSizeReached()
    {
        // Arrange
        const int batchSize = 3;
        using var accumulator = new BatchAccumulator<int>(batchSize, 60000);

        // Act
        await accumulator.AddRangeAsync(new[] { 1, 2, 3, 4, 5 });
        await accumulator.CompleteAsync();

        // Assert - Should have one batch of 3, plus leftover batch of 2
        var flushedBatches = new List<List<int>>();
        await foreach (var (items, _) in accumulator.BatchReader.ReadAllAsync())
        {
            flushedBatches.Add(items);
        }
        
        Assert.Equal(2, flushedBatches.Count);
        Assert.Equal(3, flushedBatches[0].Count);
        Assert.Equal(2, flushedBatches[1].Count);
    }

    [Fact]
    public void Dispose_DisposesResources()
    {
        // Arrange
        var accumulator = new BatchAccumulator<int>(100, 1000);
        
        // Act
        accumulator.Dispose();
        
        // Assert - No exception thrown, disposed successfully
        // Channel should be completed after dispose
        Assert.True(accumulator.BatchReader.Completion.IsCompleted);
    }

    // ---- In-flight window (ClientOptions.MaxInFlightBatches) ----

    [Fact]
    public async Task AddAsync_WithoutWindow_NeverBlocksTheProducer()
    {
        // Arrange: no window - the historical, unbounded behaviour.
        using var accumulator = new BatchAccumulator<int>(batchSize: 1, flushIntervalMs: 60000);

        // Act: publish far more batches than anything will consume.
        var produce = Task.Run(async () =>
        {
            for (var i = 0; i < 50; i++)
                await accumulator.AddAsync(i);
        });

        // Assert: completes without anyone draining the channel.
        var finished = await Task.WhenAny(produce, Task.Delay(TimeSpan.FromSeconds(5)));
        Assert.Same(produce, finished);
        await produce;
    }

    [Fact]
    public async Task AddAsync_WhenWindowIsFull_BlocksTheProducer()
    {
        // Arrange: a window of two batches, one item per batch.
        using var slots = new SemaphoreSlim(2, 2);
        using var accumulator = new BatchAccumulator<int>(batchSize: 1, flushIntervalMs: 60000, inFlightSlots: slots);

        // Act: a third batch has no slot to take.
        await accumulator.AddAsync(1);
        await accumulator.AddAsync(2);
        var blocked = accumulator.AddAsync(3).AsTask();

        // Assert: still parked - draining the channel is NOT what reopens the window.
        Assert.True(accumulator.BatchReader.TryRead(out _));
        Assert.True(accumulator.BatchReader.TryRead(out _));
        var finished = await Task.WhenAny(blocked, Task.Delay(300));
        Assert.NotSame(blocked, finished);

        // Act: an acknowledgement gives a slot back.
        slots.Release();

        // Assert: the producer resumes.
        await blocked.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.True(accumulator.BatchReader.TryRead(out _));
    }

    [Fact]
    public async Task AddAsync_WhenDisposedWhileParked_ReleasesTheProducer()
    {
        // Arrange: a window of one, already exhausted.
        using var slots = new SemaphoreSlim(1, 1);
        var accumulator = new BatchAccumulator<int>(batchSize: 1, flushIntervalMs: 60000, inFlightSlots: slots);
        await accumulator.AddAsync(1);

        var blocked = accumulator.AddAsync(2).AsTask();
        Assert.NotSame(blocked, await Task.WhenAny(blocked, Task.Delay(200)));

        // Act: tear the session down while the producer waits on a window that will never reopen.
        accumulator.Dispose();

        // Assert: it is cancelled, not hung.
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => blocked.WaitAsync(TimeSpan.FromSeconds(5)));
    }

    [Fact]
    public async Task AddAsync_WhenCancelledWhileParked_ObservesTheCallerToken()
    {
        // Arrange
        using var slots = new SemaphoreSlim(1, 1);
        using var accumulator = new BatchAccumulator<int>(batchSize: 1, flushIntervalMs: 60000, inFlightSlots: slots);
        using var cts = new CancellationTokenSource();
        await accumulator.AddAsync(1, cts.Token);

        var blocked = accumulator.AddAsync(2, cts.Token).AsTask();
        Assert.NotSame(blocked, await Task.WhenAny(blocked, Task.Delay(200)));

        // Act
        cts.Cancel();

        // Assert
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => blocked.WaitAsync(TimeSpan.FromSeconds(5)));
    }
}
