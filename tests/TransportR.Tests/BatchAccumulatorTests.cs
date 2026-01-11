using TransportR.Client.SignalR.Batching;
using TransportR.Protocol;
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
}


