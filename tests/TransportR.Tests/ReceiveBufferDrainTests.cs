using TransportR.Client.Batching;
using TransportR.Client.Configuration;

namespace TransportR.Tests;

public class ReceiveBufferDrainTests
{
    private static async Task WriteAsync(ReceiveBuffer<int> buffer, params int[] items)
        => await buffer.WriteChunkAsync(items.Length * 4L, () => Task.FromResult<List<int>?>([.. items]), CancellationToken.None);

    [Fact]
    public async Task Drain_GivesEachUnconsumedEntryOnce_AndGivesTheCapacityBack()
    {
        var buffer = new ReceiveBuffer<int>(BufferCapacity.ItemsAndBytes(100, 1000));
        await WriteAsync(buffer, 1, 2);
        await WriteAsync(buffer, 3);
        buffer.Complete();

        var drained = new List<int>();
        buffer.Drain(drained.Add);
        buffer.Drain(drained.Add);

        Assert.Equal([1, 2, 3], drained);
        Assert.Equal(0, buffer.BufferedBytes);
        Assert.Empty(await ToListAsync(buffer));
    }

    [Fact]
    public async Task Complete_DoesNotDiscard_ThePendingEntriesAreStillTheConsumers()
    {
        var buffer = new ReceiveBuffer<int>(capacity: null);
        await WriteAsync(buffer, 1, 2);

        buffer.Complete();

        Assert.Equal([1, 2], await ToListAsync(buffer));
    }

    [Fact]
    public async Task Drain_NeverGivesAnEntryTheConsumerAlreadyHolds()
    {
        var buffer = new ReceiveBuffer<int>(capacity: null);
        await WriteAsync(buffer, 1, 2, 3);
        buffer.Complete();
        await using var consumer = buffer.ReadAllAsync(CancellationToken.None).GetAsyncEnumerator();
        Assert.True(await consumer.MoveNextAsync());
        Assert.Equal(1, consumer.Current);

        var drained = new List<int>();
        buffer.Drain(drained.Add);

        Assert.Equal([2, 3], drained);
        Assert.False(await consumer.MoveNextAsync());
    }

    [Fact]
    public async Task Drain_ReleasesAWriterParkedOnTheGateOnlyAfterCompleteHasWoken_AndLosesNothing()
    {
        // Arrange: a budget of one chunk, filled, and a writer parked behind it.
        var buffer = new ReceiveBuffer<int>(BufferCapacity.ItemsAndBytes(100, 4));
        await WriteAsync(buffer, 1);
        var parked = WriteAsync(buffer, 2);
        Assert.NotSame(parked, await Task.WhenAny(parked, Task.Delay(200)));

        // Act: the session is released.
        buffer.Complete();
        await Assert.ThrowsAnyAsync<Exception>(() => parked);
        var drained = new List<int>();
        buffer.Drain(drained.Add);

        // Assert: the entry that was in is discarded, the one that never got in is not invented.
        Assert.Equal([1], drained);
    }

    [Fact]
    public async Task Drain_WhileAConsumerReads_AccountsForEveryEntryExactlyOnce()
    {
        const int entries = 2000;
        for (var round = 0; round < 50; round++)
        {
            var buffer = new ReceiveBuffer<int>(BufferCapacity.ItemsAndBytes(1_000_000, 1_000_000));
            for (var i = 0; i < entries; i++) await WriteAsync(buffer, i);
            buffer.Complete();

            var consumed = new List<int>();
            var consumer = Task.Run(async () =>
            {
                await foreach (var item in buffer.ReadAllAsync(CancellationToken.None)) consumed.Add(item);
            });
            var drained = new List<int>();
            buffer.Drain(drained.Add);
            await consumer;

            Assert.Equal(entries, consumed.Count + drained.Count);
            Assert.Empty(consumed.Intersect(drained));
            Assert.Equal(0, buffer.BufferedBytes);
        }
    }

    private static async Task<List<int>> ToListAsync(ReceiveBuffer<int> buffer)
    {
        var list = new List<int>();
        await foreach (var item in buffer.ReadAllAsync(CancellationToken.None)) list.Add(item);
        return list;
    }
}
