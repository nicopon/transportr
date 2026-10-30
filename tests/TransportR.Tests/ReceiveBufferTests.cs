using System.Threading.Channels;
using TransportR.Client.Batching;
using TransportR.Client.Configuration;

namespace TransportR.Tests;

/// <summary>
/// Unit tests for <see cref="ReceiveBuffer{T}"/> — the two dimensions of
/// <see cref="BufferCapacity"/> and the accounting that gives capacity back.
/// </summary>
public class ReceiveBufferTests
{
    private static Task<int> WriteChunk(ReceiveBuffer<int> buffer, long wireBytes, params int[] items)
        => buffer.WriteChunkAsync(wireBytes, () => Task.FromResult<List<int>?>([.. items]), CancellationToken.None);

    private static async Task<bool> IsParked(Task task)
        => task != await Task.WhenAny(task, Task.Delay(300));

    // ---- Construction ----

    [Fact]
    public void Constructor_WithAnExplicitlyEmptyCapacity_IsRefused()
    {
        // default(BufferCapacity) is a capacity built wrong, not a request for no bound. Null is
        // how that request is made, and conflating the two would swallow the mistake.
        Assert.Throws<ArgumentException>(() => new ReceiveBuffer<int>(default(BufferCapacity)));
    }

    [Fact]
    public async Task Constructor_WithNullCapacity_BoundsNothing()
    {
        // Arrange
        var buffer = new ReceiveBuffer<int>(null);

        // Act: far more than any bound this type can express, acknowledged by nobody.
        var fill = Task.Run(async () =>
        {
            for (var i = 0; i < 2000; i++) await WriteChunk(buffer, wireBytes: 1_000_000, i);
        });

        // Assert
        Assert.Same(fill, await Task.WhenAny(fill, Task.Delay(TimeSpan.FromSeconds(5))));
        await fill;
        Assert.Equal(0, buffer.BufferedBytes);
    }

    [Theory]
    [InlineData(5, null, 5, null)]
    [InlineData(null, 5L, null, 5L)]
    [InlineData(5, 7L, 5, 7L)]
    public void Of_BuildsFromTheShapeConfigurationBindingProduces(
        int? items, long? bytes, int? expectedItems, long? expectedBytes)
    {
        var capacity = BufferCapacity.Of(items, bytes);

        Assert.Equal(expectedItems, capacity.MaxItems);
        Assert.Equal(expectedBytes, capacity.MaxBytes);
    }

    [Fact]
    public void Of_WithNeitherDimension_IsRefused()
    {
        Assert.Throws<ArgumentException>(() => BufferCapacity.Of(null, null));
    }

    // ---- The item dimension ----

    [Fact]
    public async Task Items_ParkTheWriterOnceTheCountIsReached()
    {
        // Arrange
        var buffer = new ReceiveBuffer<int>(BufferCapacity.Items(3));
        await WriteChunk(buffer, wireBytes: 20, 1, 2);

        // Act: two more items into one remaining slot.
        var write = WriteChunk(buffer, wireBytes: 20, 3, 4);

        // Assert: the whole chunk waits - the slot that is free is not offered to half of it.
        Assert.True(await IsParked(write));

        // Act: the consumer takes one, and comes back for the next.
        var enumerator = buffer.ReadAllAsync(CancellationToken.None).GetAsyncEnumerator();
        await enumerator.MoveNextAsync();
        await enumerator.MoveNextAsync();

        // Assert: the writer resumes.
        Assert.Equal(2, await write.WaitAsync(TimeSpan.FromSeconds(5)));
        await enumerator.DisposeAsync();
    }

    [Fact]
    public async Task Items_AChunkLongerThanTheWholeCountIsStillAdmittedIntoAnEmptyBuffer()
    {
        // Arrange: a batch of five hundred against a bound of two.
        var buffer = new ReceiveBuffer<int>(BufferCapacity.Items(2));

        // Act
        var write = WriteChunk(buffer, wireBytes: 30, 1, 2, 3, 4, 5);

        // Assert: same rule as the byte dimension, and for the same reason - nothing can ever
        // free room for a chunk that does not fit an empty buffer, and a chunk cannot be split.
        Assert.Equal(5, await write.WaitAsync(TimeSpan.FromSeconds(5)));
    }

    // ---- The byte dimension ----

    [Fact]
    public async Task Bytes_ParkTheWriterOnceTheBudgetIsReached()
    {
        // Arrange
        var buffer = new ReceiveBuffer<int>(BufferCapacity.Bytes(100));
        await WriteChunk(buffer, wireBytes: 100, 1);
        Assert.Equal(100, buffer.BufferedBytes);

        // Act
        var write = WriteChunk(buffer, wireBytes: 100, 2);

        // Assert: the count would not have stopped this - two items is nothing.
        Assert.True(await IsParked(write));

        // Act: the consumer takes the first item, and comes back for the next.
        var enumerator = buffer.ReadAllAsync(CancellationToken.None).GetAsyncEnumerator();
        await enumerator.MoveNextAsync();
        await enumerator.MoveNextAsync();

        // Assert
        await write.WaitAsync(TimeSpan.FromSeconds(5));
        await enumerator.DisposeAsync();
    }

    [Fact]
    public async Task Bytes_AChunkLargerThanTheWholeBudget_IsStillAdmittedIntoAnEmptyBuffer()
    {
        // Arrange: a 64 MB budget against a 200 MB record batch.
        var buffer = new ReceiveBuffer<int>(BufferCapacity.Bytes(64));

        // Act
        var write = WriteChunk(buffer, wireBytes: 200, 1);

        // Assert: throttling is the point, stalling is not - nothing can ever free room for this.
        Assert.Equal(1, await write.WaitAsync(TimeSpan.FromSeconds(5)));
        Assert.Equal(200, buffer.BufferedBytes);
    }

    [Fact]
    public async Task Bytes_AreSplitAcrossAChunkWithoutLosingTheRemainder()
    {
        // Arrange: 10 bytes over 3 items does not divide.
        var buffer = new ReceiveBuffer<int>(BufferCapacity.Bytes(1000));

        // Act
        await WriteChunk(buffer, wireBytes: 10, 1, 2, 3);

        // Assert: the shares add up to the chunk, or the budget leaks a byte per chunk forever.
        Assert.Equal(10, buffer.BufferedBytes);

        await foreach (var _ in buffer.ReadAllAsync(CancellationToken.None))
        {
            buffer.Complete();
        }

        Assert.Equal(0, buffer.BufferedBytes);
    }

    [Fact]
    public async Task Bytes_AreHeldForTheItemTheConsumerIsWorkingOn()
    {
        // Arrange
        var buffer = new ReceiveBuffer<int>(BufferCapacity.Bytes(1000));
        await WriteChunk(buffer, wireBytes: 10, 1, 2);

        // Act
        var enumerator = buffer.ReadAllAsync(CancellationToken.None).GetAsyncEnumerator();
        await enumerator.MoveNextAsync();

        // Assert: the item in the consumer's hands is still in memory, so it still counts.
        Assert.Equal(10, buffer.BufferedBytes);

        // Act: the consumer comes back for the next one.
        await enumerator.MoveNextAsync();

        // Assert
        Assert.Equal(5, buffer.BufferedBytes);

        await enumerator.DisposeAsync();
        Assert.Equal(0, buffer.BufferedBytes);
    }

    [Fact]
    public async Task Bytes_AreGivenBackWhenAChunkDecodesToNothing()
    {
        // Arrange
        var buffer = new ReceiveBuffer<int>(BufferCapacity.Bytes(100));

        // Act
        var written = await buffer.WriteChunkAsync(100, () => Task.FromResult<List<int>?>(null), CancellationToken.None);

        // Assert
        Assert.Equal(0, written);
        Assert.Equal(0, buffer.BufferedBytes);
    }

    [Fact]
    public async Task Bytes_AreGivenBackWhenDecodingThrows()
    {
        // Arrange
        var buffer = new ReceiveBuffer<int>(BufferCapacity.Bytes(100));

        // Act
        await Assert.ThrowsAsync<InvalidDataException>(() => buffer.WriteChunkAsync(
            100, () => Task.FromException<List<int>?>(new InvalidDataException()), CancellationToken.None));

        // Assert: a chunk that failed to decode must not keep the budget it reserved.
        Assert.Equal(0, buffer.BufferedBytes);
    }

    // ---- Both dimensions ----

    [Fact]
    public async Task ItemsAndBytes_TheCountBindsFirstForSmallRecords()
    {
        // Arrange: ten thousand records would fit the byte budget many times over.
        var buffer = new ReceiveBuffer<int>(BufferCapacity.ItemsAndBytes(2, 1_000_000));
        await WriteChunk(buffer, wireBytes: 30, 1, 2);

        // Act
        var write = WriteChunk(buffer, wireBytes: 30, 3);

        // Assert: sixty bytes against a megabyte, and it still waits.
        Assert.True(await IsParked(write));
    }

    [Fact]
    public async Task ItemsAndBytes_TheBytesBindFirstForLargeItems()
    {
        // Arrange: two items nowhere near the count, well past the budget.
        var buffer = new ReceiveBuffer<int>(BufferCapacity.ItemsAndBytes(10_000, 100));
        await WriteChunk(buffer, wireBytes: 100, 1);

        // Act
        var write = WriteChunk(buffer, wireBytes: 100, 2);

        // Assert
        Assert.True(await IsParked(write));
    }

    // ---- Teardown ----

    [Fact]
    public async Task Complete_ReleasesAWriterParkedOnTheByteGate()
    {
        // Arrange
        var buffer = new ReceiveBuffer<int>(BufferCapacity.Bytes(100));
        await WriteChunk(buffer, wireBytes: 100, 1);
        var write = WriteChunk(buffer, wireBytes: 100, 2);
        Assert.True(await IsParked(write));

        // Act
        buffer.Complete();

        // Assert: a session torn down while the consumer was behind must not leave this pending.
        await Assert.ThrowsAsync<ChannelClosedException>(() => write.WaitAsync(TimeSpan.FromSeconds(5)));
    }

    [Fact]
    public async Task Cancellation_ReleasesAWriterParkedOnTheByteGate()
    {
        // Arrange
        var buffer = new ReceiveBuffer<int>(BufferCapacity.Bytes(100));
        await WriteChunk(buffer, wireBytes: 100, 1);

        using var cts = new CancellationTokenSource();
        var write = buffer.WriteChunkAsync(100, () => Task.FromResult<List<int>?>([2]), cts.Token);
        Assert.True(await IsParked(write));

        // Act
        await cts.CancelAsync();

        // Assert
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => write.WaitAsync(TimeSpan.FromSeconds(5)));
    }

    [Fact]
    public async Task Cancellation_PartWayThroughAChunk_PublishesNoneOfIt()
    {
        // Arrange: two items in, one slot left, and a three-item chunk to put through it.
        var buffer = new ReceiveBuffer<int>(BufferCapacity.Items(3));
        await WriteChunk(buffer, wireBytes: 20, 1, 2);

        using var cts = new CancellationTokenSource();
        var write = buffer.WriteChunkAsync(30, () => Task.FromResult<List<int>?>([3, 4, 5]), cts.Token);
        Assert.True(await IsParked(write));

        // Act: the stream is cut while the chunk is going in.
        await cts.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => write.WaitAsync(TimeSpan.FromSeconds(5)));

        // Assert: nothing of that chunk reached the consumer. The caller has no way to say "I took
        // the first two of these" - a batch is the smallest thing the protocol can resend - so
        // anything left behind here is handed over twice when the sender resends it.
        buffer.Complete();
        var received = new List<int>();
        await foreach (var item in buffer.ReadAllAsync(CancellationToken.None)) received.Add(item);
        Assert.Equal(new[] { 1, 2 }, received);
    }

    [Fact]
    public async Task Complete_StillDeliversWhatWasAlreadyBuffered()
    {
        // Arrange
        var buffer = new ReceiveBuffer<int>(BufferCapacity.ItemsAndBytes(100, 1000));
        await WriteChunk(buffer, wireBytes: 30, 1, 2, 3);

        // Act
        buffer.Complete();

        // Assert
        var received = new List<int>();
        await foreach (var item in buffer.ReadAllAsync(CancellationToken.None)) received.Add(item);
        Assert.Equal(new[] { 1, 2, 3 }, received);
    }
}
