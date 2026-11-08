using System.Buffers;
using TransportR.Client.Batching;
using TransportR.Client.Configuration;
using TransportR.Protocol;
using System.Runtime.InteropServices;

namespace TransportR.Tests;

/// <summary>
/// Every way a chunk can leave <see cref="ReceiverState"/> returns its pooled memory exactly once:
/// a buffer returned twice is handed to two renters, and nothing throws when it happens.
/// </summary>
public class ReceiverStateOwnershipTests
{
    private static byte[] Payload(byte fill) => [fill, fill, fill, fill];

    private static ReceiverState StateOver(IChunkSink sink) => new(sink);

    [Fact]
    public async Task ARealPooledChunk_ThatTheConsumerDisposes_IsBackInTheArrayPool()
    {
        // The counting owners of the other tests say how often a chunk is disposed; this one says that
        // the dispose reaches the pool. The pool hands this thread back the last array it returned.
        var owner = new PooledPayload(5000);
        Assert.True(MemoryMarshal.TryGetArray<byte>(owner.Memory, out var segment));
        var rented = segment.Array!;

        var buffer = new ReceiveBuffer<ByteChunk>(BufferCapacity.ItemsAndBytes(100, 1 << 20));
        var sink = new ByteChunkSink(buffer);
        var state = StateOver(sink);
        await state.ProcessBatchAsync(0, owner, owner.Memory);
        state.Complete();

        ByteChunk? chunk = null;
        await foreach (var read in sink.ReadAllAsync(CancellationToken.None))
        {
            chunk = read;
        }

        // No await between the return and the rent: the same thread.
        chunk!.Dispose();
        Assert.Same(rented, ArrayPool<byte>.Shared.Rent(5000));
    }

    [Fact]
    public async Task InOrder_WhenTheSinkTakesIt_TheStateLeavesTheMemoryAlone()
    {
        var owner = new CountingOwner(Payload(1));
        var state = StateOver(new ScriptedSink((_, _) => true));

        var result = await state.ProcessBatchAsync(0, owner, owner.Memory);

        Assert.Equal(0, result.BatchIdToAck);
        Assert.Equal(0, owner.Disposals);
    }

    [Fact]
    public async Task InOrder_WhenTheSinkDeclinesIt_TheStateReturnsTheMemoryOnce()
    {
        var owner = new CountingOwner(Payload(1));
        var state = StateOver(new ScriptedSink((_, _) => false));

        await state.ProcessBatchAsync(0, owner, owner.Memory);

        Assert.Equal(1, owner.Disposals);
    }

    [Fact]
    public async Task InOrder_WhenTheSinkThrows_TheStateReturnsTheMemoryOnceAndDoesNotAdvance()
    {
        var owner = new CountingOwner(Payload(1));
        var state = StateOver(new ScriptedSink((_, _) => throw new InvalidOperationException("sink failed")));

        await Assert.ThrowsAsync<InvalidOperationException>(() => state.ProcessBatchAsync(0, owner, owner.Memory));

        Assert.Equal(1, owner.Disposals);
        Assert.Equal(0, state.NextExpectedBatchId);
    }

    [Fact]
    public async Task InOrder_WhenTheWaitForRoomIsCancelled_TheStateReturnsTheMemoryOnce()
    {
        // Arrange: a byte budget of one chunk, filled, so the next write parks on the credit gate.
        var buffer = new ReceiveBuffer<ByteChunk>(BufferCapacity.ItemsAndBytes(100, 4));
        var state = StateOver(new ByteChunkSink(buffer));
        var admitted = new CountingOwner(Payload(1));
        await state.ProcessBatchAsync(0, admitted, admitted.Memory);

        var parked = new CountingOwner(Payload(2));
        using var cts = new CancellationTokenSource();
        var blocked = state.ProcessBatchAsync(1, parked, parked.Memory, cts.Token);
        Assert.NotSame(blocked, await Task.WhenAny(blocked, Task.Delay(200)));

        // Act
        await cts.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => blocked);

        // Assert: the chunk that never got in is returned by the state; the admitted one is the buffer's.
        Assert.Equal(1, parked.Disposals);
        Assert.Equal(0, admitted.Disposals);
        Assert.Equal(1, state.NextExpectedBatchId);
    }

    [Fact]
    public async Task InOrder_WhenTheBufferIsCompletedUnderTheWrite_TheStateReturnsTheMemoryOnce()
    {
        var buffer = new ReceiveBuffer<ByteChunk>(capacity: null);
        var state = StateOver(new ByteChunkSink(buffer));
        buffer.Complete();
        var owner = new CountingOwner(Payload(1));

        await Assert.ThrowsAnyAsync<Exception>(() => state.ProcessBatchAsync(0, owner, owner.Memory));

        Assert.Equal(1, owner.Disposals);
    }

    [Fact]
    public async Task Duplicate_IsReturnedOnceAndNeverReachesTheSink()
    {
        var sink = new ScriptedSink((_, _) => true);
        var state = StateOver(sink);
        var first = new CountingOwner(Payload(1));
        await state.ProcessBatchAsync(0, first, first.Memory);

        var duplicate = new CountingOwner(Payload(1));
        var result = await state.ProcessBatchAsync(0, duplicate, duplicate.Memory);

        Assert.Equal(0, result.BatchIdToAck);
        Assert.Equal(1, duplicate.Disposals);
        Assert.Equal(1, sink.Calls);
    }

    [Fact]
    public async Task OutOfOrder_IsHeldWithoutBeingReturned_ThenHandedOnWhenTheGapIsFilled()
    {
        var sink = new ScriptedSink((_, _) => true);
        var state = StateOver(sink);
        var later = new CountingOwner(Payload(2));
        var gap = new CountingOwner(Payload(1));

        await state.ProcessBatchAsync(1, later, later.Memory);
        Assert.Equal(0, later.Disposals);
        Assert.Equal(1, state.ReorderedChunks);
        Assert.Equal(0, sink.Calls);

        await state.ProcessBatchAsync(0, gap, gap.Memory);

        Assert.Equal(2, sink.Calls);
        Assert.Equal(0, state.ReorderedChunks);
        Assert.Equal(2, state.NextExpectedBatchId);
        Assert.Equal(0, later.Disposals);
        Assert.Equal(0, gap.Disposals);
    }

    [Fact]
    public async Task OutOfOrder_WhenTheSinkDeclinesBoth_EachIsReturnedOnce()
    {
        var state = StateOver(new ScriptedSink((_, _) => false));
        var later = new CountingOwner(Payload(2));
        var gap = new CountingOwner(Payload(1));

        await state.ProcessBatchAsync(1, later, later.Memory);
        await state.ProcessBatchAsync(0, gap, gap.Memory);

        Assert.Equal(1, later.Disposals);
        Assert.Equal(1, gap.Disposals);
    }

    [Fact]
    public async Task OutOfOrder_WhenTheSinkThrowsOnTheHeldChunk_ItIsReturnedOnceAndNotKept()
    {
        // Arrange: the first write succeeds (taken), the second, of the chunk held out of order, throws.
        var state = StateOver(new ScriptedSink((call, _) => call == 1 ? true : throw new InvalidOperationException()));
        var later = new CountingOwner(Payload(2));
        var gap = new CountingOwner(Payload(1));
        await state.ProcessBatchAsync(1, later, later.Memory);

        // Act
        await Assert.ThrowsAsync<InvalidOperationException>(() => state.ProcessBatchAsync(0, gap, gap.Memory));

        // Assert
        Assert.Equal(1, later.Disposals);
        Assert.Equal(0, gap.Disposals);
        Assert.Equal(0, state.ReorderedChunks);
        state.DiscardReordered();
        Assert.Equal(1, later.Disposals);
    }

    [Fact]
    public async Task DiscardReordered_ReturnsEachHeldChunkOnce_EvenCalledTwice()
    {
        var state = StateOver(new ScriptedSink((_, _) => true));
        var second = new CountingOwner(Payload(2));
        var third = new CountingOwner(Payload(3));
        await state.ProcessBatchAsync(2, second, second.Memory);
        await state.ProcessBatchAsync(3, third, third.Memory);

        state.DiscardReordered();
        state.DiscardReordered();

        Assert.Equal(1, second.Disposals);
        Assert.Equal(1, third.Disposals);
    }

    [Fact]
    public async Task AChunkTheBufferAdmitted_IsReturnedWhenTheConsumerDisposesIt_AndNotBefore()
    {
        var buffer = new ReceiveBuffer<ByteChunk>(capacity: null);
        var state = StateOver(new ByteChunkSink(buffer));
        var owner = new CountingOwner(Payload(7));

        await state.ProcessBatchAsync(0, owner, owner.Memory);
        Assert.Equal(0, owner.Disposals);

        buffer.Complete();
        await foreach (var chunk in buffer.ReadAllAsync(CancellationToken.None))
        {
            Assert.Equal(Payload(7), chunk.Memory.ToArray());
            chunk.Dispose();
            chunk.Dispose();
        }

        Assert.Equal(1, owner.Disposals);
    }

    [Fact]
    public async Task DiscardUnread_ReturnsEachUntakenChunkOnce_AndNotTheOneAReaderHolds()
    {
        // Arrange: three chunks admitted, a reader holding the first.
        var buffer = new ReceiveBuffer<ByteChunk>(capacity: null);
        var sink = new ByteChunkSink(buffer);
        var state = StateOver(sink);
        var owners = new[] { new CountingOwner(Payload(1)), new CountingOwner(Payload(2)), new CountingOwner(Payload(3)) };
        for (var i = 0; i < owners.Length; i++) await state.ProcessBatchAsync(i, owners[i], owners[i].Memory);
        await using var reader = sink.ReadAllAsync(CancellationToken.None).GetAsyncEnumerator();
        Assert.True(await reader.MoveNextAsync());
        var held = reader.Current;

        // Act: the session is released, which completes the sink and then discards.
        sink.Complete();
        sink.DiscardUnread();
        sink.DiscardUnread();

        // Assert
        Assert.Equal(0, owners[0].Disposals);
        Assert.Equal(1, owners[1].Disposals);
        Assert.Equal(1, owners[2].Disposals);
        Assert.False(await reader.MoveNextAsync());
        held.Dispose();
        Assert.Equal(1, owners[0].Disposals);
    }

    private sealed class ScriptedSink(Func<int, IMemoryOwner<byte>, bool> onWrite) : IChunkSink
    {
        public int Calls { get; private set; }

        public ChunkType BatchType => ChunkType.StreamBatch;

        public async Task<bool> WriteAsync(IMemoryOwner<byte> owner, ReadOnlyMemory<byte> body, CancellationToken ct)
        {
            var call = ++Calls;
            await Task.Yield();
            return onWrite(call, owner);
        }

        public void Complete() { }

        public void Fail(Exception error) { }

        public void DiscardUnread() { }
    }
}

/// <summary>
/// A memory owner that counts its disposals, which an <see cref="IMemoryOwner{T}"/> over an array
/// cannot show: returning a pooled buffer twice is silent.
/// </summary>
internal sealed class CountingOwner(byte[] data) : IMemoryOwner<byte>
{
    private int _disposals;

    public int Disposals => Volatile.Read(ref _disposals);

    public Memory<byte> Memory => data;

    public void Dispose() => Interlocked.Increment(ref _disposals);
}
