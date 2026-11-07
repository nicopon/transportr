using System.Buffers.Binary;
using System.Threading.Channels;
using Microsoft.IO;
using TransportR.Client.Batching;

namespace TransportR.Tests;

/// <summary>
/// The cutting of a byte stream into batches, and the rule that every way out of the accumulator
/// gives a slot back and disposes a stream exactly once. "Once" is counted twice over: by the
/// releases the accumulator makes through its callback, against a window whose maximum makes an
/// over-release throw, and by what the stream manager reports created, disposed and disposed twice.
/// </summary>
public class ByteStreamAccumulatorTests
{
    private const int Budget = 4;

    private readonly CountedStreams _streams = new(new RecyclableMemoryStreamManager());
    private int _releases;

    private ByteStreamAccumulator Create(
        SemaphoreSlim? slots = null, int flushIntervalMs = 60_000, CountedStreams? streams = null) =>
        new(Budget, flushIntervalMs, (streams ?? _streams).Manager, slots, () =>
        {
            Interlocked.Increment(ref _releases);
            slots!.Release();
        });

    private static SemaphoreSlim Window(int size) => new(size, size);

    private static byte[] Sequence(int length, byte from = 1) =>
        Enumerable.Range(0, length).Select(i => (byte)(from + i)).ToArray();

    /// <summary>Reads one batch as the send worker would, and disposes its stream.</summary>
    private static (int BatchId, int Length, byte[] Body) Take(ByteStreamAccumulator accumulator)
    {
        Assert.True(accumulator.BatchReader.TryRead(out var handed), "No batch was handed off.");
        using var batch = handed.Batch;
        var buffer = batch.GetBuffer();
        return (handed.BatchId, BinaryPrimitives.ReadInt32LittleEndian(buffer),
            buffer.AsSpan(sizeof(int), (int)batch.Length - sizeof(int)).ToArray());
    }

    private static List<(int BatchId, int Length, byte[] Body)> TakeAll(ByteStreamAccumulator accumulator)
    {
        var batches = new List<(int, int, byte[])>();
        while (accumulator.BatchReader.Count > 0) batches.Add(Take(accumulator));
        return batches;
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

    private static async Task AssertParked(Task task) =>
        Assert.NotSame(task, await Task.WhenAny(task, Task.Delay(200)));

    [Fact]
    public async Task AddAsync_ReachingTheBudget_HandsTheBatchOffAtOnce_WithItsLength()
    {
        using var accumulator = Create();

        await accumulator.AddAsync(Sequence(Budget));

        var batch = Take(accumulator);
        Assert.Equal(0, batch.BatchId);
        Assert.Equal(Budget, batch.Length);
        Assert.Equal(Sequence(Budget), batch.Body);
    }

    [Fact]
    public async Task AddAsync_BelowTheBudget_HoldsTheBatch_UntilTheFlushIntervalElapses()
    {
        using var accumulator = Create(flushIntervalMs: 50);
        accumulator.StartFlushInterval();

        await accumulator.AddAsync(Sequence(Budget - 1));

        var handed = await accumulator.BatchReader.ReadAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5));
        using var batch = handed.Batch;
        Assert.Equal(Budget - 1 + sizeof(int), batch.Length);
        Assert.Equal(Budget - 1, BinaryPrimitives.ReadInt32LittleEndian(batch.GetBuffer()));
    }

    [Fact]
    public async Task CompleteAsync_HandsOffThePartialBatch_AndCompletesTheChannel()
    {
        using var accumulator = Create();
        await accumulator.AddAsync(Sequence(Budget - 1));
        Assert.Equal(0, accumulator.BatchReader.Count);

        await accumulator.CompleteAsync();

        var batch = Take(accumulator);
        Assert.Equal(Budget - 1, batch.Length);
        Assert.Equal(Sequence(Budget - 1), batch.Body);
        Assert.True(accumulator.BatchReader.Completion.IsCompleted);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(Budget)]
    [InlineData(Budget * 3)]     // the budget divides the stream: no empty batch at the end
    [InlineData(Budget * 3 + 1)] // it does not
    public async Task AddAsync_IsCutIntoBatchesOfAtMostTheBudget_NoneEmpty_WhoseBytesJoinUpIdentical(int length)
    {
        using var accumulator = Create();
        var bytes = Sequence(length);

        await accumulator.AddAsync(bytes);
        await accumulator.CompleteAsync();

        var batches = TakeAll(accumulator);
        Assert.Equal((length + Budget - 1) / Budget, batches.Count);
        Assert.Equal(Enumerable.Range(0, batches.Count), batches.Select(b => b.BatchId));
        Assert.All(batches, b => Assert.InRange(b.Length, 1, Budget));
        Assert.All(batches, b => Assert.Equal(b.Length, b.Body.Length));
        Assert.Equal(bytes, batches.SelectMany(b => b.Body));
    }

    [Fact]
    public async Task AddAsync_KeepsNoBoundaryBetweenCalls()
    {
        using var accumulator = Create();
        var bytes = Sequence(9);

        await accumulator.AddAsync(bytes.AsMemory(0, 3));
        await accumulator.AddAsync(bytes.AsMemory(3, 3));
        await accumulator.AddAsync(bytes.AsMemory(6, 3));
        await accumulator.CompleteAsync();

        var batches = TakeAll(accumulator);
        Assert.Equal([Budget, Budget, 1], batches.Select(b => b.Length));
        Assert.Equal(bytes, batches.SelectMany(b => b.Body));
    }

    [Fact]
    public async Task AddAsync_WithNoByte_OpensNothing_AndCompleteSendsNothing()
    {
        var slots = Window(1);
        using var accumulator = Create(slots);

        await accumulator.AddAsync(ReadOnlyMemory<byte>.Empty);
        await accumulator.CompleteAsync();

        Assert.Equal(1, slots.CurrentCount);
        Assert.Equal(0, _streams.Created);
        Assert.False(accumulator.BatchReader.TryRead(out _));
    }

    [Fact]
    public async Task AddAsync_WhenTheWindowIsFull_ParksBeforeRentingAnything_UntilASlotComesBack()
    {
        var slots = Window(1);
        using var accumulator = Create(slots);
        await accumulator.AddAsync(Sequence(Budget));
        Take(accumulator);

        var parked = accumulator.AddAsync(Sequence(1)).AsTask();

        await AssertParked(parked);
        Assert.Equal(1, _streams.Created);

        // What the acknowledgement of batch 0 does.
        slots.Release();

        await parked.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(2, _streams.Created);
        Assert.Equal(0, _releases);
    }

    // ---- Every way out: one slot given back, one stream disposed, each exactly once ----

    [Fact]
    public async Task Exit1_ABatchHandedOff_TakesItsSlotAndStreamWithIt()
    {
        var slots = Window(2);
        using var accumulator = Create(slots);

        await accumulator.AddAsync(Sequence(Budget));

        Assert.Equal(1, slots.CurrentCount);
        Assert.Equal(0, _streams.Disposed);
        Take(accumulator);
        Assert.Equal(1, _streams.Disposed);

        accumulator.Dispose();
        await Task.Delay(50);
        Assert.Equal(1, _streams.Disposed);
        Assert.Equal(0, _streams.DisposedTwice);
        Assert.Equal(0, _releases);
        Assert.Equal(1, slots.CurrentCount);
    }

    [Fact]
    public async Task Exit2_CompleteAsync_WithAnOpenBatchStillEmpty_SendsNothing_AndGivesBothBack()
    {
        // A stream that refuses to grow past the length prefix: the append that follows the opening
        // of the batch throws, which leaves it open and empty.
        var streams = new CountedStreams(new RecyclableMemoryStreamManager(
            new RecyclableMemoryStreamManager.Options { BlockSize = sizeof(int), MaximumStreamCapacity = sizeof(int) }));
        var slots = Window(1);
        using var accumulator = Create(slots, streams: streams);
        await Assert.ThrowsAnyAsync<Exception>(() => accumulator.AddAsync(Sequence(1)).AsTask());
        Assert.Equal(0, slots.CurrentCount);
        Assert.Equal(1, streams.Created);

        await accumulator.CompleteAsync();

        Assert.False(accumulator.BatchReader.TryRead(out _));
        Assert.Equal(1, slots.CurrentCount);
        Assert.Equal(1, _releases);
        Assert.Equal(1, streams.Disposed);
        accumulator.Dispose();
        await Task.Delay(50);
        Assert.Equal(1, _releases);
        Assert.Equal(1, streams.Disposed);
        Assert.Equal(0, streams.DisposedTwice);
    }

    [Fact]
    public async Task Exit3_Abandon_WhileTheProducerWaitsForASlot_ReleasesIt_HoldingNothing()
    {
        var slots = Window(1);
        using var accumulator = Create(slots);
        await accumulator.AddAsync(Sequence(Budget));
        Take(accumulator);
        var parked = accumulator.AddAsync(Sequence(1)).AsTask();
        await AssertParked(parked);

        accumulator.Abandon();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => parked.WaitAsync(TimeSpan.FromSeconds(5)));
        Assert.Equal(1, _streams.Created);
        Assert.Equal(0, _releases);
        // Batch 0 still holds the only slot: the window was not widened behind its back.
        Assert.Equal(0, slots.CurrentCount);
    }

    [Fact]
    public async Task Exit3_Abandon_WithAPartialBatch_GivesBackItsSlotAndStream_Once()
    {
        var slots = Window(1);
        using var accumulator = Create(slots);
        await accumulator.AddAsync(Sequence(Budget - 1));
        Assert.Equal(0, slots.CurrentCount);

        accumulator.Abandon();

        await WaitUntil(() => slots.CurrentCount == 1);
        Assert.Equal(1, _releases);
        Assert.Equal(1, _streams.Disposed);
        Assert.False(accumulator.BatchReader.TryRead(out _));

        // Nothing is opened after it, and the disposal that follows finds nothing left to give back.
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => accumulator.AddAsync(Sequence(1)).AsTask());
        accumulator.Dispose();
        await Task.Delay(50);
        Assert.Equal(1, _releases);
        Assert.Equal(1, _streams.Created);
        Assert.Equal(1, _streams.Disposed);
        Assert.Equal(0, _streams.DisposedTwice);
    }

    [Fact]
    public async Task Exit4_Dispose_WithAPartialBatch_GivesBackItsSlotAndStream_Once()
    {
        var slots = Window(1);
        var accumulator = Create(slots);
        await accumulator.AddAsync(Sequence(Budget - 1));

        accumulator.Dispose();
        accumulator.Dispose();

        await WaitUntil(() => slots.CurrentCount == 1);
        await Task.Delay(50);
        Assert.Equal(1, _releases);
        Assert.Equal(1, _streams.Disposed);
        Assert.Equal(0, _streams.DisposedTwice);
    }

    [Fact]
    public async Task Exit4_Dispose_DisposesTheBatchesNobodyRead_AndGivesBackTheirSlots()
    {
        var slots = Window(3);
        var accumulator = Create(slots);
        await accumulator.AddAsync(Sequence(Budget * 2 + 1));
        Assert.Equal(2, accumulator.BatchReader.Count);
        Assert.Equal(0, slots.CurrentCount);

        accumulator.Dispose();

        await WaitUntil(() => slots.CurrentCount == 3);
        Assert.Equal(3, _releases);
        Assert.Equal(3, _streams.Created);
        Assert.Equal(3, _streams.Disposed);
        Assert.Equal(0, _streams.DisposedTwice);
    }

    [Fact]
    public async Task Exit5_ACallCancelledWhileParked_LeavesNoSlotOrStreamBehind_AndTheWindowWhole()
    {
        var slots = Window(1);
        using var accumulator = Create(slots);
        using var cancel = new CancellationTokenSource();

        // Batch 0 is cut and handed off, then the rest waits for the slot batch 0 holds.
        var call = accumulator.AddAsync(Sequence(Budget + 2), cancel.Token).AsTask();
        await AssertParked(call);
        cancel.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => call.WaitAsync(TimeSpan.FromSeconds(5)));
        Assert.Equal(1, _streams.Created);
        Assert.Equal(Sequence(Budget), Take(accumulator).Body);

        slots.Release();
        await accumulator.AddAsync(Sequence(1, from: 9));
        await accumulator.CompleteAsync();

        var batch = Take(accumulator);
        Assert.Equal(1, batch.BatchId);
        Assert.Equal([9], batch.Body);
        Assert.Equal(0, _releases);
    }

    [Fact]
    public async Task AddAsync_AfterAbandonOrDispose_WithoutAWindow_RentsNothing()
    {
        // Without a window there is no cancelled wait to refuse the call: a batch opened now would be
        // opened after the release that gives back what is held, and held by nobody.
        using var abandoned = Create();
        abandoned.Abandon();
        var disposed = Create();
        disposed.Dispose();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => abandoned.AddAsync(Sequence(1)).AsTask());
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => disposed.AddAsync(Sequence(1)).AsTask());
        Assert.Equal(0, _streams.Created);
    }

    [Fact]
    public async Task ABatchCutAfterTheChannelCompleted_IsDisposedWithItsSlot_AndTheCallFails()
    {
        var slots = Window(1);
        using var accumulator = Create(slots);
        await accumulator.CompleteAsync();

        await Assert.ThrowsAsync<ChannelClosedException>(() => accumulator.AddAsync(Sequence(Budget)).AsTask());

        Assert.Equal(1, slots.CurrentCount);
        Assert.Equal(1, _releases);
        Assert.Equal(1, _streams.Created);
        Assert.Equal(1, _streams.Disposed);
    }

    [Fact]
    public async Task Abandon_RacingAProducer_DisposesEveryStreamOnce_AndNeverUnderTheProducer()
    {
        for (var round = 0; round < 25; round++)
        {
            var streams = new CountedStreams(new RecyclableMemoryStreamManager());
            var slots = Window(4);
            var released = 0;
            // 2 ms, so that the flush interval cuts batches during the race.
            var accumulator = new ByteStreamAccumulator(Budget, 2, streams.Manager, slots, () =>
            {
                Interlocked.Increment(ref released);
                slots.Release();
            });
            accumulator.StartFlushInterval();

            // The send worker and the acknowledgements, in one.
            var reader = Task.Run(async () =>
            {
                await foreach (var (batch, _) in accumulator.BatchReader.ReadAllAsync())
                {
                    batch.Dispose();
                    slots.Release();
                }
            });
            var producer = Task.Run(async () =>
            {
                var sizes = new Random(round);
                while (true) await accumulator.AddAsync(Sequence(sizes.Next(1, Budget * 3)));
            });

            await Task.Delay(5 + round % 10);
            accumulator.Abandon();

            var failure = await Assert.ThrowsAnyAsync<Exception>(() => producer.WaitAsync(TimeSpan.FromSeconds(5)));
            Assert.True(failure is OperationCanceledException or ChannelClosedException,
                $"The producer failed with {failure.GetType().Name}, not a release.");
            await reader.WaitAsync(TimeSpan.FromSeconds(5));
            accumulator.Dispose();

            await WaitUntil(() => streams.Disposed == streams.Created && slots.CurrentCount == 4);
            Assert.Equal(0, streams.DisposedTwice);
        }
    }

    private sealed class CountedStreams
    {
        private int _created;
        private int _disposed;
        private int _disposedTwice;

        public CountedStreams(RecyclableMemoryStreamManager manager)
        {
            Manager = manager;
            manager.StreamCreated += (_, _) => Interlocked.Increment(ref _created);
            manager.StreamDisposed += (_, _) => Interlocked.Increment(ref _disposed);
            manager.StreamDoubleDisposed += (_, _) => Interlocked.Increment(ref _disposedTwice);
        }

        public RecyclableMemoryStreamManager Manager { get; }
        public int Created => Volatile.Read(ref _created);
        public int Disposed => Volatile.Read(ref _disposed);
        public int DisposedTwice => Volatile.Read(ref _disposedTwice);
    }
}
