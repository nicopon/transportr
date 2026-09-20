using System.Buffers;
using TransportR.Client.SignalR.Internal;
using Xunit;

namespace TransportR.Tests;

public class BatchEntryTests
{
    /// <summary>
    /// Counts disposals instead of returning a buffer, because the defect these tests guard is a
    /// second <c>ArrayPool.Return</c> — which throws nothing and shows up as corruption elsewhere.
    /// </summary>
    private sealed class CountingOwner : IMemoryOwner<byte>
    {
        private int _disposeCount;

        public int DisposeCount => Volatile.Read(ref _disposeCount);
        public Memory<byte> Memory => Memory<byte>.Empty;

        public void Dispose() => Interlocked.Increment(ref _disposeCount);
    }

    [Fact]
    public void Release_DisposesThePayloadOnce()
    {
        var owner = new CountingOwner();
        var entry = new BatchEntry(owner, 0);

        entry.Release();

        Assert.Equal(1, owner.DisposeCount);
    }

    [Fact]
    public void Release_PastZero_DoesNotDisposeAgain()
    {
        var owner = new CountingOwner();
        var entry = new BatchEntry(owner, 0);

        entry.Release();
        entry.Release();
        entry.Release();

        Assert.Equal(1, owner.DisposeCount);
    }

    [Fact]
    public void TryRetain_AfterRelease_ReturnsNull()
    {
        var owner = new CountingOwner();
        var entry = new BatchEntry(owner, 0);

        entry.Release();

        Assert.Null(entry.TryRetain());
        Assert.Equal(1, owner.DisposeCount);
    }

    [Fact]
    public void Retain_ThenRelease_KeepsThePayloadUntilTheLastReference()
    {
        var owner = new CountingOwner();
        var entry = new BatchEntry(owner, 0);

        Assert.NotNull(entry.TryRetain());
        entry.Release();
        Assert.Equal(0, owner.DisposeCount);

        entry.Release();
        Assert.Equal(1, owner.DisposeCount);
    }

    /// <summary>
    /// The releases that cross are the ones that matter: <c>BatchProcessor.DisposeAsync</c> walks
    /// <c>_unackedBatches</c> while the ack loop removes and releases the same entry.
    /// </summary>
    /// <remarks>
    /// Probabilistic in what it triggers, exact in what it asserts: a payload disposed twice is a
    /// failure whether or not this run happened to interleave.
    /// </remarks>
    [Fact]
    public void Release_FromTwoThreadsAtOnce_DisposesAtMostOnce()
    {
        for (var i = 0; i < 2_000; i++)
        {
            var owner = new CountingOwner();
            var entry = new BatchEntry(owner, 0);

            using var barrier = new Barrier(2);
            var releases = new[]
            {
                Task.Run(() => { barrier.SignalAndWait(); entry.Release(); }),
                Task.Run(() => { barrier.SignalAndWait(); entry.Release(); })
            };

            Task.WaitAll(releases);

            Assert.Equal(1, owner.DisposeCount);
        }
    }

    [Fact]
    public void PooledMemoryOwner_DisposedTwice_StaysConsistent()
    {
        var pooled = new PooledMemoryOwner(new byte[] { 1, 2, 3 });

        pooled.Dispose();
        pooled.Dispose();

        Assert.Throws<ObjectDisposedException>(() => pooled.Memory);
    }
}
