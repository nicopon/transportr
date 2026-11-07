using System.Buffers;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using TransportR.Abstractions;
using TransportR.Client;
using TransportR.Client.Batching;
using TransportR.Client.Internal;
using TransportR.Client.Sessions;
using TransportR.Models;

namespace TransportR.Tests;

/// <summary>
/// <c>CopyFromAsync</c> and <c>CopyToAsync</c> over a stand-in session, which says which path each took
/// and lets the tests count what is returned to the pool. The same calls over a real hub are in
/// <see cref="ByteStreamTransferTests"/>.
/// </summary>
public class ByteStreamExtensionsTests
{
    private const int ReadSize = 81920;

    private static byte[] Sequence(int length) => Enumerable.Range(0, length).Select(i => (byte)(i % 251)).ToArray();

    // --- CopyFromAsync ---

    [Fact]
    public async Task CopyFromAsync_OnAByteStreamHandle_AppendsBytesAndSendsNoItem()
    {
        var session = new FakeSession { IsByteStream = true };
        await using var send = new HttpSend<byte[]>(session, session);
        var source = Sequence(200_000);

        await send.CopyFromAsync(new MemoryStream(source));

        Assert.Equal(source, session.AppendedBytes.ToArray());
        Assert.Empty(session.Items);
        Assert.All(session.AppendCalls, length => Assert.InRange(length, 1, ReadSize));
    }

    [Fact]
    public async Task CopyFromAsync_NeitherCompletesNorDisposesTheHandle()
    {
        var session = new FakeSession { IsByteStream = true };
        await using var send = new HttpSend<byte[]>(session, session);

        await send.CopyFromAsync(new MemoryStream(Sequence(1000)));

        Assert.Equal(0, session.CompleteCalls);
        Assert.Equal(0, session.DisposeCalls);
    }

    [Fact]
    public async Task CopyFromAsync_OnAnEmptySource_SendsNothing()
    {
        var session = new FakeSession { IsByteStream = true };
        await using var send = new HttpSend<byte[]>(session, session);

        await send.CopyFromAsync(new MemoryStream());

        Assert.Empty(session.AppendCalls);
        Assert.Empty(session.Items);
    }

    [Fact]
    public async Task CopyFromAsync_OnAHandleThatIsNotInTheByteStreamMode_SendsEachReadAsAnItem_NeverAppending()
    {
        var session = new FakeSession { IsByteStream = false };
        await using var send = new HttpSend<byte[]>(session, session);
        var source = Sequence(2 * ReadSize + 5000);

        await send.CopyFromAsync(new MemoryStream(source));

        Assert.Empty(session.AppendCalls);
        Assert.Equal([ReadSize, ReadSize, 5000], session.Items.Select(item => item.Length));
        Assert.Equal(source, session.Items.SelectMany(item => item).ToArray());
    }

    [Fact]
    public async Task CopyFromAsync_OnAnotherKindOfHandle_SendsFreshItems_SoTheOnesKeptUntilTheirBatchIsFlushedAreNotOverwritten()
    {
        await using var send = new RecordingSend();
        var source = Sequence(2 * ReadSize + 5000);

        await send.CopyFromAsync(new MemoryStream(source));

        Assert.Equal([ReadSize, ReadSize, 5000], send.Items.Select(item => item.Length));
        Assert.Equal(3, send.Items.Distinct(ReferenceEqualityComparer.Instance).Count());
        Assert.Equal(source, send.Items.SelectMany(item => item).ToArray());
        Assert.Equal(0, send.CompleteCalls);
    }

    [Fact]
    public async Task CopyFromAsync_WhenTheSourceThrows_ReportsItUnwrapped_AndTheHandleCanStillBeReleased()
    {
        var session = new FakeSession { IsByteStream = true };
        var send = new HttpSend<byte[]>(session, session);

        var thrown = await Assert.ThrowsAsync<IOException>(() => send.CopyFromAsync(new ThrowingStream(Sequence(100))));

        Assert.Equal("The source failed.", thrown.Message);
        Assert.Equal(100, session.AppendedBytes.Count);
        await send.DisposeAsync();
        Assert.Equal(1, session.DisposeCalls);
    }

    [Fact]
    public async Task CopyFromAsync_WhenAnAppendFails_DoesNotReturnTheBufferThatAppendMayStillBeReading()
    {
        // A cancelled caller is let go before the producer it was waiting on: the buffer may still be read.
        var session = new FakeSession { IsByteStream = true, AppendFailure = new IOException("The append failed.") };
        await using var send = new HttpSend<byte[]>(session, session);

        await Assert.ThrowsAsync<IOException>(() => send.CopyFromAsync(new MemoryStream(Sequence(10))));

        var rented = ArrayPool<byte>.Shared.Rent(ReadSize);
        try
        {
            Assert.NotSame(session.LastAppendedBuffer, rented);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rented);
        }
    }

    [Fact]
    public async Task CopyFromAsync_WhenCancelled_Throws_AndAppendsNothingMore()
    {
        var session = new FakeSession { IsByteStream = true };
        await using var send = new HttpSend<byte[]>(session, session);
        using var cancelled = new CancellationTokenSource();
        await cancelled.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => send.CopyFromAsync(new MemoryStream(Sequence(1000)), cancelled.Token));

        Assert.Empty(session.AppendCalls);
    }

    [Fact]
    public void CopyFromAsync_ReturnsItsReadBufferToTheArrayPool_WhenTheCopyEnds()
    {
        // The pool hands this thread back the last array this thread returned. Seeded with one, a copy that
        // rents it and returns it leaves it for the next renter, and one that keeps it does not.
        var seeded = ArrayPool<byte>.Shared.Rent(ReadSize);
        ArrayPool<byte>.Shared.Return(seeded);
        var session = new FakeSession { IsByteStream = true };
        var send = new HttpSend<byte[]>(session, session);

        var copy = send.CopyFromAsync(new MemoryStream(Sequence(1000)));

        // Nothing yielded the thread, so the return happened here.
        Assert.True(copy.IsCompletedSuccessfully);
        Assert.Same(seeded, ArrayPool<byte>.Shared.Rent(ReadSize));
    }

    // --- CopyToAsync ---

    [Fact]
    public async Task CopyToAsync_OnAByteStreamHandle_WritesEveryChunkInOrder_AndDisposesEachOnce()
    {
        var owners = new[] { new CountingOwner([1, 2, 3]), new CountingOwner([4, 5]), new CountingOwner([6]) };
        var session = new FakeSession { IsByteStream = true, Chunks = owners };
        await using var receive = new HttpReceive<byte[]>(session, session);
        var destination = new MemoryStream();

        await receive.CopyToAsync(destination);

        Assert.Equal([1, 2, 3, 4, 5, 6], destination.ToArray());
        Assert.All(owners, owner => Assert.Equal(1, owner.Disposed));
        Assert.Equal(0, session.ItemReads);
        Assert.Equal(0, session.DisposeCalls);
    }

    [Fact]
    public async Task CopyToAsync_WhenTheDestinationThrows_DisposesTheChunkItWasWriting_Once_AndReportsTheException()
    {
        var owners = new[] { new CountingOwner([1, 2, 3]), new CountingOwner([4, 5]) };
        var session = new FakeSession { IsByteStream = true, Chunks = owners };
        await using var receive = new HttpReceive<byte[]>(session, session);

        var thrown = await Assert.ThrowsAsync<IOException>(() => receive.CopyToAsync(new ThrowingDestination()));

        Assert.Equal("The destination failed.", thrown.Message);
        Assert.Equal(1, owners[0].Disposed);
        // Not taken: the session disposes what a consumer leaves unread, as it is released.
        Assert.Equal(0, owners[1].Disposed);
    }

    [Fact]
    public async Task CopyToAsync_WhenCancelledDuringAWrite_DisposesTheChunkItWasWriting_Once()
    {
        var owners = new[] { new CountingOwner([1, 2, 3]), new CountingOwner([4, 5]) };
        var session = new FakeSession { IsByteStream = true, Chunks = owners };
        await using var receive = new HttpReceive<byte[]>(session, session);
        using var cts = new CancellationTokenSource();
        var destination = new WaitingDestination();

        var copy = receive.CopyToAsync(destination, cts.Token);
        await destination.Writing.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await cts.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => copy.WaitAsync(TimeSpan.FromSeconds(5)));
        Assert.Equal(1, owners[0].Disposed);
        Assert.Equal(0, owners[1].Disposed);
    }

    [Fact]
    public async Task CopyToAsync_OnAHandleThatIsNotInTheByteStreamMode_WritesEachItemInOrder_NeverReadingChunks()
    {
        var session = new FakeSession { IsByteStream = false, ItemsToReceive = [[1, 2], [3], [4, 5, 6]] };
        await using var receive = new HttpReceive<byte[]>(session, session);
        var destination = new MemoryStream();

        await receive.CopyToAsync(destination);

        Assert.Equal([1, 2, 3, 4, 5, 6], destination.ToArray());
        Assert.Equal(0, session.ChunkReads);
    }

    // --- Doubles ---

    private sealed class CountingOwner(byte[] data) : IMemoryOwner<byte>
    {
        private int _disposed;
        public int Disposed => Volatile.Read(ref _disposed);
        public Memory<byte> Memory => data;
        public void Dispose() => Interlocked.Increment(ref _disposed);
    }

    private sealed class ThrowingStream(byte[] data) : MemoryStream(data)
    {
        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            var read = await base.ReadAsync(buffer, cancellationToken);
            return read > 0 ? read : throw new IOException("The source failed.");
        }
    }

    private sealed class ThrowingDestination : MemoryStream
    {
        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
            => throw new IOException("The destination failed.");
    }

    private sealed class WaitingDestination : MemoryStream
    {
        public TaskCompletionSource Writing { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            Writing.TrySetResult();
            await Task.Delay(Timeout.Infinite, cancellationToken);
        }
    }

    private sealed class RecordingSend : Send<byte[]>
    {
        public RecordingSend() : base("recording") { }
        public List<byte[]> Items { get; } = [];
        public int CompleteCalls { get; private set; }

        public override Task SendAsync(byte[] item, CancellationToken cancellationToken = default)
        {
            Items.Add(item);
            return Task.CompletedTask;
        }

        public override Task CompleteAsync(CancellationToken cancellationToken = default)
        {
            CompleteCalls++;
            return Task.CompletedTask;
        }
    }

    private sealed class FakeSession : IDataSession, ISendPort<byte[]>, IReceivePort<byte[]>
    {
        public bool IsByteStream { get; init; }
        public IBatchSource Source => throw new NotSupportedException();
        public IChunkSink OpenSink() => throw new NotSupportedException();
        public Task? RefusedAsCompleted() => null;
        public Task WhileSending(Task send, CancellationToken cancellationToken) => send;
        public IReadOnlyList<CountingOwner> Chunks { get; init; } = [];
        public IReadOnlyList<byte[]> ItemsToReceive { get; init; } = [];

        public List<byte> AppendedBytes { get; } = [];
        public List<int> AppendCalls { get; } = [];
        public List<byte[]> Items { get; } = [];
        public int CompleteCalls { get; private set; }
        public int DisposeCalls { get; private set; }
        public int ChunkReads { get; private set; }
        public int ItemReads { get; private set; }

        public Exception? AppendFailure { get; init; }
        public byte[]? LastAppendedBuffer { get; private set; }

        public Task SendBytesAsync(ReadOnlyMemory<byte> bytes, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (MemoryMarshal.TryGetArray(bytes, out var segment)) LastAppendedBuffer = segment.Array;
            if (AppendFailure is not null) throw AppendFailure;
            AppendCalls.Add(bytes.Length);
            AppendedBytes.AddRange(bytes.ToArray());
            return Task.CompletedTask;
        }

        public Task SendAsync(byte[] item, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Items.Add(item);
            return Task.CompletedTask;
        }

        public Task CompleteAsync(CancellationToken cancellationToken = default)
        {
            CompleteCalls++;
            return Task.CompletedTask;
        }

        public async IAsyncEnumerable<ByteChunk> ReceiveChunksAsync([EnumeratorCancellation] CancellationToken cancellationToken)
        {
            ChunkReads++;
            foreach (var owner in Chunks)
            {
                await Task.Yield();
                yield return new ByteChunk(owner, owner.Memory.Length);
            }
        }

        public async IAsyncEnumerable<byte[]> ReceiveAsync([EnumeratorCancellation] CancellationToken cancellationToken)
        {
            ItemReads++;
            foreach (var item in ItemsToReceive)
            {
                await Task.Yield();
                yield return item;
            }
        }

        public ValueTask DisposeAsync()
        {
            DisposeCalls++;
            return ValueTask.CompletedTask;
        }

        public string TransferId => "fake";
        public int BatchSize => 1;
        public string DataUrl => "http://localhost";
        public bool IsSender => true;
        public SessionState State => SessionState.Streaming;
        public WriteTransport? WriteTransport => null;
        public string? WriteTransportFallback => null;
        public long ItemsProcessed => 0;
        public long BytesTransferred => 0;
        public long AcksReceived => 0;
        public long AcksSent => 0;
        public DateTime StartedAt => DateTime.UtcNow;
        public Task StartSenderAsync() => Task.CompletedTask;
        public Task StartReceiverAsync() => Task.CompletedTask;
        public Task SendRangeAsync(IEnumerable<byte[]> items, CancellationToken cancellationToken) => Task.CompletedTask;
        public void TriggerReconnect() { }
        public Task WaitForCompletionAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public Task EndByHubAsync() => Task.CompletedTask;
    }
}
