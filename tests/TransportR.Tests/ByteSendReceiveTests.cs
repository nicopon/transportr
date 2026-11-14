using System.Buffers;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using TransportR.Client;
using TransportR.Client.Batching;
using TransportR.Client.Internal;
using TransportR.Client.Models;
using TransportR.Client.Sessions;

namespace TransportR.Tests;

/// <summary>
/// <c>CopyFromAsync</c> and <c>CopyToAsync</c> of the handles of a stream of bytes over a stand-in
/// session, which lets the tests count what is returned to the pool. The same calls over a real hub are
/// in <see cref="ByteStreamTransferTests"/>.
/// </summary>
public class ByteSendReceiveTests
{
    private const int ReadSize = 81920;

    private static byte[] Sequence(int length) => Enumerable.Range(0, length).Select(i => (byte)(i % 251)).ToArray();

    // --- CopyFromAsync ---

    [Fact]
    public async Task CopyFromAsync_OnAByteStreamHandle_AppendsBytesAndSendsNoItem()
    {
        var session = new FakeSession { IsByteStream = true };
        await using var send = new HttpByteSend(session, session.SendBytesAsync);
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
        await using var send = new HttpByteSend(session, session.SendBytesAsync);

        await send.CopyFromAsync(new MemoryStream(Sequence(1000)));

        Assert.Equal(0, session.CompleteCalls);
        Assert.Equal(0, session.DisposeCalls);
    }

    [Fact]
    public async Task CopyFromAsync_OnAnEmptySource_SendsNothing()
    {
        var session = new FakeSession { IsByteStream = true };
        await using var send = new HttpByteSend(session, session.SendBytesAsync);

        await send.CopyFromAsync(new MemoryStream());

        Assert.Empty(session.AppendCalls);
        Assert.Empty(session.Items);
    }

    [Fact]
    public async Task CopyFromAsync_WhenTheSourceThrows_ReportsItUnwrapped_AndTheHandleCanStillBeReleased()
    {
        var session = new FakeSession { IsByteStream = true };
        var send = new HttpByteSend(session, session.SendBytesAsync);

        var thrown = await Assert.ThrowsAsync<IOException>(() => send.CopyFromAsync(new ThrowingStream(Sequence(100))));

        Assert.Equal("The source failed.", thrown.Message);
        Assert.Equal(100, session.AppendedBytes.Count);
        await send.DisposeAsync();
        Assert.Equal(1, session.DisposeCalls);
    }

    [Fact]
    public async Task CopyFromAsync_WhenAnAppendFails_DoesNotReturnTheBufferThatAppendMayStillBeReading()
    {
        // A derived class may return from a failed write while it still reads the buffer: it is not returned to the pool.
        var session = new FakeSession { IsByteStream = true, AppendFailure = new IOException("The append failed.") };
        await using var send = new HttpByteSend(session, session.SendBytesAsync);

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
        await using var send = new HttpByteSend(session, session.SendBytesAsync);
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
        var send = new HttpByteSend(session, session.SendBytesAsync);

        var copy = send.CopyFromAsync(new MemoryStream(Sequence(1000)));

        // Nothing yielded the thread, so the return happened here.
        Assert.True(copy.IsCompletedSuccessfully);
        Assert.Same(seeded, ArrayPool<byte>.Shared.Rent(ReadSize));
    }

    [Fact]
    public async Task CopyFromAsync_WithCustomBufferSize_AppendsBytesAndUsesSpecifiedChunkSize()
    {
        const int customSize = 32_768;
        var session = new FakeSession { IsByteStream = true };
        await using var send = new HttpByteSend(session, session.SendBytesAsync);
        var source = Sequence(100_000);

        await send.CopyFromAsync(new MemoryStream(source), customSize);

        Assert.Equal(source, session.AppendedBytes.ToArray());
        Assert.All(session.AppendCalls, length => Assert.InRange(length, 1, customSize));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public async Task CopyFromAsync_WithInvalidBufferSize_ThrowsArgumentOutOfRangeException(int bufferSize)
    {
        var session = new FakeSession { IsByteStream = true };
        await using var send = new HttpByteSend(session, session.SendBytesAsync);

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            () => send.CopyFromAsync(new MemoryStream(Sequence(100)), bufferSize));
    }

    [Fact]
    public void CopyFromAsync_WithCustomBufferSize_ReturnsItsReadBufferToTheArrayPool()
    {
        const int customSize = 65_536;
        var seeded = ArrayPool<byte>.Shared.Rent(customSize);
        ArrayPool<byte>.Shared.Return(seeded);
        var session = new FakeSession { IsByteStream = true };
        var send = new HttpByteSend(session, session.SendBytesAsync);

        var copy = send.CopyFromAsync(new MemoryStream(Sequence(1000)), customSize);

        Assert.True(copy.IsCompletedSuccessfully);
        Assert.Same(seeded, ArrayPool<byte>.Shared.Rent(customSize));
    }

    // --- CopyToAsync ---

    [Fact]
    public async Task CopyToAsync_OnAByteStreamHandle_WritesEveryChunkInOrder_AndDisposesEachOnce()
    {
        var owners = new[] { new CountingOwner([1, 2, 3]), new CountingOwner([4, 5]), new CountingOwner([6]) };
        var session = new FakeSession { IsByteStream = true, Chunks = owners };
        await using var receive = new HttpByteReceive(session, session.ReceiveChunksAsync);
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
        await using var receive = new HttpByteReceive(session, session.ReceiveChunksAsync);

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
        await using var receive = new HttpByteReceive(session, session.ReceiveChunksAsync);
        using var cts = new CancellationTokenSource();
        var destination = new WaitingDestination();

        var copy = receive.CopyToAsync(destination, cts.Token);
        await destination.Writing.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await cts.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => copy.WaitAsync(TimeSpan.FromSeconds(5)));
        Assert.Equal(1, owners[0].Disposed);
        Assert.Equal(0, owners[1].Disposed);
    }

    // --- ReadAsync ---

    private static async Task<List<byte[]>> ReadAllAsync(ByteReceive receive, int bufferLength)
    {
        var reads = new List<byte[]>();
        var buffer = new byte[bufferLength];
        int read;
        while ((read = await receive.ReadAsync(buffer)) > 0) reads.Add(buffer[..read]);
        return reads;
    }

    [Fact]
    public async Task ReadAsync_GivesAtMostTheBufferOfTheBytesInOrder_AcrossChunks_AndDisposesEachChunkOnce()
    {
        var owners = new[] { new CountingOwner([1, 2, 3]), new CountingOwner([4, 5]), new CountingOwner([6]) };
        var session = new FakeSession { IsByteStream = true, Chunks = owners };
        await using var receive = new HttpByteReceive(session, session.ReceiveChunksAsync);

        var reads = await ReadAllAsync(receive, bufferLength: 2);

        Assert.Equal([[1, 2], [3], [4, 5], [6]], reads);
        Assert.All(owners, owner => Assert.Equal(1, owner.Disposed));
    }

    [Fact]
    public async Task ReadAsync_ReturnsZeroOnlyOnceTheTransferHasReachedItsOutcome_AndAgainOnTheNextCall()
    {
        var session = new FakeSession { IsByteStream = true, Chunks = [new CountingOwner([1])] };
        await using var receive = new HttpByteReceive(session, session.ReceiveChunksAsync);
        var buffer = new byte[8];

        Assert.Equal(1, await receive.ReadAsync(buffer));
        Assert.Equal(0, session.WaitCalls);

        Assert.Equal(0, await receive.ReadAsync(buffer));
        Assert.Equal(1, session.WaitCalls);
        Assert.Equal(0, await receive.ReadAsync(buffer));
    }

    [Fact]
    public async Task ReadAsync_WithAnEmptyBuffer_IsRefused_ForZeroMeansTheEnd()
    {
        var session = new FakeSession { IsByteStream = true, Chunks = [new CountingOwner([1])] };
        await using var receive = new HttpByteReceive(session, session.ReceiveChunksAsync);

        await Assert.ThrowsAsync<ArgumentException>(async () => await receive.ReadAsync(Memory<byte>.Empty));
    }

    [Fact]
    public async Task ReadAsync_WhenTheTransferFailed_HandsWhatWasAdmittedFirst_ThenThrowsTheFailure()
    {
        var owner = new CountingOwner([7, 8]);
        var session = new FakeSession { IsByteStream = true };
        await using var receive = new HttpByteReceive(session, token => ThenFailure(owner));
        var buffer = new byte[8];

        Assert.Equal(2, await receive.ReadAsync(buffer));
        var failure = await Assert.ThrowsAsync<TransferFailedException>(async () => await receive.ReadAsync(buffer));

        Assert.Equal("The transfer was ended.", failure.Message);
        Assert.Equal(1, owner.Disposed);
    }

    [Fact]
    public async Task ReadAsync_CutByItsToken_LosesNothing_TheNextCallGetsTheBytesThatWaitYielded()
    {
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var owner = new CountingOwner([1, 2, 3]);
        var session = new FakeSession { IsByteStream = true };
        await using var receive = new HttpByteReceive(session, token => AfterGate(gate.Task, owner));
        var buffer = new byte[8];
        using var cts = new CancellationTokenSource();

        var cut = receive.ReadAsync(buffer, cts.Token).AsTask();
        await cts.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => cut);
        gate.SetResult();

        Assert.Equal(3, await receive.ReadAsync(buffer));
        Assert.Equal([1, 2, 3], buffer[..3]);
        Assert.Equal(1, owner.Disposed);
    }

    [Fact]
    public async Task ReadAsync_AndCopyToAsync_ExcludeEachOther_WhileOneIsUnderWay()
    {
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var session = new FakeSession { IsByteStream = true };
        await using var receive = new HttpByteReceive(session, token => AfterGate(gate.Task, new CountingOwner([1])));
        var buffer = new byte[8];

        var reading = receive.ReadAsync(buffer).AsTask();

        await Assert.ThrowsAsync<InvalidOperationException>(async () => await receive.ReadAsync(buffer));
        await Assert.ThrowsAsync<InvalidOperationException>(() => receive.CopyToAsync(new MemoryStream()));
        gate.SetResult();
        Assert.Equal(1, await reading);
    }

    [Fact]
    public async Task ReadAsync_WhileACopyIsUnderWay_IsRefused()
    {
        var owners = new[] { new CountingOwner([1, 2, 3]), new CountingOwner([4]) };
        var session = new FakeSession { IsByteStream = true, Chunks = owners };
        await using var receive = new HttpByteReceive(session, session.ReceiveChunksAsync);
        using var cts = new CancellationTokenSource();
        var destination = new WaitingDestination();
        var copy = receive.CopyToAsync(destination, cts.Token);
        await destination.Writing.Task.WaitAsync(TimeSpan.FromSeconds(5));

        await Assert.ThrowsAsync<InvalidOperationException>(async () => await receive.ReadAsync(new byte[8]));
        await Assert.ThrowsAsync<InvalidOperationException>(() => receive.CopyToAsync(new MemoryStream()));

        await cts.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => copy.WaitAsync(TimeSpan.FromSeconds(5)));
        Assert.Equal(1, owners[0].Disposed);
    }

    [Fact]
    public async Task CopyToAsync_AfterAPartialRead_WritesTheRestOfThatChunkFirst_WithoutAnotherCopy()
    {
        var owners = new[] { new CountingOwner([1, 2, 3, 4]), new CountingOwner([5, 6]) };
        var session = new FakeSession { IsByteStream = true, Chunks = owners };
        await using var receive = new HttpByteReceive(session, session.ReceiveChunksAsync);
        Assert.Equal(1, await receive.ReadAsync(new byte[1]));
        var destination = new MemoryStream();

        await receive.CopyToAsync(destination);

        Assert.Equal([2, 3, 4, 5, 6], destination.ToArray());
        Assert.All(owners, owner => Assert.Equal(1, owner.Disposed));
    }

    [Fact]
    public async Task DisposeAsync_WhileAChunkIsHeldUnread_GivesItBackOnce_AndDoesItAgainWithoutHarm()
    {
        var owner = new CountingOwner([1, 2, 3]);
        var session = new FakeSession { IsByteStream = true, Chunks = [owner] };
        var receive = new HttpByteReceive(session, session.ReceiveChunksAsync);
        Assert.Equal(1, await receive.ReadAsync(new byte[1]));
        Assert.Equal(0, owner.Disposed);

        await receive.DisposeAsync();
        await receive.DisposeAsync();

        Assert.Equal(1, owner.Disposed);
        Assert.Equal(2, session.DisposeCalls);
    }

    private static async IAsyncEnumerable<ByteChunk> ThenFailure(CountingOwner owner)
    {
        await Task.Yield();
        yield return new ByteChunk(owner, owner.Memory.Length);
        throw new TransferFailedException("fake", "The transfer was ended.");
    }

    private static async IAsyncEnumerable<ByteChunk> AfterGate(Task gate, CountingOwner owner)
    {
        await gate;
        yield return new ByteChunk(owner, owner.Memory.Length);
    }

    // --- The abstract handles ---

    [Fact]
    public async Task ByteSend_CanBeDerivedFromByATestDouble_WhoseWriteAsyncServesCopyFromAsync()
    {
        await using var send = new RecordingByteSend();
        var source = Sequence(200_000);

        await send.CopyFromAsync(new MemoryStream(source));

        Assert.Equal(source, send.Written.ToArray());
        Assert.Equal("recording", send.TransferId);
        Assert.Equal(0, send.CompleteCalls);
    }

    [Fact]
    public async Task ByteReceive_CanBeDerivedFromByATestDouble()
    {
        await using var receive = new SequenceByteReceive([1, 2, 3]);
        var destination = new MemoryStream();

        await receive.CopyToAsync(destination);

        Assert.Equal([1, 2, 3], destination.ToArray());
        Assert.Equal("sequence", receive.TransferId);
        await receive.WaitForCompletionAsync();
    }

    private sealed class RecordingByteSend() : ByteSend("recording")
    {
        public List<byte> Written { get; } = [];
        public int CompleteCalls { get; private set; }

        public override Task WriteAsync(ReadOnlyMemory<byte> bytes, CancellationToken cancellationToken = default)
        {
            Written.AddRange(bytes.ToArray());
            return Task.CompletedTask;
        }

        public override Task CompleteAsync(CancellationToken cancellationToken = default)
        {
            CompleteCalls++;
            return Task.CompletedTask;
        }
    }

    private sealed class SequenceByteReceive(byte[] bytes) : ByteReceive("sequence")
    {
        private int _position;

        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            var count = Math.Min(buffer.Length, bytes.Length - _position);
            bytes.AsMemory(_position, count).CopyTo(buffer);
            _position += count;
            return ValueTask.FromResult(count);
        }

        public override async Task CopyToAsync(Stream destination, CancellationToken cancellationToken = default)
        {
            await destination.WriteAsync(bytes.AsMemory(_position), cancellationToken);
            _position = bytes.Length;
        }
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

    private sealed class FakeSession : IDataSession
    {
        public bool IsByteStream { get; init; }
        public Task? RefusedAsCompleted() => null;
        public Task WhileSending(Task send, CancellationToken cancellationToken) => send;
        public IReadOnlyList<CountingOwner> Chunks { get; init; } = [];

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
        public void TriggerReconnect() { }
        public int WaitCalls { get; private set; }

        public Task WaitForCompletionAsync(CancellationToken cancellationToken)
        {
            WaitCalls++;
            return Task.CompletedTask;
        }
        public Task EndByHubAsync() => Task.CompletedTask;
    }
}
