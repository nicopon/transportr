using System.Text;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using TransportR.Hub.Streams;

namespace TransportR.Tests;

/// <summary>
/// The hub's side of a write stream sent in segments: the stream a bridge copies from is made of
/// them in the order of their offsets, a resend is relayed from where the peer stands, and nothing
/// that does not follow on from the stream gets in.
/// </summary>
public class UploadEpochTests
{
    private static readonly TimeSpan Patience = TimeSpan.FromSeconds(10);

    [Fact]
    public async Task Segments_InTheOrderOfTheirOffsets_MakeUpTheStream()
    {
        var peer = new Peer();
        var epoch = Started(peer);

        Assert.Equal(new SegmentOutcome(SegmentStatus.Relayed, 3), await AcceptAsync(epoch, 0, "abc"));
        Assert.Equal(new SegmentOutcome(SegmentStatus.Relayed, 6), await AcceptAsync(epoch, 3, "def"));
        Assert.Equal(new SegmentOutcome(SegmentStatus.Relayed, 6), await AcceptAsync(epoch, 6, "", final: true));

        await epoch.Completion.WaitAsync(Patience);
        Assert.Equal("abcdef", peer.Received);
        Assert.True(peer.SawTheEnd);
    }

    [Fact]
    public async Task ASegmentSentAgain_IsRelayedFromWhereThePeerStands()
    {
        var peer = new Peer();
        var epoch = Started(peer);

        await AcceptAsync(epoch, 0, "abc");
        var outcome = await AcceptAsync(epoch, 1, "bcdef");

        Assert.Equal(new SegmentOutcome(SegmentStatus.Relayed, 6), outcome);
        Assert.Equal("abcdef", peer.Received);
    }

    [Fact]
    public async Task ASegmentThePeerHasAlready_IsAnsweredWithoutBeingRelayedAgain()
    {
        var peer = new Peer();
        var epoch = Started(peer);

        await AcceptAsync(epoch, 0, "abc");
        var outcome = await AcceptAsync(epoch, 0, "abc");

        Assert.Equal(new SegmentOutcome(SegmentStatus.Relayed, 3), outcome);
        Assert.Equal("abc", peer.Received);
    }

    [Fact]
    public async Task ASegmentBeyondWhatThePeerTook_IsAConflict_AndRelaysNothing()
    {
        var peer = new Peer();
        var epoch = Started(peer);

        await AcceptAsync(epoch, 0, "abc");
        var outcome = await AcceptAsync(epoch, 5, "x");

        Assert.Equal(new SegmentOutcome(SegmentStatus.Conflict, 3), outcome);
        Assert.Equal("abc", peer.Received);
    }

    [Fact]
    public async Task ASegmentOfAnEpochWhoseStreamIsOver_IsAConflict()
    {
        var epoch = Started(new Peer());

        await AcceptAsync(epoch, 0, "abc", final: true);
        var outcome = await AcceptAsync(epoch, 3, "d");

        Assert.Equal(SegmentStatus.Conflict, outcome.Status);
    }

    [Fact]
    public async Task ASegmentThePeerIsSlowToTake_IsHeldAtTheBound_AndTheResendCarriesOnWhereThePeerStands()
    {
        // The peer takes two bytes, then cannot write them on until the gate opens: a destination that
        // is full.
        var peer = new Peer(readSize: 2) { Gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously) };
        var epoch = Started(peer, holdBound: TimeSpan.FromMilliseconds(200));

        var held = await AcceptAsync(epoch, 0, "abcdef");

        // Taken is not relayed: the client is told nothing got through, and sends it all again.
        Assert.Equal(new SegmentOutcome(SegmentStatus.Held, 0), held);

        peer.Gate!.SetResult();
        var resent = await AcceptAsync(epoch, 0, "abcdef");

        Assert.Equal(new SegmentOutcome(SegmentStatus.Relayed, 6), resent);
        Assert.Equal("abcdef", peer.Received);
    }

    [Fact]
    public async Task ASegmentWithNoPeerToTakeIt_IsHeldAtTheBound_WithNothingRelayed()
    {
        var epoch = new UploadEpoch(Guid.NewGuid(), "t:Data", null, TimeSpan.FromSeconds(15), TimeProvider.System, NullLogger.Instance, TimeSpan.FromMilliseconds(200));
        epoch.Start((_, token) => Task.Delay(Timeout.Infinite, token).ContinueWith(_ => { }), () => { });

        var outcome = await AcceptAsync(epoch, 0, "abc");

        Assert.Equal(new SegmentOutcome(SegmentStatus.Held, 0), outcome);
        epoch.Abandon();
    }

    [Fact]
    public async Task TheLastSegment_IsAnsweredOnlyOnceTheStreamItEndsIsClosed()
    {
        var closed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var peer = new Peer { AfterTheEnd = closed.Task };
        var epoch = Started(peer);

        var last = AcceptAsync(epoch, 0, "abc", final: true);

        await UntilAsync(() => peer.SawTheEnd);
        await Task.Delay(100);
        Assert.False(last.IsCompleted, "the last segment was answered before the bridge closed the stream");

        closed.SetResult();
        Assert.Equal(new SegmentOutcome(SegmentStatus.Relayed, 3), await last.WaitAsync(Patience));
    }

    [Fact]
    public async Task TheEndOfAStream_ReachesTheBridgeWithItsLastBytes()
    {
        // The bridge's next read, straight after the last bytes, finds the end without waiting on the
        // request that brought them: a peer that has those bytes may already be closing its side.
        var endAtOnce = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var epoch = new UploadEpoch(Guid.NewGuid(), "t:Control", null, TimeSpan.FromSeconds(15), TimeProvider.System, NullLogger.Instance);
        epoch.Start(async (input, token) =>
        {
            var buffer = new byte[1024];
            var last = await input.ReadAsync(buffer, token);
            var next = input.ReadAsync(buffer, token);
            endAtOnce.TrySetResult(last == 3 && next.IsCompletedSuccessfully && next.Result == 0);
            await next;
        }, () => { });

        // The request's body says it has no more only a while after its last bytes, as one still
        // arriving does.
        await epoch.AcceptAsync(0, new SlowToEnd("abc"), final: true, CancellationToken.None).WaitAsync(Patience);

        Assert.True(await endAtOnce.Task.WaitAsync(Patience), "the bridge read the last bytes before it could read the end");
    }

    private sealed class SlowToEnd(string text) : Stream
    {
        private byte[]? _bytes = Encoding.ASCII.GetBytes(text);

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            if (_bytes == null)
            {
                await Task.Delay(200, cancellationToken);
                return 0;
            }

            _bytes.CopyTo(buffer);
            var count = _bytes.Length;
            _bytes = null;
            return count;
        }

        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
        public override void Flush() { }
        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }

    [Fact]
    public async Task AnEpochWithNoSegmentForTwoHeartbeatIntervals_IsTakenAsCut()
    {
        var time = new FakeTimeProvider();
        var epoch = new UploadEpoch(Guid.NewGuid(), "t:Data", null, TimeSpan.FromSeconds(1), time, NullLogger.Instance);
        epoch.Start((_, token) => Task.Delay(Timeout.Infinite, token).ContinueWith(_ => { }), () => { });

        time.Advance(TimeSpan.FromMilliseconds(1900));
        await Task.Delay(50);
        Assert.False(epoch.Token.IsCancellationRequested, "the epoch was cut before two intervals had passed");

        time.Advance(TimeSpan.FromMilliseconds(200));
        await UntilAsync(() => epoch.Token.IsCancellationRequested);
        await epoch.Completion.WaitAsync(Patience);
    }

    [Fact]
    public async Task ASegmentUnderWay_IsNotSilence()
    {
        // The hold bound is past the silence limit, and the segment held that long keeps the epoch.
        var time = new FakeTimeProvider();
        var epoch = new UploadEpoch(Guid.NewGuid(), "t:Data", null, TimeSpan.FromSeconds(1), time, NullLogger.Instance, TimeSpan.FromSeconds(10));
        epoch.Start((_, token) => Task.Delay(Timeout.Infinite, token).ContinueWith(_ => { }), () => { });

        var held = AcceptAsync(epoch, 0, "abc");
        await Task.Delay(50);
        time.Advance(TimeSpan.FromSeconds(5));
        await Task.Delay(50);
        Assert.False(epoch.Token.IsCancellationRequested, "a segment under way was taken for silence");

        time.Advance(TimeSpan.FromSeconds(5));
        Assert.Equal(SegmentStatus.Held, (await held.WaitAsync(Patience)).Status);

        // The clock starts again from the answer.
        time.Advance(TimeSpan.FromMilliseconds(1900));
        await Task.Delay(50);
        Assert.False(epoch.Token.IsCancellationRequested);

        time.Advance(TimeSpan.FromMilliseconds(200));
        await UntilAsync(() => epoch.Token.IsCancellationRequested);
    }

    [Fact]
    public void ANewEpoch_EndsTheOneItReplaces()
    {
        var epochs = new UploadEpochs(TimeProvider.System, NullLogger<UploadEpochs>.Instance);
        var first = epochs.Open("t:Data", Guid.NewGuid(), null, TimeSpan.FromSeconds(15), WaitForCancellation);
        var second = epochs.Open("t:Data", Guid.NewGuid(), null, TimeSpan.FromSeconds(15), WaitForCancellation);

        Assert.True(first.Token.IsCancellationRequested);
        Assert.False(epochs.TryGet("t:Data", first.Id, out _));
        Assert.True(epochs.TryGet("t:Data", second.Id, out var current));
        Assert.Same(second, current);
        second.Abandon();
    }

    [Fact]
    public async Task AnEpochLeaves_WhenTheBridgeIsDoneWithItsStream()
    {
        var epochs = new UploadEpochs(TimeProvider.System, NullLogger<UploadEpochs>.Instance);
        var epoch = epochs.Open("t:Data", Guid.NewGuid(), null, TimeSpan.FromSeconds(15), (_, _) => throw new IOException("destination gone"));

        await epoch.Completion.WaitAsync(Patience);

        Assert.False(epochs.TryGet("t:Data", epoch.Id, out _));
    }

    private static Task WaitForCancellation(Stream _, CancellationToken token) => Task.Delay(Timeout.Infinite, token);

    private static UploadEpoch Started(Peer peer, TimeSpan? holdBound = null)
    {
        var epoch = new UploadEpoch(Guid.NewGuid(), "t:Data", null, TimeSpan.FromSeconds(15), TimeProvider.System, NullLogger.Instance, holdBound);
        epoch.Start(peer.CopyAsync, () => { });
        return epoch;
    }

    private static Task<SegmentOutcome> AcceptAsync(UploadEpoch epoch, long offset, string body, bool final = false) =>
        epoch.AcceptAsync(offset, new MemoryStream(Encoding.ASCII.GetBytes(body)), final, CancellationToken.None).WaitAsync(Patience);

    private static async Task UntilAsync(Func<bool> condition)
    {
        var deadline = DateTime.UtcNow + Patience;
        while (!condition())
        {
            Assert.True(DateTime.UtcNow < deadline, "The condition was not reached in time.");
            await Task.Delay(10);
        }
    }

    /// <summary>
    /// What a bridge does with the stream: read, write on, read again, until the end. A read comes
    /// back only once the previous one was written on, which is what the epoch counts as relayed.
    /// </summary>
    private sealed class Peer(int readSize = 256 * 1024)
    {
        private readonly MemoryStream _received = new();

        /// <summary>Waited for before each write: closed, the destination is full.</summary>
        public TaskCompletionSource? Gate { get; init; }

        /// <summary>Waited for once the stream has ended, as a bridge retires the transfer.</summary>
        public Task AfterTheEnd { get; init; } = Task.CompletedTask;

        public volatile bool SawTheEnd;

        public string Received
        {
            get { lock (_received) return Encoding.ASCII.GetString(_received.ToArray()); }
        }

        public async Task CopyAsync(Stream input, CancellationToken token)
        {
            var buffer = new byte[readSize];
            int read;
            while ((read = await input.ReadAsync(buffer, token)) > 0)
            {
                if (Gate != null) await Gate.Task.WaitAsync(token);
                lock (_received) _received.Write(buffer, 0, read);
            }

            SawTheEnd = true;
            await AfterTheEnd.WaitAsync(token);
        }
    }
}
