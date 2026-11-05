using System.Buffers;
using Microsoft.Extensions.Logging;

namespace TransportR.Hub.Streams;

/// <summary>What became of a segment, which is what its answer says.</summary>
internal enum SegmentStatus
{
    /// <summary>All of it reached the peer; for the last segment, the stream has ended as well.</summary>
    Relayed,

    /// <summary>Answered at the hold bound with part of it relayed, or none: the client sends the rest again.</summary>
    Held,

    /// <summary>The epoch is over, or the segment does not follow on from it: only a new epoch goes on.</summary>
    Conflict,
}

/// <param name="Relayed">How many bytes of the epoch's stream had reached the peer when the segment was answered.</param>
internal readonly record struct SegmentOutcome(SegmentStatus Status, long Relayed);

/// <summary>
/// One opening of a write stream, sent as a succession of segments. Presents them, in the order of
/// their offsets, as the stream a bridge copies from, and answers each with how much of that stream
/// has reached the peer.
/// </summary>
/// <remarks>
/// <para>
/// It keeps no byte past the request that brought it. A segment is answered once what it carried has
/// been written to the peer, or at the hold bound with what had been, and the client sends the rest
/// again: a hub that kept bytes to answer a resend would store and forward. What it does keep, per
/// epoch, is two counters — what the bridge took and what it wrote on.
/// </para>
/// <para>
/// Segments are taken one at a time, and a segment beyond what the bridge took is refused: there is no
/// reordering. A segment that starts before is relayed from where the bridge stands.
/// </para>
/// </remarks>
internal sealed class UploadEpoch
{
    /// <summary>
    /// How long a segment waits for the peer before it is answered with what got through. Below what
    /// a front end with default settings allows a request that has no answer, 60 s.
    /// </summary>
    public static readonly TimeSpan DefaultHoldBound = TimeSpan.FromSeconds(30);

    private const int BodyReadSize = 64 * 1024;

    private readonly object _gate = new();
    private readonly SemaphoreSlim _serial = new(1, 1);
    private readonly CancellationTokenSource _cts = new();
    private readonly string _bridgeKey;
    private readonly TimeSpan _holdBound;
    private readonly TimeSpan _silenceLimit;
    private readonly TimeProvider _time;
    private readonly ILogger _logger;

    // Replaced on every change, which wakes whoever waits on the old one to look again.
    private TaskCompletionSource _changed = NewSignal();

    private ReadOnlyMemory<byte> _offered;
    private long _taken;
    private long _relayed;
    private bool _endOffered;
    private bool _endTaken;
    private bool _over;
    private int _segmentsUnderway;
    private long _lastHeard;
    private Task _run = Task.CompletedTask;

    public UploadEpoch(Guid id, string bridgeKey, string? subject, TimeSpan heartbeatInterval, TimeProvider time, ILogger logger, TimeSpan? holdBound = null)
    {
        Id = id;
        Subject = subject;
        _bridgeKey = bridgeKey;
        _silenceLimit = heartbeatInterval * 2;
        _holdBound = holdBound ?? DefaultHoldBound;
        _time = time;
        _logger = logger;
        _lastHeard = time.GetTimestamp();
        Input = new EpochInput(this);
    }

    public Guid Id { get; }

    /// <summary>The identity attested by the request that opened the epoch; null in dev mode.</summary>
    public string? Subject { get; }

    /// <summary>The stream the segments make up, for the bridge to copy from. One reader.</summary>
    public Stream Input { get; }

    /// <summary>Cancelled when the epoch is replaced, falls silent, or its stream is over.</summary>
    public CancellationToken Token => _cts.Token;

    /// <summary>Ends when the bridge is done with <see cref="Input"/>, however that went.</summary>
    public Task Completion => _run;

    /// <summary>
    /// Hands <see cref="Input"/> to <paramref name="handle"/>, which must not throw, and starts judging
    /// the epoch's silence: no segment for two heartbeat intervals is a stream that was cut, the rule
    /// of the presence heartbeat.
    /// </summary>
    public void Start(Func<Stream, CancellationToken, Task> handle, Action onOver)
    {
        _run = RunAsync(handle, onOver);
        _ = WatchSilenceAsync();
    }

    /// <summary>Ends the epoch: a newer one of the same stream took its place.</summary>
    public void Abandon() => _cts.Cancel();

    /// <summary>
    /// Takes one segment: <paramref name="body"/> holds the bytes of the stream from
    /// <paramref name="offset"/> on. Returns once they are relayed, the epoch is over, or the hold bound
    /// has passed. A last segment returns once the stream it ends is closed.
    /// </summary>
    /// <exception cref="OperationCanceledException">The request went away.</exception>
    public async Task<SegmentOutcome> AcceptAsync(long offset, Stream body, bool final, CancellationToken requestAborted)
    {
        Heard(+1);
        try
        {
            await _serial.WaitAsync(requestAborted).ConfigureAwait(false);
            try
            {
                return await TakeAsync(offset, body, final, requestAborted).ConfigureAwait(false);
            }
            finally
            {
                _serial.Release();
            }
        }
        finally
        {
            Heard(-1);
        }
    }

    private async Task<SegmentOutcome> TakeAsync(long offset, Stream body, bool final, CancellationToken requestAborted)
    {
        long skip;
        lock (_gate)
        {
            if (_over || offset > _taken) return Outcome(SegmentStatus.Conflict);
            skip = _taken - offset;
        }

        using var hold = new CancellationTokenSource(_holdBound, _time);
        using var wait = CancellationTokenSource.CreateLinkedTokenSource(hold.Token, requestAborted);
        var buffer = ArrayPool<byte>.Shared.Rent(BodyReadSize);
        var ahead = final ? ArrayPool<byte>.Shared.Rent(BodyReadSize) : null;
        var end = offset;
        try
        {
            var read = await body.ReadAsync(buffer, wait.Token).ConfigureAwait(false);
            while (read > 0)
            {
                // A last segment reads a chunk ahead, so that the end goes to the bridge with the last
                // bytes rather than after them. Apart, the end waits on this request being scheduled
                // again, while the peer, which has the last bytes, may already be closing its side.
                var next = ahead != null ? await body.ReadAsync(ahead, wait.Token).ConfigureAwait(false) : 0;
                var already = (int)Math.Min(skip, read);
                skip -= already;
                end += read;
                if (already < read && !await OfferAsync(buffer.AsMemory(already, read - already), endsTheStream: final && next == 0, wait.Token).ConfigureAwait(false))
                {
                    return Outcome(SegmentStatus.Conflict);
                }

                if (ahead != null)
                {
                    (buffer, ahead) = (ahead, buffer);
                    read = next;
                }
                else
                {
                    read = await body.ReadAsync(buffer, wait.Token).ConfigureAwait(false);
                }
            }

            if (!final)
            {
                await WaitUntilAsync(() => _relayed >= end || _over, wait.Token).ConfigureAwait(false);
                return Outcome(Relayed(end) ? SegmentStatus.Relayed : SegmentStatus.Conflict);
            }

            // Already offered with the last bytes, unless the segment had none left to relay.
            lock (_gate)
            {
                _endOffered = true;
                Pulse();
            }

            await WaitUntilAsync(() => _endTaken || _over, wait.Token).ConfigureAwait(false);
            if (!EndTaken()) return Outcome(SegmentStatus.Conflict);

            // Past its end the stream waits on no peer: what is left is the bridge closing it, which the
            // client's release waits for as the end of its write stream.
            await _run.WaitAsync(requestAborted).ConfigureAwait(false);
            return Outcome(SegmentStatus.Relayed);
        }
        catch (OperationCanceledException) when (hold.IsCancellationRequested && !requestAborted.IsCancellationRequested)
        {
            return Outcome(SegmentStatus.Held);
        }
        finally
        {
            // Withdrawn under the gate the reader copies under, so nothing reads the buffer once it is
            // back in the pool. An end the reader has not taken goes too: the resend brings it again.
            lock (_gate)
            {
                _offered = default;
                if (!_endTaken) _endOffered = false;
            }

            ArrayPool<byte>.Shared.Return(buffer);
            if (ahead != null) ArrayPool<byte>.Shared.Return(ahead);
        }
    }

    private async Task<bool> OfferAsync(ReadOnlyMemory<byte> bytes, bool endsTheStream, CancellationToken token)
    {
        lock (_gate)
        {
            _offered = bytes;
            _endOffered |= endsTheStream;
            Pulse();
        }

        await WaitUntilAsync(() => _offered.IsEmpty || _over, token).ConfigureAwait(false);
        lock (_gate) return _offered.IsEmpty;
    }

    private async ValueTask<int> ReadAsync(Memory<byte> destination, CancellationToken cancellationToken)
    {
        while (true)
        {
            Task changed;
            lock (_gate)
            {
                // The bridge reads again only once it has written and flushed what the previous read
                // returned, so whatever it took before this call has reached the peer.
                if (_relayed != _taken)
                {
                    _relayed = _taken;
                    Pulse();
                }

                if (!_offered.IsEmpty)
                {
                    var count = Math.Min(destination.Length, _offered.Length);
                    _offered.Span[..count].CopyTo(destination.Span);
                    _offered = _offered[count..];
                    _taken += count;
                    Pulse();
                    return count;
                }

                if (_endOffered)
                {
                    _endTaken = true;
                    Pulse();
                    return 0;
                }

                changed = _changed.Task;
            }

            await changed.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task RunAsync(Func<Stream, CancellationToken, Task> handle, Action onOver)
    {
        try
        {
            await handle(Input, _cts.Token).ConfigureAwait(false);
        }
        finally
        {
            lock (_gate)
            {
                _over = true;
                Pulse();
            }

            _cts.Cancel();
            onOver();
        }
    }

    private async Task WatchSilenceAsync()
    {
        try
        {
            while (true)
            {
                TimeSpan wait;
                lock (_gate)
                {
                    var silentFor = _time.GetElapsedTime(_lastHeard);
                    if (_segmentsUnderway == 0 && silentFor >= _silenceLimit) break;

                    // A segment under way is not silence; its end sets the clock again.
                    wait = _segmentsUnderway > 0 ? _silenceLimit : _silenceLimit - silentFor;
                }

                await Task.Delay(wait, _time, _cts.Token).ConfigureAwait(false);
            }

            _logger.LogWarning("BRIDGE: No segment of {BridgeKey} for {Seconds:0.#}s: its stream is taken as cut", _bridgeKey, _silenceLimit.TotalSeconds);
            _cts.Cancel();
        }
        catch (OperationCanceledException)
        {
        }
    }

    private void Heard(int underway)
    {
        lock (_gate)
        {
            _segmentsUnderway += underway;
            _lastHeard = _time.GetTimestamp();
        }
    }

    private bool Relayed(long end)
    {
        lock (_gate) return _relayed >= end;
    }

    private bool EndTaken()
    {
        lock (_gate) return _endTaken;
    }

    private SegmentOutcome Outcome(SegmentStatus status)
    {
        lock (_gate) return new SegmentOutcome(status, _relayed);
    }

    private async Task WaitUntilAsync(Func<bool> done, CancellationToken token)
    {
        while (true)
        {
            Task changed;
            lock (_gate)
            {
                if (done()) return;
                changed = _changed.Task;
            }

            await changed.WaitAsync(token).ConfigureAwait(false);
        }
    }

    private void Pulse()
    {
        var previous = _changed;
        _changed = NewSignal();
        previous.TrySetResult();
    }

    // Signalled with the gate held: run inline, a waiter's continuation would execute inside it.
    private static TaskCompletionSource NewSignal() => new(TaskCreationOptions.RunContinuationsAsynchronously);

    private sealed class EpochInput(UploadEpoch epoch) : Stream
    {
        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default) =>
            epoch.ReadAsync(buffer, cancellationToken);

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
            epoch.ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();

        // Segments arrive on requests of their own: a blocking read would hold a thread for as long as
        // the next one takes to come.
        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();

        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();
        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        public override void Flush() { }
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }
}
