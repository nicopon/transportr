using System.Net.WebSockets;
using Microsoft.Extensions.Logging;
using TransportR.Protocol;

namespace TransportR.Hub.Streams;

/// <summary>
/// One opening of a write stream carried by a WebSocket: answers its probe, presents its binary messages
/// as the stream a bridge copies from, one reader, and says how it ended with a <c>Close</c>.
/// </summary>
/// <remarks>
/// Nothing cancels the socket's own read. A token on it would kill the socket, and a stream the hub gives
/// up on still has to say so: a caller's cancellation leaves the read pending, in a buffer of this class's
/// since the caller's may be back in a pool by then, for <see cref="EndAsync"/> to go on with.
/// </remarks>
internal sealed class WebSocketInput : Stream
{
    // The size of the bridge's copy buffer: a read here hands it one frame's worth at a time.
    private const int BufferSize = 256 * 1024;

    // Longer than the client gives the probe, so that the client is the one to decide about the path.
    private static readonly TimeSpan ProbeBound = TimeSpan.FromSeconds(10);

    // Bounds a Close and, after a refusal, the wait for the client's: unread bytes on a socket that
    // closes turn into a reset, which can take the refusal with it.
    private static readonly TimeSpan CloseBound = TimeSpan.FromSeconds(2);

    private const long NotReading = long.MinValue;

    private readonly WebSocket _socket;
    private readonly string _bridgeKey;
    private readonly TimeSpan? _silenceLimit;
    private readonly TimeProvider _time;
    private readonly ILogger _logger;
    private readonly byte[] _buffer = new byte[BufferSize];
    private readonly CancellationTokenSource _stopWatching = new();

    private ReadOnlyMemory<byte> _unread;
    private Task<ValueWebSocketReceiveResult>? _pending;
    private long _readingSince = NotReading;

    // The client's Close has been read, which the bridge may not have reached yet.
    private bool _endRead;

    /// <param name="heartbeatInterval">
    /// How often the client writes when it has nothing to say, for a stream whose silence is judged; null
    /// for one whose silence says nothing, which is never ended for it.
    /// </param>
    public WebSocketInput(WebSocket socket, string bridgeKey, TimeSpan? heartbeatInterval, TimeProvider time, ILogger logger)
    {
        _socket = socket;
        _bridgeKey = bridgeKey;
        _silenceLimit = heartbeatInterval * 2;
        _time = time;
        _logger = logger;
    }

    /// <summary>The bridge has read the stream to the end the client gave it.</summary>
    public bool EndTaken { get; private set; }

    /// <summary>
    /// Answers the client's probe, a text message, with one. The answer waits on nothing but this hop, so
    /// it says nothing of the peer. Returns false when no probe came, and the socket is of no further use.
    /// </summary>
    public async Task<bool> AnswerProbeAsync(CancellationToken requestAborted)
    {
        using var bound = CancellationTokenSource.CreateLinkedTokenSource(requestAborted);
        bound.CancelAfter(ProbeBound);
        try
        {
            var probe = await _socket.ReceiveAsync(_buffer.AsMemory(), bound.Token).ConfigureAwait(false);
            if (probe.MessageType != WebSocketMessageType.Text || !probe.EndOfMessage)
            {
                _logger.LogWarning("BRIDGE: The WebSocket of {BridgeKey} did not open with a probe", _bridgeKey);
                return false;
            }

            await _socket.SendAsync(ReadOnlyMemory<byte>.Empty, WebSocketMessageType.Text, endOfMessage: true, bound.Token).ConfigureAwait(false);
            return true;
        }
        catch (Exception ex) when (ex is WebSocketException or OperationCanceledException or IOException)
        {
            _logger.LogInformation("BRIDGE: The WebSocket of {BridgeKey} went away before its probe was answered ({Message})", _bridgeKey, ex.Message);
            return false;
        }
    }

    /// <summary>
    /// Starts judging the stream's silence, when it has a heartbeat interval: a read left waiting for two
    /// of them with no byte is a stream that was cut, the rule of the presence heartbeat. A hub parked on
    /// the peer has no read pending, so a slow peer never reads as a silent client.
    /// </summary>
    public void JudgeSilence()
    {
        if (_silenceLimit is { } limit)
        {
            _ = WatchSilenceAsync(limit);
        }
    }

    /// <summary>
    /// Waits for the stream to say something, or to end. A stream takes its place on a bridge only then,
    /// as an epoch does with its first segment: a peer that reconnects cuts whatever source the bridge
    /// holds, and cut in its turn, a side with nothing to say yet would cut the peer again, for ever.
    /// Returns false when the socket went away first.
    /// </summary>
    public async Task<bool> WaitForFirstByteAsync(CancellationToken requestAborted)
    {
        try
        {
            await FillAsync(requestAborted).ConfigureAwait(false);
            return true;
        }
        catch (Exception ex) when (ex is IOException or OperationCanceledException)
        {
            _logger.LogInformation("BRIDGE: The WebSocket of {BridgeKey} went away before it carried anything ({Message})", _bridgeKey, ex.Message);
            return false;
        }
    }

    public override async ValueTask<int> ReadAsync(Memory<byte> destination, CancellationToken cancellationToken = default)
    {
        await FillAsync(cancellationToken).ConfigureAwait(false);
        if (_unread.IsEmpty)
        {
            EndTaken = true;
            return 0;
        }

        var count = Math.Min(destination.Length, _unread.Length);
        _unread.Span[..count].CopyTo(destination.Span);
        _unread = _unread[count..];
        return count;
    }

    public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
        ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();

    // Leaves bytes in _unread, unless the stream has ended.
    private async Task FillAsync(CancellationToken cancellationToken)
    {
        while (_unread.IsEmpty && !_endRead)
        {
            var received = await ReceiveAsync(cancellationToken).ConfigureAwait(false);
            switch (received.MessageType)
            {
                case WebSocketMessageType.Binary:
                    _unread = _buffer.AsMemory(0, received.Count);
                    break;

                case WebSocketMessageType.Close when _socket.CloseStatus == (WebSocketCloseStatus)DataPlane.EndOfStreamCloseStatus:
                    _endRead = true;
                    break;

                case WebSocketMessageType.Close:
                    throw new IOException($"The WebSocket closed with status {(int?)_socket.CloseStatus}, which does not end the stream.");

                default:
                    throw new IOException("The WebSocket carried a message that is not part of the stream.");
            }
        }
    }

    /// <summary>
    /// Says how the stream ended, once the bridge is done with it. A stream the bridge read to its end is
    /// answered at once, which tells the client it is closed. Any other is refused when the client next
    /// writes, as a segment would be: told any sooner, a client whose peer reconnected would reconnect in
    /// turn and cut that peer, and the two would go on taking turns. Returns once that is said, or the
    /// socket has gone, and never throws: a socket that has gone has nobody left to tell.
    /// </summary>
    public async Task EndAsync(CancellationToken requestAborted)
    {
        _stopWatching.Cancel();
        try
        {
            if (!EndTaken)
            {
                await FillAsync(requestAborted).ConfigureAwait(false);
            }

            using var bound = new CancellationTokenSource(CloseBound, _time);
            var status = EndTaken ? DataPlane.EndOfStreamCloseStatus : DataPlane.StreamOverCloseStatus;
            await _socket.CloseOutputAsync((WebSocketCloseStatus)status, null, bound.Token).ConfigureAwait(false);
            while (_socket.State == WebSocketState.CloseSent)
            {
                await ReceiveAsync(bound.Token).ConfigureAwait(false);
            }
        }
        catch (Exception ex) when (ex is WebSocketException or IOException or OperationCanceledException or ObjectDisposedException)
        {
            _logger.LogDebug("BRIDGE: The WebSocket of {BridgeKey} ended without closing ({Message})", _bridgeKey, ex.Message);
        }
    }

    private async Task<ValueWebSocketReceiveResult> ReceiveAsync(CancellationToken cancellationToken)
    {
        if (_pending == null)
        {
            Volatile.Write(ref _readingSince, _time.GetTimestamp());
            _pending = _socket.ReceiveAsync(_buffer.AsMemory(), CancellationToken.None).AsTask();
        }

        try
        {
            var received = await _pending.WaitAsync(cancellationToken).ConfigureAwait(false);
            _pending = null;
            Volatile.Write(ref _readingSince, NotReading);
            return received;
        }
        catch (Exception ex) when (ex is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
        {
            _pending = null;
            Volatile.Write(ref _readingSince, NotReading);
            throw new IOException("The WebSocket carrying the stream failed.", ex);
        }
    }

    private async Task WatchSilenceAsync(TimeSpan limit)
    {
        var stop = _stopWatching.Token;
        try
        {
            while (true)
            {
                var since = Volatile.Read(ref _readingSince);
                var silentFor = since == NotReading ? TimeSpan.Zero : _time.GetElapsedTime(since);
                if (silentFor >= limit) break;

                await Task.Delay(limit - silentFor, _time, stop).ConfigureAwait(false);
            }

            _logger.LogWarning("BRIDGE: No byte from {BridgeKey} for {Seconds:0.#}s: its stream is taken as cut", _bridgeKey, limit.TotalSeconds);
            _socket.Abort();
        }
        catch (OperationCanceledException)
        {
        }
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _stopWatching.Cancel();
            _stopWatching.Dispose();

            // The socket goes right after this and takes a pending read down with it, with nobody left to look.
            _pending?.ContinueWith(static read => _ = read.Exception, TaskContinuationOptions.OnlyOnFaulted);
        }

        base.Dispose(disposing);
    }

    // Messages arrive whenever the client sends them: a blocking read would hold a thread that long.
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
