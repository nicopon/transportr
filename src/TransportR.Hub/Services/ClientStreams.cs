using System.Collections.Concurrent;
using System.Net.ServerSentEvents;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Threading.Channels;
using TransportR.Protocol;

namespace TransportR.Hub.Services;

/// <summary>
/// The event streams this hub instance holds open, one per registered client, and the only way the
/// hub reaches a client: an event written to its stream and, for an event that asks, the reply the
/// client posts back.
/// </summary>
/// <remarks>
/// Local to the instance. A client whose stream another instance holds cannot be reached from here,
/// which is why a sender is redirected to its receiver's instance, and why a connection whose token
/// ran out is closed by the instance that holds it.
/// </remarks>
internal sealed class ClientStreams
{
    private readonly ConcurrentDictionary<string, ClientStream> _streams = new();
    private readonly ConcurrentDictionary<Guid, PendingReply> _pending = new();
    private readonly TimeProvider _time;

    public ClientStreams(TimeProvider timeProvider)
    {
        _time = timeProvider;
    }

    /// <param name="heartbeatInterval">
    /// What the client announced: the stream ends once nothing has been heard from the client for two
    /// of them. A cut that closes nothing leaves no other trace on this side, since writing to such a
    /// stream does not fail.
    /// </param>
    internal ClientStream Open(Guid clientId, TimeSpan heartbeatInterval)
    {
        var stream = new ClientStream(Guid.NewGuid().ToString("N"), clientId, heartbeatInterval, _time);
        _streams[stream.ConnectionId] = stream;
        return stream;
    }

    /// <summary>
    /// Forgets the stream, and fails every reply still awaited from it, unless the client holds another
    /// stream on this instance: the reply is then awaited from that one.
    /// </summary>
    internal void Remove(ClientStream stream)
    {
        _streams.TryRemove(new KeyValuePair<string, ClientStream>(stream.ConnectionId, stream));
        stream.Close();

        // A client replacing its stream opens the new one before it closes the old, and posts its reply
        // whichever stream carried the question: a reply names the client and the request, not a stream.
        var successor = _streams.Values.FirstOrDefault(s => s.ClientId == stream.ClientId);

        foreach (var (requestId, pending) in _pending)
        {
            if (!ReferenceEquals(pending.Stream, stream)) continue;

            if (successor != null)
            {
                _pending.TryUpdate(requestId, pending with { Stream = successor }, pending);
            }
            else if (_pending.TryRemove(requestId, out _))
            {
                pending.Answer.TrySetException(new IOException("The client's stream closed before it answered."));
            }
        }
    }

    internal bool Holds(string connectionId) => _streams.ContainsKey(connectionId);

    /// <returns>True when this instance holds the stream and has closed it.</returns>
    internal bool TryClose(string connectionId)
    {
        if (!_streams.TryGetValue(connectionId, out var stream)) return false;

        stream.Close();
        return true;
    }

    /// <returns>False when this instance holds no such stream, or it has closed.</returns>
    internal bool TrySend(string connectionId, string eventType, object data) =>
        _streams.TryGetValue(connectionId, out var stream) && stream.TryWrite(eventType, data);

    /// <summary>
    /// Writes an event that asks the client something, and waits for the reply it posts to
    /// <see cref="TryReply"/>. The event carries the id of the request, made here.
    /// </summary>
    /// <exception cref="IOException">This instance holds no such stream, or it closed first.</exception>
    internal async Task<CallbackOutcome> AskAsync(string connectionId, string eventType, Func<Guid, object> data, CancellationToken cancellationToken)
    {
        if (!_streams.TryGetValue(connectionId, out var stream))
        {
            throw new IOException("This hub instance holds no stream for the connection.");
        }

        var requestId = Guid.NewGuid();
        var pending = new PendingReply(stream, new TaskCompletionSource<CallbackOutcome>(TaskCreationOptions.RunContinuationsAsynchronously));
        _pending[requestId] = pending;
        try
        {
            if (!stream.TryWrite(eventType, data(requestId)))
            {
                throw new IOException("The client's stream closed before the event was written.");
            }

            return await pending.Answer.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _pending.TryRemove(requestId, out _);
        }
    }

    /// <returns>False when nothing awaits that request from that client any more.</returns>
    internal bool TryReply(Guid clientId, Guid requestId, CallbackOutcome outcome)
    {
        if (!_pending.TryGetValue(requestId, out var pending) || pending.Stream.ClientId != clientId) return false;

        return _pending.TryRemove(requestId, out _) && pending.Answer.TrySetResult(outcome);
    }

    /// <summary>Records that the client was heard from, which keeps its streams on this instance open.</summary>
    internal void Touch(Guid clientId)
    {
        foreach (var stream in _streams.Values)
        {
            if (stream.ClientId == clientId) stream.Touch();
        }
    }

    private sealed record PendingReply(ClientStream Stream, TaskCompletionSource<CallbackOutcome> Answer);
}

/// <summary>One client's stream of events, as long as it is open.</summary>
internal sealed class ClientStream
{
    /// <summary>
    /// How long the stream may carry nothing before a keepalive is written: under the idle timeout of
    /// an ordinary load balancer, and half of what the client waits before taking its stream as cut.
    /// </summary>
    internal static readonly TimeSpan KeepAliveInterval = TimeSpan.FromSeconds(15);

    private readonly Channel<SseItem<string>> _events = Channel.CreateUnbounded<SseItem<string>>(new UnboundedChannelOptions { SingleReader = true });
    private readonly CancellationTokenSource _closed = new();
    private readonly TimeSpan _silenceLimit;
    private readonly TimeProvider _time;
    private long _lastHeard;

    internal ClientStream(string connectionId, Guid clientId, TimeSpan heartbeatInterval, TimeProvider time)
    {
        ConnectionId = connectionId;
        ClientId = clientId;
        _silenceLimit = heartbeatInterval * 2;
        _time = time;
        Touch();
    }

    public string ConnectionId { get; }

    public Guid ClientId { get; }

    /// <summary>Why the stream ended on the hub's side, once it has: null when the client left.</summary>
    public string? EndReason { get; private set; }

    internal void Touch() => Interlocked.Exchange(ref _lastHeard, _time.GetUtcNow().UtcTicks);

    internal bool TryWrite(string eventType, object data) =>
        !_closed.IsCancellationRequested && _events.Writer.TryWrite(Item(eventType, data));

    internal void Close()
    {
        EndReason ??= "closed by the hub";
        try { _closed.Cancel(); } catch (ObjectDisposedException) { }
    }

    /// <summary>
    /// The stream's events, the registration first, then what is written to it and a keepalive when
    /// nothing is, until the client leaves (<paramref name="cancellationToken"/>), the hub closes it, or
    /// the client falls silent.
    /// </summary>
    internal async IAsyncEnumerable<SseItem<string>> ReadAllAsync([EnumeratorCancellation] CancellationToken cancellationToken)
    {
        using var ended = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _closed.Token);

        yield return Item(ControlPlane.Registered, new RegisteredEvent(ConnectionId));

        var lastWritten = _time.GetUtcNow();
        while (await NextAsync(lastWritten, ended.Token).ConfigureAwait(false) is { } item)
        {
            lastWritten = _time.GetUtcNow();
            yield return item;
        }
    }

    private async Task<SseItem<string>?> NextAsync(DateTimeOffset lastWritten, CancellationToken ended)
    {
        while (true)
        {
            if (_events.Reader.TryRead(out var queued)) return queued;

            var now = _time.GetUtcNow();
            var silentUntil = new DateTimeOffset(Interlocked.Read(ref _lastHeard), TimeSpan.Zero) + _silenceLimit;
            if (now >= silentUntil)
            {
                EndReason ??= $"nothing heard from the client for {_silenceLimit.TotalSeconds:0.#}s";
                return null;
            }

            var keepAliveAt = lastWritten + KeepAliveInterval;
            if (now >= keepAliveAt) return Item(ControlPlane.KeepAlive, string.Empty);

            using var wake = new CancellationTokenSource(Min(keepAliveAt, silentUntil) - now, _time);
            using var either = CancellationTokenSource.CreateLinkedTokenSource(ended, wake.Token);
            try
            {
                await _events.Reader.WaitToReadAsync(either.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (ended.IsCancellationRequested)
            {
                return null;
            }
            catch (OperationCanceledException)
            {
                // Woken to write a keepalive or to judge the silence, both decided at the top.
            }
        }
    }

    private static DateTimeOffset Min(DateTimeOffset a, DateTimeOffset b) => a < b ? a : b;

    private static SseItem<string> Item(string eventType, object data) =>
        new(data as string ?? JsonSerializer.Serialize(data, ControlPlane.Json), eventType);
}
