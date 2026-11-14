using System.Net;
using System.Net.Http.Headers;
using System.Net.WebSockets;
using Microsoft.Extensions.Logging;
using TransportR.Client.Internal;
using TransportR.Client.Protocol;
using TransportR.Protocol;

namespace TransportR.Client.Sessions;

/// <summary>
/// How a stream attempt ended, which is what the session picks its next state on.
/// </summary>
internal enum StreamOutcome
{
    /// <summary>The request ran to its end, or the read side stopped reading.</summary>
    Ended,

    /// <summary>The attempt was cancelled, by the session or by its other stream failing.</summary>
    Cancelled,

    /// <summary>The hub does not know the transfer (404 or 410).</summary>
    Gone,

    /// <summary>The connection failed, and a new attempt may get past it.</summary>
    Retryable,

    /// <summary>A new attempt would fail the same way.</summary>
    Fatal,
}

/// <summary>
/// Manages the underlying HTTP streams for a data session.
/// Handles connection establishment and the single token-refresh retry; a stream that fails
/// for any other reason is handed back to the session, which decides whether to reconnect.
/// </summary>
internal class HttpStreamManager : IAsyncDisposable
{
    private readonly IStreamTransport _transport;
    private readonly ISessionConfig _config;
    private readonly string _transferId;
    private readonly string _dataUrl;
    private readonly string? _affinity;

    /// <param name="Refused">The hub answered the stream with a refusal, rather than the connection failing under it.</param>
    public readonly record struct StreamResult(StreamOutcome Outcome, string? ErrorMessage = null, bool Refused = false)
    {
        public bool IsSuccess => Outcome is StreamOutcome.Ended or StreamOutcome.Cancelled;
    }

    /// <summary>
    /// Bounds a wait for the hub to end a read stream whose transfer may be over: after the write side
    /// is refused, and when a completed receiver is released. The hub ends the stream once the transfer
    /// is over, but one whose peer died, or that is not over after all, leaves it open.
    /// </summary>
    internal static readonly TimeSpan ReadSideEndTimeout = TimeSpan.FromSeconds(2);

    // A path that cuts a proven WebSocket, on the size of a message for one, would otherwise have the
    // session reconnect for ever where segments pass. A connection that lives this long has carried
    // something, and says nothing of the kind.
    private const int ShortLivedRupturesUntilFallback = 3;
    private static readonly TimeSpan ShortLived = TimeSpan.FromSeconds(30);

    private CancellationTokenSource _streamCts;
    private int _shortLivedRuptures;
    private volatile string? _webSocketUnavailable;
    private WriteTransport? _writeTransport;
    private Task<StreamResult>? _senderTask;
    private Task<StreamResult>? _receiverTask;

    // No more Events - passive component
    public Task<StreamResult>? SenderTask => _senderTask;
    public Task<StreamResult>? ReceiverTask => _receiverTask;

    /// <summary>What carries the write stream of the current attempt; null before the first opens.</summary>
    public WriteTransport? WriteTransport => _writeTransport;

    /// <summary>Why this session does not use a WebSocket, once it has given up on one.</summary>
    public string? WriteTransportFallback => _webSocketUnavailable;

    /// <summary>
    /// The current attempt's task for whichever direction this side writes on (Data for a sender,
    /// Control for a receiver). Unlike <see cref="SenderTask"/>/<see cref="ReceiverTask"/>, which
    /// only resolve once <b>both</b> directions finish (or one fails), this resolves as soon as the
    /// hub answers the end of the write side, which it does once it has closed the stream - what a
    /// caller waiting to know a specific message actually reached the server needs, since the read
    /// side has no reason to end on its own.
    /// </summary>
    public Task<StreamResult>? WriteTask { get; private set; }

    /// <summary>
    /// The current attempt's task for whichever direction this side reads on (Control for a sender,
    /// Data for a receiver), which resolves when the read loop returns. A receiver in its closing
    /// phase reads its data GET to the end, so for it this is the hub ending that request.
    /// </summary>
    public Task<StreamResult>? ReadTask { get; private set; }

    /// <param name="affinity">The cookies of the hub instance holding the transfer's bridge, on every stream.</param>
    public HttpStreamManager(IStreamTransport transport, ISessionConfig config, string transferId, string dataUrl, string? affinity)
    {
        _transport = transport;
        _config = config;
        _transferId = transferId;
        _dataUrl = dataUrl;
        _affinity = affinity;
        _streamCts = new CancellationTokenSource();
    }

    public void StartStreams(bool isSender, Func<Stream, CancellationToken, Task> writeAction, Func<Stream, CancellationToken, Task> readAction, CancellationToken sessionToken)
    {
        // The attempt being replaced is cancelled but not disposed: it owns its source now and
        // still has streams unwinding that will cancel it on their way out.
        _streamCts.Cancel();
        var attempt = CancellationTokenSource.CreateLinkedTokenSource(sessionToken);
        _streamCts = attempt;

        if (isSender)
        {
            // Sender: Data=Write, Control=Read
            _senderTask = RunDualStreamAsync(true, writeAction, readAction, attempt);
            _receiverTask = null;
        }
        else
        {
            // Receiver: Data=Read, Control=Write
            _receiverTask = RunDualStreamAsync(false, writeAction, readAction, attempt);
            _senderTask = null;
        }
    }

    /// <summary>
    /// Runs one attempt's pair of streams. The attempt owns <paramref name="attempt"/> for its
    /// whole life, including after the manager has moved on to the next one.
    /// </summary>
    private async Task<StreamResult> RunDualStreamAsync(bool isSender, Func<Stream, CancellationToken, Task> writeAction, Func<Stream, CancellationToken, Task> readAction, CancellationTokenSource attempt)
    {
        var token = attempt.Token;
        // Trigger: Data Stream
        // Trigger: Control Stream

        Task<StreamResult> tData;
        Task<StreamResult> tControl;
        Task<StreamResult> written;
        Task<StreamResult> read;

        if (isSender)
        {
            // Sender writes Data, reads Control
            tData = RunHttpStreamAsync(ConnectionType.Data, isWrite: true, writeAction, readAction, token);
            tControl = RunHttpStreamAsync(ConnectionType.Control, isWrite: false, writeAction, readAction, token);
            (written, read) = (tData, tControl);
        }
        else
        {
            // Receiver reads Data, writes Control
            tData = RunHttpStreamAsync(ConnectionType.Data, isWrite: false, writeAction, readAction, token);
            tControl = RunHttpStreamAsync(ConnectionType.Control, isWrite: true, writeAction, readAction, token);
            (written, read) = (tControl, tData);
        }
        WriteTask = written;
        ReadTask = read;

        var tasks = new List<Task<StreamResult>> { tData, tControl };

        while (tasks.Count > 0)
        {
            var completedTask = await Task.WhenAny(tasks);
            tasks.Remove(completedTask);

            var result = await completedTask;
            if (!result.IsSuccess)
            {
                // What the hub relayed to the read side before refusing the write side may still be on
                // its way: a receiver's acknowledgement, refused because the sender's Abort ended the
                // transfer, comes back ahead of that Abort. Cancelling the read side now loses it, and
                // the session fails on the refusal instead of the Abort's reason. A cut is not waited
                // on: nothing says the hub is done with the read side, and the reconnection has to be quick.
                if (result.Refused && completedTask == written)
                {
                    try
                    {
                        await read.WaitAsync(ReadSideEndTimeout);
                    }
                    catch (TimeoutException)
                    {
                        _config.Logger.LogDebug("The hub had not ended the read stream of Transfer={TransferId} after refusing its write stream", _transferId);
                    }
                }

                // Cancels this attempt's other stream, never whatever attempt the manager is on
                // now: an attempt that has already been replaced is still unwinding, and reaching
                // for the shared field made it cancel the streams that succeeded it — every
                // reopened stream then died within a fraction of a millisecond.
                attempt.Cancel();
                return result;
            }
        }

        return new StreamResult(StreamOutcome.Ended);
    }

    public void CancelStreams()
    {
        try { _streamCts.Cancel(); } catch (ObjectDisposedException) { }
    }

    public async ValueTask DisposeAsync()
    {
        CancelStreams();
        if (_senderTask != null) try { await _senderTask; } catch { }
        if (_receiverTask != null) try { await _receiverTask; } catch { }
        _streamCts.Dispose();
    }

    private Task<StreamResult> RunHttpStreamAsync(
        ConnectionType type,
        bool isWrite,
        Func<Stream, CancellationToken, Task> writeAction,
        Func<Stream, CancellationToken, Task> readAction,
        CancellationToken token) =>
        isWrite ? RunWriteStreamAsync(type, writeAction, token) : RunReadStreamAsync(type, readAction, token);

    /// <summary>
    /// Runs the write side as one opening of the stream, over what <see cref="OpenWriteStreamAsync"/>
    /// picks, ended once <paramref name="writeAction"/> returns. A refusal from the hub ends the attempt
    /// with what it means; a segment's own resends stay inside <see cref="SegmentedWriteStream"/>.
    /// </summary>
    private async Task<StreamResult> RunWriteStreamAsync(ConnectionType type, Func<Stream, CancellationToken, Task> writeAction, CancellationToken token)
    {
        WriteStream stream;
        try
        {
            stream = await OpenWriteStreamAsync(type, token);
        }
        catch (OperationCanceledException)
        {
            return new StreamResult(StreamOutcome.Cancelled);
        }
        catch (Exception ex)
        {
            return WriteStreamFailed(ex, type);
        }

        var openedAtMs = Environment.TickCount64;
        await using (stream)
        {
            using var writing = CancellationTokenSource.CreateLinkedTokenSource(token, stream.Lost);
            try
            {
                await writeAction(stream, writing.Token);
                await stream.CompleteAsync(writing.Token);
                return new StreamResult(StreamOutcome.Ended);
            }
            catch (OperationCanceledException) when (stream.Failure is null || token.IsCancellationRequested)
            {
                _config.Logger.LogDebug("WriteLoop closed ({Type}) - Transfer={TransferId}", type, _transferId);
                return new StreamResult(StreamOutcome.Cancelled);
            }
            catch (Exception ex)
            {
                var result = WriteStreamFailed(stream.Failure ?? ex, type);
                if (stream is WebSocketWriteStream && result is { Outcome: StreamOutcome.Retryable, Refused: false })
                {
                    CountRupture(openedAtMs);
                }

                return result;
            }
        }
    }

    private StreamResult WriteStreamFailed(Exception failure, ConnectionType type)
    {
        switch (failure)
        {
            case StreamRefusedException refused:
                return new StreamResult(refused.Outcome, refused.Message, Refused: true);

            case WireProtocolViolationException:
                _config.Logger.LogError("Protocol violation, session failed: {Message}", failure.Message);
                return new StreamResult(StreamOutcome.Fatal, failure.Message);

            default:
                _config.Logger.LogWarning("WriteLoop interrupted ({Type}) - Transfer={TransferId}: {Message}", type, _transferId, failure.Message);
                return new StreamResult(StreamOutcome.Retryable, failure.Message);
        }
    }

    private void CountRupture(long openedAtMs)
    {
        if (Environment.TickCount64 - openedAtMs >= ShortLived.TotalMilliseconds)
        {
            _shortLivedRuptures = 0;
        }
        else if (++_shortLivedRuptures >= ShortLivedRupturesUntilFallback)
        {
            GiveUpOnWebSocket($"{_shortLivedRuptures} connections in a row were cut within {ShortLived.TotalSeconds:0}s of opening");
        }
    }

    /// <summary>
    /// Opens the write stream over a WebSocket while the session has not given up on one, and over
    /// segments otherwise. Giving up lasts until the session ends: a transport that changes back and
    /// forth would have to pay for the test again at each reconnection.
    /// </summary>
    /// <exception cref="StreamRefusedException">The hub refused the stream, or a WebSocket is required and the path carries none.</exception>
    private async Task<WriteStream> OpenWriteStreamAsync(ConnectionType type, CancellationToken token)
    {
        var policy = _config.Options.WriteTransportPolicy;
        if (policy != WriteTransportPolicy.SegmentsOnly && _webSocketUnavailable == null
            && await TryOpenWebSocketAsync(type, token) is { } socket)
        {
            LogOpened(type, TransportR.WriteTransport.WebSocket);
            return new WebSocketWriteStream(socket, _config.Options.StreamBufferSize);
        }

        if (policy == WriteTransportPolicy.RequireWebSocket)
        {
            throw new StreamRefusedException(StreamOutcome.Fatal, "The data stream requires a WebSocket, and the path to the hub does not carry one.");
        }

        LogOpened(type, TransportR.WriteTransport.Segments);
        return new SegmentedWriteStream(
            _transport,
            _config.Logger,
            query => NewRequest(HttpMethod.Post, $"{_dataUrl}/{_transferId}/{WireRoutes.SendSegment}?{query}", type),
            _config.Options.StreamBufferSize,
            type == ConnectionType.Data ? _config.Options.PresenceHeartbeatInterval : null);
    }

    private void LogOpened(ConnectionType type, WriteTransport transport)
    {
        _writeTransport = transport;
        _config.Logger.LogInformation("Write stream ({Type}) of Transfer={TransferId} opened over {Transport}", type, _transferId, transport);
    }

    /// <summary>
    /// Opens a WebSocket and proves that the path carries it. Returns null once the session has given up
    /// on one: anything short of a proven socket that is not the hub's own verdict says the path does
    /// not carry one, and an actual outage then fails on the first segment and reconnects as usual.
    /// </summary>
    /// <exception cref="StreamRefusedException">The hub refused the stream.</exception>
    private async Task<WebSocket?> TryOpenWebSocketAsync(ConnectionType type, CancellationToken token)
    {
        var refreshed = false;
        while (true)
        {
            var accessToken = await _transport.GetAccessTokenAsync(refreshed);
            var socket = NewWebSocket(type, accessToken);
            using var bound = CancellationTokenSource.CreateLinkedTokenSource(token);
            bound.CancelAfter(_config.Options.WebSocketOpenBound);

            var proven = false;
            try
            {
                await socket.ConnectAsync(WebSocketUri(), _transport.HttpClient, bound.Token);
                await ProveAsync(socket, bound.Token);
                proven = true;
                return socket;
            }
            catch (OperationCanceledException) when (!token.IsCancellationRequested)
            {
                var seconds = _config.Options.WebSocketOpenBound.TotalSeconds;
                GiveUpOnWebSocket(socket.HttpStatusCode == HttpStatusCode.SwitchingProtocols
                    ? $"the path accepted the upgrade but did not carry its probe within {seconds:0.#}s"
                    : $"the upgrade was not answered within {seconds:0.#}s");
                return null;
            }
            catch (WebSocketException)
            {
                switch (socket.HttpStatusCode)
                {
                    case HttpStatusCode.Gone:
                    case HttpStatusCode.NotFound when CarriesRefusal(socket):
                        throw new StreamRefusedException(StreamOutcome.Gone, $"Transfer Gone ({socket.HttpStatusCode})");

                    case HttpStatusCode.Unauthorized when !refreshed:
                        _config.Logger.LogWarning("Stream Unauthorized (401). Refreshing token and retrying...");
                        refreshed = true;
                        continue;

                    case HttpStatusCode.Unauthorized:
                        throw new StreamRefusedException(StreamOutcome.Retryable, "Unauthorized (401) - Check credentials");

                    case HttpStatusCode.SwitchingProtocols:
                        GiveUpOnWebSocket("the path accepted the upgrade but did not carry its probe");
                        return null;

                    case 0:
                        GiveUpOnWebSocket("the connection failed before the upgrade was answered");
                        return null;

                    default:
                        GiveUpOnWebSocket($"the upgrade was answered {(int)socket.HttpStatusCode}");
                        return null;
                }
            }
            finally
            {
                if (!proven)
                {
                    socket.Dispose();
                }
            }
        }
    }

    private void GiveUpOnWebSocket(string reason)
    {
        _webSocketUnavailable = reason;
        _config.Logger.LogWarning("No WebSocket for the write stream of Transfer={TransferId}: {Reason}", _transferId, reason);
    }

    private static bool CarriesRefusal(ClientWebSocket socket) =>
        socket.HttpResponseHeaders?.Keys.Any(name => string.Equals(name, WireRoutes.RefusedHeader, StringComparison.OrdinalIgnoreCase)) == true;

    private static async Task ProveAsync(WebSocket socket, CancellationToken token)
    {
        await socket.SendAsync(ReadOnlyMemory<byte>.Empty, WebSocketMessageType.Text, endOfMessage: true, token);

        var answer = await socket.ReceiveAsync(new byte[1].AsMemory(), token);
        if (answer.MessageType != WebSocketMessageType.Text)
        {
            throw new WebSocketException(WebSocketError.InvalidMessageType, "The probe was not answered with a text message.");
        }
    }

    // No KeepAliveTimeout: it aborts a sound stream as soon as the hub stops reading, which is what a
    // hub parked on a slow peer does. The interval alone sends what a front end's idle timer needs.
    private ClientWebSocket NewWebSocket(ConnectionType type, string? accessToken)
    {
        var socket = new ClientWebSocket();
        var options = socket.Options;
        options.HttpVersion = HttpVersion.Version11;
        options.HttpVersionPolicy = HttpVersionPolicy.RequestVersionExact;
        options.CollectHttpResponseDetails = true;
        options.KeepAliveInterval = _config.Options.PresenceHeartbeatInterval;
        options.SetRequestHeader(DataPlane.ConnectionTypeHeader, type.ToString());

        if (!string.IsNullOrEmpty(_affinity))
        {
            options.SetRequestHeader("Cookie", _affinity);
        }

        if (!string.IsNullOrEmpty(accessToken))
        {
            options.SetRequestHeader("Authorization", $"Bearer {accessToken}");
        }

        return socket;
    }

    private Uri WebSocketUri()
    {
        var uri = new UriBuilder($"{_dataUrl}/{_transferId}/{WireRoutes.SendSegment}")
        {
            Query = $"{DataPlane.HeartbeatParameter}={(long)_config.Options.PresenceHeartbeatInterval.TotalMilliseconds}"
        };
        uri.Scheme = uri.Scheme == Uri.UriSchemeHttps ? "wss" : "ws";
        return uri.Uri;
    }

    private async Task<StreamResult> RunReadStreamAsync(ConnectionType type, Func<Stream, CancellationToken, Task> readAction, CancellationToken token)
    {
        bool forceRefresh = false;

        while (!token.IsCancellationRequested)
        {
            try
            {
                using var request = NewRequest(HttpMethod.Get, $"{_dataUrl}/{_transferId}/{WireRoutes.ReceiveSegment}", type);

                // Add Authorization Header
                var tokenStr = await _transport.GetAccessTokenAsync(forceRefresh);
                if (!string.IsNullOrEmpty(tokenStr))
                {
                    request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", tokenStr);
                }

                using var response = await _transport.HttpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, token);

                // 1. Handle Gone/NotFound (Fatal)
                if (response.StatusCode == HttpStatusCode.Gone || response.StatusCode == HttpStatusCode.NotFound)
                {
                    return new StreamResult(StreamOutcome.Gone, $"Transfer Gone ({response.StatusCode})");
                }

                // 2. Handle Unauthorized (401) - Token expired
                if (response.StatusCode == HttpStatusCode.Unauthorized)
                {
                    if (!forceRefresh)
                    {
                        // Retry once with forced refresh
                        _config.Logger.LogWarning("Stream Unauthorized (401). Refreshing token and retrying...");
                        forceRefresh = true;
                        continue;
                    }
                    else
                    {
                        // Second failure -> Retryable (force Session reconnect)
                        return new StreamResult(StreamOutcome.Retryable, "Unauthorized (401) - Check credentials");
                    }
                }

                response.EnsureSuccessStatusCode();

                using var stream = await response.Content.ReadAsStreamAsync(token);
                await readAction(stream, token);
                return new StreamResult(StreamOutcome.Ended);
            }
            catch (OperationCanceledException)
            {
                return new StreamResult(StreamOutcome.Cancelled);
            }
            catch (WireProtocolViolationException ex)
            {
                // The peer sent something this version cannot interpret; reconnecting would only
                // repeat the violation, so this is fatal rather than retryable.
                _config.Logger.LogError("Protocol violation, session failed: {Message}", ex.Message);
                return new StreamResult(StreamOutcome.Fatal, ex.Message);
            }
            catch (Exception ex)
            {
                // Network errors allow retry
                _config.Logger.LogWarning("Stream failed, returning Retryable: {Message}", ex.Message);
                return new StreamResult(StreamOutcome.Retryable, ex.Message);
            }

            // Only a 401 loops here, to retry once with a refreshed token. Every other path
            // returns, including an exception: restarting a broken stream is DataSession's, so
            // that the decision to retry is taken where the session state is known.
        }

        return new StreamResult(StreamOutcome.Cancelled);
    }

    /// <summary>
    /// Asks the hub to end the transfer, on a request of its own. This is for an Abort that could not
    /// be written to the data stream: behind data nobody reads the stream is full, and a request is
    /// the only thing that still gets through. Best effort and bounded by <paramref name="timeout"/>;
    /// never throws. A hub that predates the route answers 404, which is no reason to say anything.
    /// </summary>
    /// <returns>True when the hub accepted the request.</returns>
    public async Task<bool> RequestAbortAsync(TimeSpan timeout)
    {
        using var cts = new CancellationTokenSource(timeout);
        try
        {
            using var request = new HttpRequestMessage(HttpMethod.Post, $"{_dataUrl}/{_transferId}/{WireRoutes.AbortSegment}");
            request.Version = HttpVersion.Version11;
            request.VersionPolicy = HttpVersionPolicy.RequestVersionExact;
            AddAffinity(request);

            var token = await _transport.GetAccessTokenAsync(forceRefresh: false).WaitAsync(cts.Token);
            if (!string.IsNullOrEmpty(token))
            {
                request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
            }

            using var response = await _transport.HttpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cts.Token);
            if (response.IsSuccessStatusCode)
            {
                _config.Logger.LogInformation("Asked the hub to end Transfer={TransferId}: the Abort could not be written to the data stream", _transferId);
                return true;
            }

            _config.Logger.LogDebug("The hub did not end Transfer={TransferId}: {Status}", _transferId, response.StatusCode);
        }
        catch (Exception ex)
        {
            _config.Logger.LogWarning("Could not ask the hub to end Transfer={TransferId} ({Message}); the receiver learns of it when the hub's bridge timeout expires", _transferId, ex.Message);
        }

        return false;
    }

    // Only the bridge's instance can carry the streams: one the client reached for another transfer
    // since, or for its own stream, is the wrong one.
    private void AddAffinity(HttpRequestMessage request)
    {
        if (!string.IsNullOrEmpty(_affinity))
        {
            request.Headers.TryAddWithoutValidation("Cookie", _affinity);
        }
    }

    private HttpRequestMessage NewRequest(HttpMethod method, string url, ConnectionType type)
    {
        var request = new HttpRequestMessage(method, url);
        request.Headers.Add(DataPlane.ConnectionTypeHeader, type.ToString());

        // Stated rather than inherited, on the segments as on the read side: the data plane runs on
        // HTTP/1.1, see "Data Plane Transport: HTTP/1.1" in TransportRArchitecture.md.
        request.Version = HttpVersion.Version11;
        request.VersionPolicy = HttpVersionPolicy.RequestVersionExact;

        AddAffinity(request);
        return request;
    }

}
