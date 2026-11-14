using System.Net;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Net.ServerSentEvents;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using TransportR.Client.Configuration;
using TransportR.Protocol;

namespace TransportR.Client.Internal;

/// <summary>
/// The client's side of the control plane: the stream of events the hub reaches it by, held open for
/// as long as the client is, and the requests it makes of the hub, each carrying the token held at
/// that moment and the affinity it is meant for.
/// </summary>
/// <remarks>
/// <para>
/// The stream is the registration. One that ends for any reason but disposal or its replacement is
/// opened again, with a growing, jittered delay, until it is back or the client is disposed. Each
/// opening registers the client afresh, from the principal of that request.
/// </para>
/// <para>
/// Cookies are kept here and not by the handler. The stream, discovery, the presence heartbeat and
/// the renewal carry the client's own, which a proxy with affinity sets; a transfer carries the
/// affinity of the instance holding its bridge, set on each of its requests. A cookie container on
/// the handler would override that one, since it wins over a header set by hand.
/// </para>
/// </remarks>
internal sealed class ControlChannel : IAsyncDisposable
{
    /// <summary>The message of every refusal: the hub answers them all alike, and says why only in its log.</summary>
    internal const string RefusedMessage = "The hub refused the request.";

    /// <summary>The message of a 404 that does not carry <see cref="WireRoutes.RefusedHeader"/>.</summary>
    internal const string NoHubRouteMessage = "The response did not come from a TransportR route (404).";

    // Twice the hub's keepalive interval: a stream that carried nothing for this long is taken as cut.
    private static readonly TimeSpan StreamSilenceLimit = TimeSpan.FromSeconds(30);

    private static readonly TimeSpan FirstReopenDelay = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan MaxReopenDelay = TimeSpan.FromSeconds(30);
    private const int FirstOpenAttempts = 5;
    private static readonly TimeSpan FirstOpenDelay = TimeSpan.FromMilliseconds(500);
    private static readonly TimeSpan MaxOpenDelay = TimeSpan.FromSeconds(5);

    private readonly HttpClient _http;
    private readonly string _clientUrl;
    private readonly Uri _cookieScope;
    private readonly ClientOptions _options;
    private readonly ILogger _logger;
    private readonly Func<Task<string>>? _tokenProvider;
    private readonly CookieContainer _cookies = new();

    // Auth state
    private string? _currentAccessToken;
    private DateTime? _tokenExpiry;

    private readonly CancellationTokenSource _lifetime = new();
    private readonly object _lock = new();

    // One attempt at a time to open a lost stream again, the reopening's or a refresh's: a second
    // stream would register the client over the first, and closing it would then mark the client
    // disconnected. Never disposed, like the client's own turn.
    private readonly SemaphoreSlim _reopenTurn = new(1, 1);
    private EventStream? _current;
    private Task? _reopening;
    private IReadOnlyList<string>? _groups;
    private bool _disposed;

    public ControlChannel(
        string hubUrl,
        Guid clientId,
        HttpClient http,
        ClientOptions options,
        ILogger logger,
        string? initialAccessToken = null,
        Func<Task<string>>? tokenProvider = null)
    {
        var root = hubUrl.TrimEnd('/');
        ClientId = clientId;
        _clientUrl = $"{root}{WireRoutes.ClientBase}/{clientId}";
        _cookieScope = new Uri(root + "/");
        _http = http;
        _options = options;
        _logger = logger;
        _currentAccessToken = initialAccessToken;
        _tokenProvider = tokenProvider;

        if (!string.IsNullOrEmpty(initialAccessToken))
        {
            _tokenExpiry = JwtHelper.ExtractExpiry(initialAccessToken);
        }
    }

    public Guid ClientId { get; }

    /// <summary>
    /// Called with each event the hub sends but the keepalives, on a task of its own: an event that
    /// takes long to handle must not hold the stream up, or the stream would be taken as cut.
    /// </summary>
    public Func<string, string, Task>? EventReceived { get; set; }

    /// <summary>Run once a stream that was lost is open again, the client registered on it.</summary>
    public Func<Task>? Reopened { get; set; }

    /// <summary>True while the client holds a stream the hub registered it on.</summary>
    public bool IsOpen => _current is { Ended: false };

    public bool HasTokenProvider => _tokenProvider != null;

    /// <summary>The expiry the current token declares, when this client can read it.</summary>
    public DateTime? TokenExpiry => _tokenExpiry;

    /// <summary>
    /// The client's own cookies as a <c>Cookie</c> header, or null when it has none: what reaches the
    /// instance holding its stream.
    /// </summary>
    public string? CookieHeader()
    {
        var header = _cookies.GetCookieHeader(_cookieScope);
        return header.Length == 0 ? null : header;
    }

    /// <summary><paramref name="header"/> with <paramref name="name"/> set to <paramref name="value"/>, and its other cookies kept.</summary>
    public static string WithCookie(string? header, string name, string value)
    {
        var others = (header ?? string.Empty)
            .Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Where(cookie => !cookie.StartsWith(name + "=", StringComparison.Ordinal));
        return string.Join("; ", others.Append($"{name}={value}"));
    }

    /// <summary>
    /// Opens the stream and returns once the hub has registered the client on it. A hub out of reach
    /// is tried a few times; a refusal is raised at once, and leaves the channel as it was, for the
    /// caller to try again.
    /// </summary>
    public async Task OpenAsync(IReadOnlyList<string>? groups, CancellationToken cancellationToken)
    {
        if (_current != null) throw new InvalidOperationException("The client is already connected.");

        _groups = groups;
        var delay = FirstOpenDelay;
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                var stream = await OpenStreamAsync(await GetAccessTokenAsync(), cancellationToken);
                if (!Install(stream, expected: null))
                {
                    await stream.DisposeAsync();
                    throw new ObjectDisposedException(nameof(ControlChannel));
                }

                return;
            }
            catch (Exception ex) when (attempt < FirstOpenAttempts && IsTransient(ex) && !cancellationToken.IsCancellationRequested)
            {
                _logger.LogWarning("Connection attempt {Attempt}/{MaxRetries} failed, retrying in {Delay}ms: {Message}",
                    attempt, FirstOpenAttempts, delay.TotalMilliseconds, ex.Message);
                await Task.Delay(delay, cancellationToken);
                delay = TimeSpan.FromTicks(Math.Min(delay.Ticks * 2, MaxOpenDelay.Ticks));
            }
        }
    }

    /// <summary>
    /// Registers the client again on a new stream, with <paramref name="token"/> or the one held now,
    /// and closes the old one once the new one is registered: the client stays reachable throughout.
    /// A refusal leaves the old stream, and the registration it holds, as they were.
    /// </summary>
    public async Task ReplaceAsync(string? token, CancellationToken cancellationToken)
    {
        var old = _current;
        if (old is not { Ended: false }) throw new InvalidOperationException("The client holds no stream to replace.");

        var stream = await OpenStreamAsync(token ?? await GetAccessTokenAsync(), cancellationToken);
        if (!Install(stream, old))
        {
            await stream.DisposeAsync();
            throw new InvalidOperationException("The stream was lost while it was being replaced.");
        }
    }

    /// <summary>
    /// Registers the client now on a new stream in place of the one it lost, with <paramref name="token"/>,
    /// rather than at the next attempt of the reopening. A refusal is raised as the hub gives it, and the
    /// reopening goes on either way. Returns at once when the stream is already back.
    /// </summary>
    public async Task ReopenNowAsync(string? token, CancellationToken cancellationToken)
    {
        EventStream lost;
        lock (_lock)
        {
            if (_disposed || _current == null) throw new InvalidOperationException("The client holds no stream.");
            if (!_current.Ended) return;
            lost = _current;
        }

        await ReopenOnceAsync(lost, token, cancellationToken);
    }

    /// <summary>
    /// Asks the hub whether it still holds a registration for the stream the client holds: false when
    /// it has forgotten it, and the client must register again. A 404 the hub did not mark raises.
    /// </summary>
    public async Task<bool> IsKnownAsync(CancellationToken cancellationToken)
    {
        var connectionId = _current is { Ended: false } open
            ? open.ConnectionId
            : throw new InvalidOperationException("The client holds no stream.");

        using var response = await SendAsync(HttpMethod.Post, $"{WireRoutes.PresenceSegment}/{connectionId}", body: null, affinity: null, cancellationToken);
        EnsureFromHub(response);
        if (response.StatusCode == HttpStatusCode.NotFound) return false;

        response.EnsureSuccessStatusCode();
        return true;
    }

    /// <summary>
    /// Sends a request under the client's path, <c>{ClientBase}/{clientId}/{path}</c>, with
    /// <paramref name="body"/> as JSON.
    /// </summary>
    /// <param name="affinity">
    /// The cookies of the transfer it is about; null for the client's own, which a response may update.
    /// </param>
    public Task<HttpResponseMessage> SendAsync(HttpMethod method, string path, object? body, string? affinity, CancellationToken cancellationToken)
    {
        var request = new HttpRequestMessage(method, $"{_clientUrl}/{path}");
        if (body != null)
        {
            request.Content = JsonContent.Create(body, body.GetType(), options: ControlPlane.Json);
        }

        return SendAsync(request, affinity, cancellationToken);
    }

    /// <summary>
    /// Sends <paramref name="request"/> over HTTP/1.1, with the token held now unless it carries one,
    /// and with <paramref name="affinity"/> or the client's own cookies. The request is disposed.
    /// </summary>
    public async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, string? affinity, CancellationToken cancellationToken)
    {
        using (request)
        {
            await PrepareAsync(request, affinity ?? CookieHeader());

            var response = await _http.SendAsync(request, cancellationToken);
            if (affinity == null) Absorb(response);
            return response;
        }
    }

    /// <summary>The path of a request under the client's own, for a caller that builds the request itself.</summary>
    public string UrlOf(string path) => $"{_clientUrl}/{path}";

    /// <summary>
    /// Fails with a <see cref="HubRequestFailedException"/> when the hub refused the request or could
    /// not carry it out, and with an <see cref="HttpRequestException"/> on any other failure.
    /// </summary>
    public static async Task EnsureAcceptedAsync(HttpResponseMessage response, CancellationToken cancellationToken = default)
    {
        if (response.IsSuccessStatusCode) return;

        EnsureFromHub(response);
        if (response.StatusCode == HttpStatusCode.NotFound)
        {
            throw new HubRequestFailedException(RefusedMessage);
        }

        if (response.StatusCode == HttpStatusCode.Conflict
            && await response.Content.ReadFromJsonAsync<HubError>(ControlPlane.Json, cancellationToken) is { } error)
        {
            throw new HubRequestFailedException(error.Message);
        }

        response.EnsureSuccessStatusCode();
    }

    /// <summary>
    /// Fails with an <see cref="HttpRequestException"/> on a 404 without <see cref="WireRoutes.RefusedHeader"/>:
    /// no TransportR route answered it, so it is no refusal, and whatever stands in front of the hub,
    /// or the hub's address, is to blame.
    /// </summary>
    public static void EnsureFromHub(HttpResponseMessage response)
    {
        if (response.StatusCode == HttpStatusCode.NotFound && !response.Headers.Contains(WireRoutes.RefusedHeader))
        {
            throw new HttpRequestException(NoHubRouteMessage, inner: null, HttpStatusCode.NotFound);
        }
    }

    public async Task<string?> GetAccessTokenAsync(bool forceRefresh = false)
    {
        if (_tokenProvider == null) return _currentAccessToken;

        if (forceRefresh || _currentAccessToken == null ||
            (_tokenExpiry.HasValue && DateTime.UtcNow > _tokenExpiry.Value - _options.TokenRefreshThreshold))
        {
            try
            {
                _logger.LogInformation("Acquiring token (Force={Force})...", forceRefresh);
                await AcquireTokenAsync();
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to acquire token, using existing token");
            }
        }
        return _currentAccessToken;
    }

    /// <summary>
    /// Asks the provider for a token now, whatever the one held is worth, and says so when it
    /// cannot. A request that needs a token takes the old one on failure
    /// (<see cref="GetAccessTokenAsync"/>); renewal must know that it did not get a new one.
    /// </summary>
    public async Task<string> AcquireTokenAsync()
    {
        if (_tokenProvider == null) throw new InvalidOperationException("No token provider is configured.");

        var token = await _tokenProvider();
        _currentAccessToken = token;
        _tokenExpiry = JwtHelper.ExtractExpiry(token);
        _logger.LogInformation("Token acquired, new expiry: {Expiry}", _tokenExpiry?.ToString("u") ?? "unknown");
        return token;
    }

    public async ValueTask DisposeAsync()
    {
        // A host that disconnects its client and then disposes it reaches this twice. The flag is up
        // before the stream goes: its end must not read as a loss to recover from.
        EventStream? current;
        Task? reopening;
        lock (_lock)
        {
            if (_disposed) return;
            _disposed = true;
            current = _current;
            reopening = _reopening;
        }

        _lifetime.Cancel();

        if (reopening != null)
        {
            try { await reopening; } catch (Exception) { /* a reopening that failed has said so already */ }
        }

        if (current != null) await current.DisposeAsync();

        _lifetime.Dispose();
    }

    /// <summary>
    /// Makes <paramref name="stream"/> the client's, in place of <paramref name="expected"/>, and starts
    /// reading it. False, and nothing changed, when the channel was disposed or another stream took the
    /// place meanwhile.
    /// </summary>
    private bool Install(EventStream stream, EventStream? expected)
    {
        EventStream? old;
        lock (_lock)
        {
            if (_disposed || !ReferenceEquals(_current, expected)) return false;

            old = _current;
            _current = stream;
            stream.Reading = Task.Run(() => ReadAsync(stream));
        }

        if (old != null)
        {
            old.Replaced = true;
            _ = old.DisposeAsync().AsTask();
        }

        return true;
    }

    private async Task ReadAsync(EventStream stream)
    {
        string why;
        try
        {
            while (true)
            {
                stream.Cancel.CancelAfter(StreamSilenceLimit);
                if (!await stream.Events.MoveNextAsync())
                {
                    why = "the hub ended it";
                    break;
                }

                var item = stream.Events.Current;
                if (item.EventType != ControlPlane.KeepAlive)
                {
                    _ = DispatchAsync(item);
                }
            }
        }
        catch (Exception) when (stream.Replaced || _lifetime.IsCancellationRequested)
        {
            stream.Ended = true;
            return;
        }
        catch (OperationCanceledException)
        {
            why = $"nothing received for {StreamSilenceLimit.TotalSeconds:0}s";
        }
        catch (Exception ex)
        {
            why = ex.Message;
        }

        stream.Ended = true;
        if (stream.Replaced) return;

        lock (_lock)
        {
            if (_disposed || !ReferenceEquals(stream, _current)) return;

            // A hub that ends the stream on purpose (a token that ran out) is no exception: nothing else
            // would bring the client back.
            _logger.LogWarning("The stream of events from the hub ended ({Why}). Opening it again.", why);
            _reopening = ReopenAsync(stream, _lifetime.Token);
        }
    }

    private async Task ReopenAsync(EventStream lost, CancellationToken cancellationToken)
    {
        var failures = 0;
        while (!cancellationToken.IsCancellationRequested)
        {
            var ceiling = Backoff.Ceiling(FirstReopenDelay, MaxReopenDelay, failures + 1);
            try
            {
                await Task.Delay(Backoff.Jittered(ceiling), cancellationToken);

                await ReopenOnceAsync(lost, token: null, cancellationToken);
                return;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                _logger.LogWarning("Could not open the stream of events again, next attempt in at most {Delay}s: {Message}",
                    ceiling.TotalSeconds, ex.Message);
                failures++;
            }
        }
    }

    /// <summary>
    /// Opens a stream in place of <paramref name="lost"/> and runs <see cref="Reopened"/> once the client
    /// is registered on it. Nothing is opened when another attempt has replaced <paramref name="lost"/>
    /// already, or the channel was disposed.
    /// </summary>
    private async Task ReopenOnceAsync(EventStream lost, string? token, CancellationToken cancellationToken)
    {
        await _reopenTurn.WaitAsync(cancellationToken);
        try
        {
            lock (_lock)
            {
                if (_disposed || !ReferenceEquals(_current, lost)) return;
            }

            // Without a token of the caller's, one from the provider: the stream comes back with the
            // token the old stream lacked.
            var stream = await OpenStreamAsync(token ?? await GetAccessTokenAsync(), cancellationToken);
            if (!Install(stream, lost))
            {
                await stream.DisposeAsync();
                return;
            }
        }
        finally
        {
            _reopenTurn.Release();
        }

        _logger.LogInformation("The stream of events from the hub is back.");
        if (Reopened is { } reopened) await reopened();
    }

    private async Task DispatchAsync(SseItem<string> item)
    {
        try
        {
            if (EventReceived is { } handler) await handler(item.EventType, item.Data);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Handling the hub's {Event} event failed", item.EventType);
        }
    }

    /// <summary>
    /// Opens a stream and waits for the hub to register the client on it.
    /// </summary>
    /// <remarks>
    /// A proxy with affinity gives a client its cookie on the response to a request that carried none,
    /// while the hub, which read the request, registered the client without it, and nobody could be
    /// sent to that client's instance. So a stream whose response changed the cookies is opened once
    /// more, carrying them. Once: a proxy that sets a new cookie on every response is not followed.
    /// </remarks>
    private async Task<EventStream> OpenStreamAsync(string? token, CancellationToken cancellationToken)
    {
        var sent = CookieHeader();
        var stream = await OpenStreamOnceAsync(token, sent, cancellationToken);

        if (CookieHeader() is not { } now || now == sent) return stream;

        try
        {
            var again = await OpenStreamOnceAsync(token, now, cancellationToken);
            await stream.DisposeAsync();
            return again;
        }
        catch (Exception ex) when (!cancellationToken.IsCancellationRequested)
        {
            _logger.LogWarning("Could not open the stream again with the cookies the hub's proxy set, keeping the first one: {Message}", ex.Message);
            return stream;
        }
    }

    private async Task<EventStream> OpenStreamOnceAsync(string? token, string? cookies, CancellationToken cancellationToken)
    {
        var query = $"{ControlPlane.HeartbeatParameter}={(long)_options.PresenceHeartbeatInterval.TotalMilliseconds}"
            + string.Concat((_groups ?? []).Select(g => $"&{ControlPlane.GroupsParameter}={Uri.EscapeDataString(g)}"));
        var request = new HttpRequestMessage(HttpMethod.Get, $"{_clientUrl}/{WireRoutes.EventsSegment}?{query}");
        request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("text/event-stream"));
        if (!string.IsNullOrEmpty(token)) request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);

        // The stream's own source: the caller's token bounds the opening only.
        var cancel = CancellationTokenSource.CreateLinkedTokenSource(_lifetime.Token);
        var opening = cancellationToken.Register(() => cancel.Cancel());
        HttpResponseMessage? response = null;
        try
        {
            await PrepareAsync(request, cookies);
            cancel.CancelAfter(StreamSilenceLimit);

            response = await _http.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancel.Token);
            Absorb(response);
            EnsureFromHub(response);
            if (response.StatusCode == HttpStatusCode.NotFound)
            {
                throw new HubRequestFailedException(RefusedMessage);
            }

            response.EnsureSuccessStatusCode();

            var body = await response.Content.ReadAsStreamAsync(cancel.Token);
            var events = SseParser.Create(body).EnumerateAsync(cancel.Token).GetAsyncEnumerator(cancel.Token);
            if (!await events.MoveNextAsync() || events.Current.EventType != ControlPlane.Registered
                || JsonSerializer.Deserialize<RegisteredEvent>(events.Current.Data, ControlPlane.Json) is not { } registered)
            {
                throw new HttpRequestException("The hub's stream did not open with the registration.");
            }

            opening.Dispose();
            return new EventStream(response, events, cancel, registered.ConnectionId);
        }
        catch
        {
            opening.Dispose();
            request.Dispose();
            response?.Dispose();
            cancel.Dispose();
            throw;
        }
    }

    private async Task PrepareAsync(HttpRequestMessage request, string? cookies)
    {
        // HTTP/1.1, as on the data plane: a proxy that buffers or multiplexes HTTP/2 has not been tried
        // with the stream.
        request.Version = HttpVersion.Version11;
        request.VersionPolicy = HttpVersionPolicy.RequestVersionExact;

        if (request.Headers.Authorization == null && await GetAccessTokenAsync() is { Length: > 0 } token)
        {
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
        }

        if (!string.IsNullOrEmpty(cookies))
        {
            request.Headers.TryAddWithoutValidation("Cookie", cookies);
        }
    }

    private void Absorb(HttpResponseMessage response)
    {
        if (!response.Headers.TryGetValues("Set-Cookie", out var values)) return;

        foreach (var value in values)
        {
            try
            {
                _cookies.SetCookies(_cookieScope, value);
            }
            catch (CookieException ex)
            {
                // Set by whatever stands in front of the hub, so not the client's to fail on.
                _logger.LogWarning("Ignoring a cookie the hub's response set and that cannot be read: {Message}", ex.Message);
            }
        }
    }

    private static bool IsTransient(Exception ex) => ex is HttpRequestException or IOException or OperationCanceledException;

    private sealed class EventStream : IAsyncDisposable
    {
        private readonly HttpResponseMessage _response;

        public EventStream(HttpResponseMessage response, IAsyncEnumerator<SseItem<string>> events, CancellationTokenSource cancel, string connectionId)
        {
            _response = response;
            Events = events;
            Cancel = cancel;
            ConnectionId = connectionId;
        }

        public IAsyncEnumerator<SseItem<string>> Events { get; }

        /// <summary>Ends the stream: on its silence, on its replacement, or with the channel.</summary>
        public CancellationTokenSource Cancel { get; }

        public string ConnectionId { get; }

        public Task? Reading { get; set; }

        public volatile bool Replaced;

        public volatile bool Ended;

        public async ValueTask DisposeAsync()
        {
            try { Cancel.Cancel(); } catch (ObjectDisposedException) { }

            if (Reading != null)
            {
                try { await Reading; } catch (Exception) { /* the loop reports its own end */ }
            }

            try { await Events.DisposeAsync(); } catch (Exception) { /* the stream is gone either way */ }
            _response.Dispose();
            Cancel.Dispose();
        }
    }
}
