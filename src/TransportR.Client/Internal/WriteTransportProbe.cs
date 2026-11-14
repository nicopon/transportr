using System.Net;
using System.Net.Http.Headers;
using System.Net.WebSockets;
using Microsoft.Extensions.Logging;
using TransportR.Client.Configuration;
using TransportR.Client.Sessions;
using TransportR.Protocol;

namespace TransportR.Client.Internal;

/// <summary>
/// Proves, after each registration of the client, what the path to the hub carries of a write stream,
/// for the hub to tell its host before any transfer: a WebSocket, else segments, as the policy allows.
/// </summary>
/// <remarks>
/// It informs and decides nothing: no transfer reads it, and each still finds its own transport when its
/// stream opens. One probe at a time: a new registration cancels the one under way, whose stream the hub
/// would refuse by now, and starts its own once that one has stopped. The client only logs the outcome.
/// </remarks>
internal sealed class WriteTransportProbe : IAsyncDisposable
{
    private readonly IStreamTransport _transport;
    private readonly Func<string?> _affinity;
    private readonly string _url;
    private readonly ClientOptions _options;
    private readonly ILogger _logger;
    private readonly object _lock = new();

    private CancellationTokenSource? _current;
    private Task _running = Task.CompletedTask;
    private bool _disposed;

    /// <param name="affinity">The client's own cookies at the time of a probe, which reach the instance holding its stream.</param>
    /// <param name="dataUrl">The hub's <see cref="WireRoutes.StreamBase"/>.</param>
    public WriteTransportProbe(IStreamTransport transport, Func<string?> affinity, string dataUrl, ClientOptions options, ILogger logger)
    {
        _transport = transport;
        _affinity = affinity;
        _url = $"{dataUrl}/{WireRoutes.ProbeSegment}/{WireRoutes.SendSegment}";
        _options = options;
        _logger = logger;
    }

    /// <summary>Probes, in the background, for the registration on the stream of events <paramref name="connectionId"/>.</summary>
    public void Start(string connectionId)
    {
        lock (_lock)
        {
            if (_disposed) return;

            var replaced = _current;
            replaced?.Cancel();
            var probe = new CancellationTokenSource();
            _current = probe;

            var previous = _running;
            _running = Task.Run(async () =>
            {
                await previous;
                replaced?.Dispose();
                await RunAsync(connectionId, probe.Token);
            });
        }
    }

    public async ValueTask DisposeAsync()
    {
        Task running;
        CancellationTokenSource? current;
        lock (_lock)
        {
            if (_disposed) return;
            _disposed = true;
            current = _current;
            current?.Cancel();
            running = _running;
        }

        await running;
        current?.Dispose();
    }

    // Never throws: the task is awaited by the next probe and by disposal.
    private async Task RunAsync(string connectionId, CancellationToken token)
    {
        try
        {
            await ProbeAsync(connectionId, token);
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
        }
        catch (StreamRefusedException refused)
        {
            // A refusal of the WebSocket's is one of the POST's as well.
            NothingCarried(refused.Outcome == StreamOutcome.Gone ? "the hub refused the probe" : refused.Message);
        }
        catch (Exception ex)
        {
            NothingCarried(ex.Message);
        }
    }

    private async Task ProbeAsync(string connectionId, CancellationToken token)
    {
        var policy = _options.WriteTransportPolicy;
        var query = $"{DataPlane.ConnectionParameter}={Uri.EscapeDataString(connectionId)}";
        string? noWebSocket = null;

        if (policy != WriteTransportPolicy.SegmentsOnly)
        {
            var opening = await ProvenWebSocket.OpenAsync(
                _transport,
                ProvenWebSocket.UriOf(_url, query),
                accessToken => ProvenWebSocket.NewSocket(type: null, _affinity(), accessToken, _options.PresenceHeartbeatInterval),
                _options.WebSocketOpenBound,
                _logger,
                token);

            if (opening.Socket is { } socket)
            {
                _logger.LogInformation("Write-transport probe: the path to the hub carries a write stream over {Transport}", WriteTransport.WebSocket);
                await CloseAsync(socket, token);
                return;
            }

            noWebSocket = $"no WebSocket, {opening.Unavailable}";
            if (policy == WriteTransportPolicy.RequireWebSocket)
            {
                NothingCarried(noWebSocket);
                return;
            }
        }

        if (await PostAsync(query, token) is { } failure)
        {
            NothingCarried(noWebSocket == null ? failure : $"{noWebSocket}; {failure}");
        }
        else if (noWebSocket == null)
        {
            _logger.LogInformation("Write-transport probe: the path to the hub carries a write stream over {Transport}", WriteTransport.Segments);
        }
        else
        {
            _logger.LogInformation("Write-transport probe: the path to the hub carries a write stream over {Transport}, {Reason}", WriteTransport.Segments, noWebSocket);
        }
    }

    private void NothingCarried(string why) =>
        _logger.LogWarning("Write-transport probe: nothing {Policy} allows reached the hub: {Reason}", _options.WriteTransportPolicy, why);

    // The hub waits for this Close, within a bound of its own, and answers it.
    private async Task CloseAsync(WebSocket socket, CancellationToken token)
    {
        using (socket)
        {
            using var closing = CancellationTokenSource.CreateLinkedTokenSource(token);
            closing.CancelAfter(_options.WebSocketOpenBound);
            try
            {
                await socket.CloseAsync(WebSocketCloseStatus.NormalClosure, null, closing.Token);
            }
            catch (Exception ex) when (!token.IsCancellationRequested)
            {
                _logger.LogDebug("Write-transport probe: the WebSocket did not close cleanly ({Message})", ex.Message);
            }
        }
    }

    /// <returns>Null once the hub took the empty <c>POST</c>, or why it did not.</returns>
    private async Task<string?> PostAsync(string query, CancellationToken token)
    {
        var refreshed = false;
        while (true)
        {
            using var request = new HttpRequestMessage(HttpMethod.Post, $"{_url}?{query}")
            {
                Version = HttpVersion.Version11,
                VersionPolicy = HttpVersionPolicy.RequestVersionExact,
            };

            if (_affinity() is { Length: > 0 } cookies)
            {
                request.Headers.TryAddWithoutValidation("Cookie", cookies);
            }

            if (await _transport.GetAccessTokenAsync(refreshed) is { Length: > 0 } accessToken)
            {
                request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", accessToken);
            }

            HttpResponseMessage response;
            try
            {
                response = await _transport.HttpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, token);
            }
            catch (HttpRequestException ex)
            {
                return $"segments, {ex.Message}";
            }

            using (response)
            {
                switch (response.StatusCode)
                {
                    case HttpStatusCode.NoContent:
                        return null;

                    case HttpStatusCode.Unauthorized when !refreshed:
                        _logger.LogWarning("Stream Unauthorized (401). Refreshing token and retrying...");
                        refreshed = true;
                        continue;

                    case HttpStatusCode.NotFound when response.Headers.Contains(WireRoutes.RefusedHeader):
                        return "the hub refused the probe";

                    default:
                        return $"segments, the POST was answered {(int)response.StatusCode}";
                }
            }
        }
    }
}
