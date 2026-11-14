using System.Net;
using System.Net.Http.Json;
using Microsoft.Extensions.Logging;
using TransportR.Client.Configuration;
using TransportR.Client.Internal;
using TransportR.Client.Models;
using TransportR.Client.Sessions;
using TransportR.Protocol;

namespace TransportR.Client.Services;

/// <summary>
/// What a client is to the hub, whatever its transfers carry: its registration and the turn that
/// orders it, the presence heartbeat, the token renewal, the write-transport probe, the requests it
/// makes of the control plane, and the order in which all of it is let go. It is also the
/// <see cref="ISessionContext"/> its sessions run in.
/// </summary>
/// <remarks>
/// Owns the <see cref="ControlChannel"/> and the <see cref="HttpClient"/> it is given, and disposes
/// both. The sessions stay the client's to make and to hold, and their release is handed over here:
/// it has to come before the <see cref="HttpClient"/> goes, which a session being released still
/// needs to tell its peer.
/// </remarks>
internal sealed class ClientConnection : ISessionContext, IAsyncDisposable
{
    // What OpenTransferAsync allows the hub beyond the handshake timeout it announced: the round trip of
    // the request, and the work the hub does around the handshake itself.
    private static readonly TimeSpan HandshakeMargin = TimeSpan.FromSeconds(10);

    private readonly ControlChannel _channel;
    private readonly ILogger _logger;
    private readonly ClientOptions _options;
    private readonly HttpClient _httpClient;
    private readonly IAsyncDisposable _sessions;
    private readonly Func<IEnumerable<IDataSession>> _listSessions;
    private readonly Action<string> _removeSession;
    private readonly WriteTransportProbe? _probe;

    private bool _registered;
    private Timer? _presenceTimer;
    private TokenRenewer? _renewer;

    // Held by whoever registers this client with the hub, so that two registrations do not overlap;
    // a refresh holds it across the token push it waits for. Never disposed: a caller still waiting
    // on it when the client goes must not meet an ObjectDisposedException.
    private readonly SemaphoreSlim _registration = new(1, 1);

    /// <param name="logger">The client's own: what is logged here is logged as the client.</param>
    /// <param name="sessions">The client's sessions, released by <see cref="DisconnectAsync"/>.</param>
    /// <param name="listSessions">
    /// The sessions held at the moment of the call. Asked again each time, since what it returns does
    /// not follow the sessions that come and go after it.
    /// </param>
    /// <param name="removeSession">Drops a session from the client's registry once it has ended.</param>
    /// <param name="dataUrl">The hub's <see cref="WireRoutes.StreamBase"/>, which the write-transport probe goes to.</param>
    public ClientConnection(
        ControlChannel channel,
        ILogger logger,
        ClientOptions options,
        HttpClient httpClient,
        string dataUrl,
        IAsyncDisposable sessions,
        Func<IEnumerable<IDataSession>> listSessions,
        Action<string> removeSession)
    {
        _channel = channel;
        _logger = logger;
        _options = options;
        _httpClient = httpClient;
        _sessions = sessions;
        _listSessions = listSessions;
        _removeSession = removeSession;

        if (options.ProbeWriteTransport)
        {
            _probe = new WriteTransportProbe(this, channel.CookieHeader, dataUrl, options, logger);
            channel.Registered = _probe.Start;
        }
    }

    public Guid ClientId => _channel.ClientId;

    public ControlChannel Channel => _channel;

    public HttpClient HttpClient => _httpClient;

    public ILogger Logger => _logger;

    public ClientOptions Options => _options;

    public Task<string?> GetAccessTokenAsync(bool forceRefresh = false) => _channel.GetAccessTokenAsync(forceRefresh);

    public void RemoveSession(string transferId) => _removeSession(transferId);

    public async Task ConnectAsync(IEnumerable<string>? groups, CancellationToken cancellationToken)
    {
        await _registration.WaitAsync(cancellationToken);
        try
        {
            // A refusal leaves the client as it was, for the caller to try again.
            var groupsList = groups?.ToList();
            _logger.LogInformation("Registering Client (ClientId={ClientId}, Groups=[{Groups}])...",
                ClientId, groupsList != null ? string.Join(", ", groupsList) : "none");
            await _channel.OpenAsync(groupsList, cancellationToken);
            _registered = true;

            StartPresenceHeartbeat();
            StartTokenRenewal();
        }
        finally
        {
            _registration.Release();
        }
    }

    public async Task RefreshRegistrationAsync(CancellationToken cancellationToken)
    {
        const string notConnected = "The registration can only be refreshed while the client is connected to the hub.";

        await _registration.WaitAsync(cancellationToken);
        try
        {
            // Read once the turn is ours: the one before it may have disconnected the client.
            if (!_registered) throw new InvalidOperationException(notConnected);

            // Asked of the provider first, so that a provider that is down costs nothing, and so that
            // the new stream registers the identity this token attests.
            var token = _renewer != null ? await _channel.AcquireTokenAsync() : null;

            _logger.LogInformation("Refreshing the registration of Client (ClientId={ClientId})...", ClientId);
            if (_channel.IsOpen)
            {
                await _channel.ReplaceAsync(token, cancellationToken);
            }
            else
            {
                try
                {
                    await _channel.ReopenNowAsync(token, cancellationToken);
                }
                catch (Exception ex) when (ex is not HubRequestFailedException && !cancellationToken.IsCancellationRequested)
                {
                    throw new InvalidOperationException(notConnected, ex);
                }
            }

            // The renewer arms its next renewal on the expiry the hub answers with.
            if (token != null)
            {
                await _renewer!.PushNowAsync(token, cancellationToken);
            }
        }
        finally
        {
            _registration.Release();
        }
    }

    public async Task<bool> IsRegisteredAsync(CancellationToken cancellationToken)
    {
        await _registration.WaitAsync(cancellationToken);
        try
        {
            if (!_registered || !_channel.IsOpen)
            {
                throw new InvalidOperationException(
                    "The registration can only be checked while the client is connected to the hub.");
            }

            return await _channel.IsKnownAsync(cancellationToken);
        }
        finally
        {
            _registration.Release();
        }
    }

    private void StartTokenRenewal()
    {
        if (_channel.HasTokenProvider)
        {
            _renewer ??= new TokenRenewer(_channel, _options.TokenRefreshThreshold, _logger);
            _renewer.Start();
        }
        else if (_channel.TokenExpiry is { } expiry)
        {
            // Nothing here can get another token, and the hub holds a stream to the expiry it
            // attested: it closes this one then, and a new one cannot get past authentication.
            _logger.LogWarning(
                "The access token is static and expires at {Expiry:u}. The hub closes this connection then. Configure a token provider to keep it.",
                expiry);
        }
    }

    /// <summary>
    /// Tells the hub this client is still there, whether or not a transfer is running, and
    /// re-registers if the hub says it no longer knows this connection.
    /// </summary>
    /// <remarks>
    /// The per-transfer heartbeat cannot do this: it only exists while a transfer does, and it
    /// reports transfer statistics rather than the liveness of the client itself.
    /// </remarks>
    private void StartPresenceHeartbeat()
    {
        if (_presenceTimer != null) return;

        var interval = (int)_options.PresenceHeartbeatInterval.TotalMilliseconds;
        _presenceTimer = new Timer(async _ => await SendPresenceHeartbeatAsync(), null, interval, interval);
    }

    private async Task SendPresenceHeartbeatAsync()
    {
        // A registration being written tells the hub this client is there as well as a beat does, and
        // a beat that read "unknown" before it would register the client a second time over it.
        if (!await _registration.WaitAsync(0)) return;
        try
        {
            if (!_channel.IsOpen) return;

            // Bounded by the next beat: a hub that does not answer must not hold the turn a refresh waits for.
            using var timeout = new CancellationTokenSource(_options.PresenceHeartbeatInterval);
            var known = await _channel.IsKnownAsync(timeout.Token);
            if (!known)
            {
                await ReRegisterHeldAsync("the hub no longer knows this connection");
            }
        }
        catch (Exception ex)
        {
            // A failed heartbeat is not fatal: the next tick retries, and a lost stream is the
            // channel's to reopen, not this timer's.
            _logger.LogDebug(ex, "Presence heartbeat failed for {ClientId}", ClientId);
        }
        finally
        {
            _registration.Release();
        }
    }

    // The caller holds _registration.
    private async Task ReRegisterHeldAsync(string reason)
    {
        try
        {
            _logger.LogInformation("Re-registering Client ({Reason}) (ClientId={ClientId})...", reason, ClientId);
            await _channel.ReplaceAsync(token: null, CancellationToken.None);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to re-register Client {ClientId}", ClientId);
        }
    }

    public async Task DisconnectAsync()
    {
        _logger.LogInformation("Disconnecting Client {ClientId}...", ClientId);

        _registered = false;
        if (_presenceTimer != null)
        {
            await _presenceTimer.DisposeAsync();
            _presenceTimer = null;
        }

        if (_renewer != null)
        {
            await _renewer.DisposeAsync();
            _renewer = null;
        }

        if (_probe != null)
        {
            await _probe.DisposeAsync();
        }

        // Stop all sessions
        await _sessions.DisposeAsync();

        // Stop Hub
        await _channel.DisposeAsync();
    }

    public async Task<List<ClientInfo>> GetReceiversAsync(CancellationToken cancellationToken)
    {
        using var response = await _channel.SendAsync(HttpMethod.Get, WireRoutes.ReceiversSegment, body: null, affinity: null, cancellationToken);
        await ControlChannel.EnsureAcceptedAsync(response, cancellationToken);
        return await response.Content.ReadFromJsonAsync<List<ClientInfo>>(ControlPlane.Json, cancellationToken) ?? [];
    }

    /// <summary>
    /// Asks the hub for a transfer to <paramref name="targetClientId"/>, following it to the instance
    /// that holds the receiver, and returns the transfer the hub opened with the affinity that reached
    /// that instance.
    /// </summary>
    public async Task<(string TransferId, string? Affinity)> OpenTransferAsync(Guid targetClientId)
    {
        // The affinity this request carries is the transfer's from then on: the handshake runs on the
        // instance it reaches, and the bridge is there.
        var request = new InitTransferRequest(targetClientId, (int)_options.HandshakeTimeout.TotalMilliseconds);
        var affinity = _channel.CookieHeader();

        // The hub answers within the handshake timeout it was given or not at all, and the HttpClient has
        // no timeout of its own: this one deadline is all that ends a call to a hub that has gone quiet.
        using var deadline = new CancellationTokenSource(_options.HandshakeTimeout + HandshakeMargin);

        InitTransferResponse opened;
        HttpResponseMessage? response = null;
        try
        {
            response = await _channel.SendAsync(HttpMethod.Post, WireRoutes.TransfersSegment, request, affinity, deadline.Token);
            if (response.StatusCode == HttpStatusCode.MisdirectedRequest)
            {
                // The receiver is on another instance. Only this request goes there: the client's own
                // stream, and its other transfers, stay where they are.
                var redirect = await response.Content.ReadFromJsonAsync<AffinityRedirect>(ControlPlane.Json, deadline.Token)
                    ?? throw new HubRequestFailedException("The hub redirected the transfer without saying where.");
                _logger.LogInformation("The receiver is on another hub instance. Sending the transfer request there (cookie {CookieName}).", redirect.CookieName);

                affinity = ControlChannel.WithCookie(affinity, redirect.CookieName, redirect.Token);
                response.Dispose();
                response = await _channel.SendAsync(HttpMethod.Post, WireRoutes.TransfersSegment, request, affinity, deadline.Token);

                if (response.StatusCode == HttpStatusCode.MisdirectedRequest)
                {
                    throw new HubRequestFailedException($"Target client {targetClientId} is not reachable.");
                }
            }

            await ControlChannel.EnsureAcceptedAsync(response, deadline.Token);
            opened = await response.Content.ReadFromJsonAsync<InitTransferResponse>(ControlPlane.Json, deadline.Token)
                ?? throw new HubRequestFailedException("The hub opened the transfer without naming it.");
        }
        catch (OperationCanceledException) when (deadline.IsCancellationRequested)
        {
            _logger.LogWarning(
                "The hub did not answer the transfer request to ReceiverId={TargetId} within {Seconds}s (handshake timeout {HandshakeSeconds}s plus {MarginSeconds}s).",
                targetClientId, (_options.HandshakeTimeout + HandshakeMargin).TotalSeconds, _options.HandshakeTimeout.TotalSeconds, HandshakeMargin.TotalSeconds);
            throw new HubRequestFailedException("The hub did not answer the transfer request in time.");
        }
        finally
        {
            response?.Dispose();
        }

        return (opened.TransferId, affinity);
    }

    public IReadOnlyList<TransferInfo> GetTransfers()
    {
        return
        [
            .. _listSessions()
                .Select(s => new TransferInfo(
                    s.TransferId,
                    s.DataUrl,
                    s.IsSender ? TransferDirection.Sending : TransferDirection.Receiving,
                    ToTransferStatus(s.State),
                    s.ItemsProcessed,
                    s.BytesTransferred,
                    s.StartedAt,
                    DateTime.UtcNow - s.StartedAt,
                    s.AcksReceived,
                    s.AcksSent,
                    s.WriteTransport,
                    s.WriteTransportFallback
                ))
        ];
    }

    private static TransferStatus ToTransferStatus(SessionState state) => state switch
    {
        SessionState.Completed => TransferStatus.Completed,
        SessionState.Failed => TransferStatus.Failed,
        SessionState.ReceiverCompleteReceived => TransferStatus.Completing,
        _ => TransferStatus.Active
    };

    /// <inheritdoc cref="IControlPlane.ReportTransferAsync"/>
    public async Task ReportTransferAsync(string transferId, string? affinity, long items, long acks, CancellationToken cancellationToken)
    {
        using var response = await _channel.SendAsync(HttpMethod.Post,
            $"{WireRoutes.TransfersSegment}/{transferId}/{WireRoutes.HeartbeatSegment}",
            new TransferProgress(items, acks), affinity, cancellationToken);
        response.EnsureSuccessStatusCode();
    }

    /// <inheritdoc cref="IControlPlane.ProbeAsync"/>
    public async Task<bool> ProbeAsync(string transferId, string? affinity, CancellationToken cancellationToken)
    {
        using var response = await _channel.SendAsync(HttpMethod.Post,
            $"{WireRoutes.TransfersSegment}/{transferId}/{WireRoutes.PingSegment}",
            body: null, affinity, cancellationToken);
        ControlChannel.EnsureFromHub(response);
        return response.IsSuccessStatusCode || response.StatusCode == HttpStatusCode.NotFound;
    }

    public async ValueTask DisposeAsync()
    {
        await DisconnectAsync();
        _httpClient.Dispose();
    }
}
