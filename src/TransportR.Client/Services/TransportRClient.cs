using System.Net;
using System.Net.Http.Json;
using Microsoft.Extensions.Logging;
using Microsoft.IO;
using TransportR.Abstractions;
using TransportR.Client.Configuration;
using TransportR.Client.Internal;
using TransportR.Client.Sessions;
using TransportR.Interfaces;
using TransportR.Models;
using TransportR.Protocol;

namespace TransportR.Client.Services;

/// <summary>
/// Client for sending and receiving data transfers.
/// Acts as a coordinator between ControlChannel, SessionRegistry, and DataSessions.
/// </summary>
public partial class TransportRClient<T> : IDataSender<T>, IDataReceiver<T>, ISessionContext<T>, IAsyncDisposable
{
    public Guid ClientId => _channel.ClientId;

    /// <summary>The client's control plane. Internal so that the tests can reach the hub through it.</summary>
    internal ControlChannel Channel => _channel;

    // Dependencies
    private readonly ControlChannel _channel;
    private readonly SessionRegistry<T> _sessionRegistry;
    private readonly IBatchSerializer<T> _serializer;
    private readonly ILogger _logger;
    private readonly ClientOptions _options;
    
    // Configuration
    private readonly int _defaultBatchSize;
    private readonly string _dataUrl;

    // Infrastructure
    private readonly RecyclableMemoryStreamManager _streamManager;
    private readonly HttpClient _httpClient;

    public event Func<Receive<T>, Task>? OnTransferStarted;

    public event Func<Send<T>, Task>? OnSendRequested;

    // What InitTransferAsync allows the hub beyond the handshake timeout it announced: the round trip of
    // the request, and the work the hub does around the handshake itself.
    private static readonly TimeSpan HandshakeMargin = TimeSpan.FromSeconds(10);

    private readonly TransferSignaling<T> _signaling;

    // Internal so that only the builder instantiates it.
    internal TransportRClient(
        ControlChannel channel,
        SessionRegistry<T> sessionRegistry,
        IBatchSerializer<T> serializer,
        ILogger logger,
        int defaultBatchSize,
        ClientOptions options,
        string dataUrl,
        HttpClient httpClient,
        RecyclableMemoryStreamManager streamManager)
    {
        _channel = channel;
        _sessionRegistry = sessionRegistry;
        _serializer = serializer;
        _logger = logger;
        _defaultBatchSize = defaultBatchSize;
        _options = options;
        _dataUrl = dataUrl;
        
        _streamManager = streamManager;
        _httpClient = httpClient;
        
        // SRP: Extract signaling logic
        _signaling = new TransferSignaling<T>(
            channel,
            sessionRegistry,
            this, // ISessionContext
            logger,
            defaultBatchSize,
            dataUrl,
            (t) => OnTransferStarted?.Invoke(t) ?? Task.CompletedTask,
            () => OnSendRequested
        );
        _signaling.Initialize();
    }


    private bool _registered;
    private Timer? _presenceTimer;
    private TokenRenewer? _renewer;

    // Held by whoever registers this client with the hub, so that two registrations do not overlap;
    // a refresh holds it across the token push it waits for. Never disposed: a caller still waiting
    // on it when the client goes must not meet an ObjectDisposedException.
    private readonly SemaphoreSlim _registration = new(1, 1);

    public async Task ConnectAsync(IEnumerable<string>? groups = null, CancellationToken cancellationToken = default)
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

    /// <summary>
    /// Registers this client with the hub again, now: the groups its identity holds at this instant,
    /// and the token its provider gives now, under the same <see cref="ClientId"/>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// For a host that owns the rights of its clients and changes them while they run. The hub writes a
    /// client's groups when it registers and when it renews its token, so until one of those a client
    /// keeps the groups it had; this is the third occasion, at the host's choosing.
    /// </para>
    /// <para>
    /// Transfers in flight are not interrupted: the client registers on a new stream before the old one
    /// closes, and nothing else restarts. The hub judges them again against the new groups, as it does
    /// at every renewal, and ends those the new groups no longer let reach each other. A client with no
    /// token provider (dev mode, a static token) registers again with the groups it was given, and
    /// presents no token.
    /// </para>
    /// <para>
    /// It returns once the hub has recorded both the registration and the token. Nothing has been
    /// written when the token provider fails (its exception is raised) or when the identity holds no
    /// group (a <see cref="HubRequestFailedException"/>): the registration the hub had
    /// stays. A hub that turns the token away after the registration is raised as an
    /// <see cref="HttpRequestException"/>, and the client keeps presenting the token it holds, as it does
    /// after a restart. Refreshes, the registration the presence heartbeat makes when the hub has
    /// forgotten the client, and the heartbeat itself take turns (a beat that finds one under way is
    /// skipped); the renewal is ordered against the push by the renewer, which makes one attempt at a time.
    /// </para>
    /// </remarks>
    /// <exception cref="InvalidOperationException">
    /// The client is not connected: it has not connected, has disconnected, or its connection is being
    /// started again. Whether to wait and call again is the caller's.
    /// </exception>
    public async Task RefreshRegistrationAsync(CancellationToken cancellationToken = default)
    {
        await _registration.WaitAsync(cancellationToken);
        try
        {
            // Read once the turn is ours: the one before it may have lost the stream.
            if (!_registered || !_channel.IsOpen)
            {
                throw new InvalidOperationException(
                    "The registration can only be refreshed while the client is connected to the hub.");
            }

            // Asked of the provider first, so that a provider that is down costs nothing, and so that
            // the new stream registers the identity this token attests.
            var token = _renewer != null ? await _channel.AcquireTokenAsync() : null;

            _logger.LogInformation("Refreshing the registration of Client (ClientId={ClientId})...", ClientId);
            await _channel.ReplaceAsync(token, cancellationToken);

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

    /// <summary>
    /// Asks the hub whether it still holds a registration for this client's connection: the answer
    /// the presence heartbeat acts on, for a host that wants it now.
    /// </summary>
    /// <returns>
    /// <c>false</c> when the hub has forgotten the connection (a hub that restarted, a registration
    /// evicted, another stream of the same identity that took it over). The next presence heartbeat
    /// registers the client again; <see cref="RefreshRegistrationAsync"/> does it at once.
    /// </returns>
    /// <remarks>
    /// Waits for a registration under way, so that a client being registered again is not reported
    /// as unknown in the middle of it. The wait lasts as long as that registration does, a refresh
    /// waiting on its token provider included: <paramref name="cancellationToken"/> bounds it.
    /// </remarks>
    /// <exception cref="InvalidOperationException">
    /// The client is not connected: it has not connected, its <see cref="ConnectAsync"/> failed, it has
    /// disconnected, or its connection is being started again. In that last case only, the client
    /// registers by itself once the connection is back.
    /// </exception>
    /// <exception cref="HttpRequestException">
    /// The hub could not be asked, or a 404 came back without <see cref="WireRoutes.RefusedHeader"/>:
    /// no TransportR route served the request.
    /// </exception>
    public async Task<bool> IsRegisteredAsync(CancellationToken cancellationToken = default)
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

        // Stop all sessions
        await _sessionRegistry.DisposeAsync();
        
        // Stop Hub
        await _channel.DisposeAsync();
    }

    public async Task<List<ClientInfo>> GetReceiversAsync(CancellationToken cancellationToken = default)
    {
        using var response = await _channel.SendAsync(HttpMethod.Get, WireRoutes.ReceiversSegment, body: null, affinity: null, cancellationToken);
        await ControlChannel.EnsureAcceptedAsync(response, cancellationToken);
        return await response.Content.ReadFromJsonAsync<List<ClientInfo>>(ControlPlane.Json, cancellationToken) ?? [];
    }

    public async Task<Send<T>> InitTransferAsync(Guid targetClientId, int? batchSize = null)
    {
        int size = batchSize ?? _defaultBatchSize;
        _logger.LogInformation("Initiating Transfer to ReceiverId={TargetId} (Size={Size})...", targetClientId, size);

        // The affinity this request carries is the transfer's from then on: the handshake runs on the
        // instance it reaches, and the bridge is there.
        var request = new InitTransferRequest(targetClientId, size, (int)_options.HandshakeTimeout.TotalMilliseconds);
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

        return new HttpSend<T>(await _signaling.StartRequestedSendAsync(opened.TransferId, size, targetClientId, affinity));
    }

    public IReadOnlyList<TransferInfo> GetTransfers()
    {
        return
        [
            .. _sessionRegistry.Sessions
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
                    s.AcksSent
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

    public async ValueTask DisposeAsync()
    {
        await DisconnectAsync();
        _httpClient.Dispose();
        GC.SuppressFinalize(this);
    }
    
    // --- Internal Logic ---
    


    #region ISessionContext Implementation

    ILogger ISessionConfig.Logger => _logger;
    IBatchSerializer<T> ISessionConfig<T>.Serializer => _serializer;
    HttpClient IStreamTransport.HttpClient => _httpClient;
    int ISessionConfig.BatchFlushIntervalMs => _options.BatchFlushIntervalMs;
    RecyclableMemoryStreamManager ISessionConfig.StreamManager => _streamManager;
    ClientOptions ISessionConfig.Options => _options;

    void IControlPlane.RemoveSession(string transferId)
    {
        _sessionRegistry.TryRemove(transferId, out _);
    }

    async Task IControlPlane.ReportTransferAsync(string transferId, string? affinity, long items, long acks, CancellationToken cancellationToken)
    {
        using var response = await _channel.SendAsync(HttpMethod.Post,
            $"{WireRoutes.TransfersSegment}/{transferId}/{WireRoutes.HeartbeatSegment}",
            new TransferProgress(items, acks), affinity, cancellationToken);
        response.EnsureSuccessStatusCode();
    }

    async Task<bool> IControlPlane.ProbeAsync(string transferId, string? affinity, CancellationToken cancellationToken)
    {
        using var response = await _channel.SendAsync(HttpMethod.Post,
            $"{WireRoutes.TransfersSegment}/{transferId}/{WireRoutes.PingSegment}",
            body: null, affinity, cancellationToken);
        ControlChannel.EnsureFromHub(response);
        return response.IsSuccessStatusCode || response.StatusCode == HttpStatusCode.NotFound;
    }

    Task<string?> IStreamTransport.GetAccessTokenAsync(bool forceRefresh)
    {
        return _channel.GetAccessTokenAsync(forceRefresh);
    }

    #endregion
}
