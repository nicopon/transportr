using Microsoft.AspNetCore.SignalR.Client;
using Microsoft.Extensions.Logging;
using Microsoft.IO;
using TransportR.Abstractions;
using TransportR.Client.SignalR.Configuration;
using TransportR.Client.SignalR.Internal;
using TransportR.Client.SignalR.Sessions;
using TransportR.Interfaces;
using TransportR.Models;

namespace TransportR.Client.SignalR.Services;

/// <summary>
/// SignalR-based client for sending and receiving data transfers.
/// Acts as a coordinator between HubConnectionManager, SessionRegistry, and DataSessions.
/// </summary>
public partial class SignalRDataClient<T> : IDataSender<T>, IDataReceiver<T>, ISessionContext<T>, IAsyncDisposable
{
    public Guid ClientId { get; }

    /// <summary>
    /// The single connection this client carries, current across reconnects and affinity
    /// redirects. For a host extending the hub (<c>DataHubBuilder.UseHub&lt;THub&gt;</c>) and
    /// hooking its own handlers on via <c>TransportRClientBuilder.WithControlConnection</c>, this
    /// is what its own invocations go through.
    /// </summary>
    public HubConnection ControlConnection => _connectionManager.Connection;

    // Dependencies
    private readonly HubConnectionManager _connectionManager;
    private readonly SessionRegistry<T> _sessionRegistry;
    private readonly IBatchSerializer<T> _serializer;
    private readonly ILogger _logger;
    private readonly ClientOptions _options;
    
    // Configuration
    private readonly int _defaultBatchSize;
    private readonly string _dataUrl;
    private readonly string _clientUrl;

    // Infrastructure
    private readonly RecyclableMemoryStreamManager _streamManager;
    private readonly HttpClient _httpClient;

    public event Func<Receive<T>, Task>? OnTransferStarted;

    public event Func<Send<T>, Task>? OnSendRequested;

    // The hub declares no default for this: one setting, one place that answers for it.
    private const int HandshakeTimeoutMs = 20_000;

    private readonly SignalRTransferSignaling<T> _signaling;

    // Internal so that only the builder instantiates it.
    internal SignalRDataClient(
        HubConnectionManager connectionManager,
        SessionRegistry<T> sessionRegistry,
        IBatchSerializer<T> serializer,
        ILogger logger,
        int defaultBatchSize,
        ClientOptions options,
        string dataUrl,
        string clientUrl,
        HttpClient httpClient,
        RecyclableMemoryStreamManager streamManager)
    {
        ClientId = Guid.NewGuid();
        _connectionManager = connectionManager;
        _sessionRegistry = sessionRegistry;
        _serializer = serializer;
        _logger = logger;
        _defaultBatchSize = defaultBatchSize;
        _options = options;
        _dataUrl = dataUrl;
        _clientUrl = clientUrl;
        
        _streamManager = streamManager;
        _httpClient = httpClient;
        
        // SRP: Extract signaling logic
        _signaling = new SignalRTransferSignaling<T>(
            connectionManager,
            sessionRegistry,
            this, // ISessionContext
            logger,
            defaultBatchSize,
            dataUrl,
            (t) => OnTransferStarted?.Invoke(t) ?? Task.CompletedTask,
            () => OnSendRequested,
            async _ =>
            {
                // A reconnection gives this client a new connection id while the hub's registration
                // still points at the old one. The presence heartbeat would catch it within one
                // interval; doing it here closes the window immediately.
                if (_registered) await ReRegisterAsync("SignalR reconnection");
            }
        );
        _signaling.Initialize();

        // A connection the hub closed comes back as a stranger to it: registered again, and with the
        // token held now presented.
        _connectionManager.ConnectionRestarted = () => _registered
            ? ReRegisterAsync("the connection was closed")
            : Task.CompletedTask;
    }


    private IEnumerable<string>? _lastConnectGroups;
    private bool _registered;
    private Timer? _presenceTimer;
    private TokenRenewer? _renewer;

    // Held by whoever registers this client with the hub, so that two registrations do not overlap;
    // a refresh holds it across the token push it waits for. Never disposed: a caller still waiting
    // on it when the client goes must not meet an ObjectDisposedException.
    private readonly SemaphoreSlim _registration = new(1, 1);

    public async Task ConnectAsync(IEnumerable<string>? groups = null, CancellationToken cancellationToken = default)
    {
        _lastConnectGroups = groups;
        await _connectionManager.StartAsync(cancellationToken);

        await _registration.WaitAsync(cancellationToken);
        try
        {
            await InvokeConnectAsync("Registering Client", cancellationToken);
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
    /// Transfers in flight are not interrupted: the connection stays up and nothing restarts. The hub
    /// judges them again against the new groups, as it does at every renewal, and ends those the new
    /// groups no longer let reach each other. A client with no token provider (dev mode, a static token)
    /// registers again with the groups it was given, and presents no token.
    /// </para>
    /// <para>
    /// It returns once the hub has recorded both the registration and the token. Nothing has been
    /// written when the token provider fails (its exception is raised) or when the identity holds no
    /// group (a <see cref="Microsoft.AspNetCore.SignalR.HubException"/>): the registration the hub had
    /// stays. A hub that turns the token away after the registration is raised as an
    /// <see cref="HttpRequestException"/>, and the client keeps presenting the token it holds, as it does
    /// after a restart. Refreshes, the registration a restart, a reconnection or a redirect makes, and
    /// the presence heartbeat take turns (a beat that finds one under way is skipped); the renewal is
    /// ordered against the push by the renewer, which makes one attempt at a time.
    /// </para>
    /// <para>
    /// Needs a hub of the same release. An older one writes the older expiry of the connection's
    /// principal between the two steps, and its sweep ends the transfers of a client it finds expired.
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
            // Read once the turn is ours: the one before it may have been a restart.
            if (!_registered || _connectionManager.Connection.State != HubConnectionState.Connected)
            {
                throw new InvalidOperationException(
                    "The registration can only be refreshed while the client is connected to the hub.");
            }

            // Asked of the provider first, so that a provider that is down costs nothing: once the
            // hub has registered the client it has the older expiry of the connection until a token
            // reaches it.
            var token = _renewer != null ? await _connectionManager.AcquireTokenAsync() : null;

            await InvokeConnectAsync("Refreshing the registration of Client", cancellationToken);

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

    // The caller holds _registration.
    private Task InvokeConnectAsync(string what, CancellationToken cancellationToken = default)
    {
        var groupsList = _lastConnectGroups?.ToList();
        _logger.LogInformation("{What} (ClientId={ClientId}, Groups=[{Groups}])...",
            what, ClientId, groupsList != null ? string.Join(", ", groupsList) : "none");

        return _connectionManager.Connection.InvokeAsync(nameof(ICommandHub.Connect), ClientId, groupsList, cancellationToken);
    }

    private void StartTokenRenewal()
    {
        if (_connectionManager.HasTokenProvider)
        {
            _renewer ??= new TokenRenewer(_connectionManager, _httpClient, _clientUrl, ClientId, _options.TokenRefreshThreshold, _logger);
            _renewer.Start();
        }
        else if (_connectionManager.TokenExpiry is { } expiry)
        {
            // Nothing here can get another token, and the hub holds a connection to the expiry it
            // attested: it closes this one then, and a restart cannot get past authentication.
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
            if (_connectionManager.Connection.State != HubConnectionState.Connected) return;

            var known = await _connectionManager.Connection.InvokeAsync<bool>(nameof(ICommandHub.Heartbeat));
            if (!known)
            {
                await ReRegisterHeldAsync("the hub no longer knows this connection");
            }
        }
        catch (Exception ex)
        {
            // A failed heartbeat is not fatal: the next tick retries, and a dropped connection is
            // SignalR's problem, not this timer's.
            _logger.LogDebug(ex, "Presence heartbeat failed for {ClientId}", ClientId);
        }
        finally
        {
            _registration.Release();
        }
    }

    private async Task ReRegisterAsync(string reason)
    {
        await _registration.WaitAsync();
        try
        {
            await ReRegisterHeldAsync(reason);
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
            await InvokeConnectAsync($"Re-registering Client ({reason})");

            // The registration was written from the principal of a connection that may be as old as
            // the token it opened with, however long ago that was.
            _renewer?.PushNow();
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
        await _connectionManager.DisposeAsync();
    }

    public async Task<List<ClientInfo>> GetReceiversAsync(CancellationToken cancellationToken = default)
    {
        return await _connectionManager.Connection.InvokeAsync<List<ClientInfo>>(nameof(ICommandHub.GetReceivers), cancellationToken);
    }

    public async Task<Send<T>> InitTransferAsync(Guid targetClientId, int? batchSize = null)
    {
        int size = batchSize ?? _defaultBatchSize;
        _logger.LogInformation("Initiating Transfer to ReceiverId={TargetId} (Size={Size})...", targetClientId, size);

        try
        {
            var transferId = await _connectionManager.Connection.InvokeAsync<string>(nameof(ICommandHub.InitTransfer), targetClientId, size, HandshakeTimeoutMs);

            return new HttpSend<T>(await _signaling.StartRequestedSendAsync(transferId, size, targetClientId));
        }
        catch (Exception ex) when (ex.Message.Contains("AFFINITY_REDIRECT:"))
        {
            var match = System.Text.RegularExpressions.Regex.Match(ex.Message, @"AFFINITY_REDIRECT:([^:]+):(.+)");
            if (!match.Success) throw new InvalidOperationException($"Invalid affinity redirect format: {ex.Message}");
            
            var cookieName = match.Groups[1].Value;
            var affinityToken = match.Groups[2].Value;
            
            await _connectionManager.HandleAffinityRedirectAsync(cookieName, affinityToken, async () => 
            {
                await _registration.WaitAsync();
                try
                {
                    await InvokeConnectAsync("Re-registering Client after redirect");
                }
                finally
                {
                    _registration.Release();
                }
            });
            
            // Re-invoke Connect is handled by the callback above.
            // Now retry InitTransfer on new connection
            return await InitTransferAsync(targetClientId, batchSize);
        }
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
    HubConnection ISignalRInteraction.HubConnection => _connectionManager.Connection; // Expose underlying connection
    int ISessionConfig.BatchFlushIntervalMs => _options.BatchFlushIntervalMs;
    RecyclableMemoryStreamManager ISessionConfig.StreamManager => _streamManager;
    ClientOptions ISessionConfig.Options => _options;

    void ISignalRInteraction.RemoveSession(string transferId)
    {
        _sessionRegistry.TryRemove(transferId, out _);
    }

    Task<string?> IStreamTransport.GetAccessTokenAsync(bool forceRefresh)
    {
        return _connectionManager.GetAccessTokenAsync(forceRefresh);
    }

    #endregion
}
