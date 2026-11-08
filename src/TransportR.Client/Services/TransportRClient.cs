using Microsoft.Extensions.Logging;
using Microsoft.IO;
using TransportR.Abstractions;
using TransportR.Client.Batching;
using TransportR.Client.Configuration;
using TransportR.Client.Internal;
using TransportR.Client.Sessions;
using TransportR.Interfaces;
using TransportR.Models;
using TransportR.Protocol;

namespace TransportR.Client.Services;

/// <summary>
/// Client for sending and receiving data transfers.
/// Acts as a coordinator between ClientConnection, SessionRegistry, and DataSessions.
/// </summary>
public partial class TransportRClient<T> : IDataSender<T>, IDataReceiver<T>, ISessionContext, IAsyncDisposable
{
    public Guid ClientId => _connection.ClientId;

    /// <summary>The client's control plane. Internal so that the tests can reach the hub through it.</summary>
    internal ControlChannel Channel => _connection.Channel;

    // Dependencies
    private readonly ClientConnection _connection;
    private readonly SessionRegistry<TransferSession<T>> _sessionRegistry;
    private readonly IBatchSerializer<T>? _serializer;
    private readonly ILogger _logger;
    private readonly ClientOptions _options;
    
    // Configuration
    private readonly int _defaultBatchSize;
    private readonly string _dataUrl;

    // Infrastructure
    private readonly RecyclableMemoryStreamManager _streamManager;

    public event Func<Receive<T>, Task>? OnTransferStarted;

    public event Func<Send<T>, Task>? OnSendRequested;

    private readonly TransferSignaling<TransferSession<T>, Send<T>, Receive<T>> _signaling;

    // Internal so that only the builder instantiates it.
    internal TransportRClient(
        ControlChannel channel,
        SessionRegistry<TransferSession<T>> sessionRegistry,
        IBatchSerializer<T>? serializer,
        ILogger logger,
        int defaultBatchSize,
        ClientOptions options,
        string dataUrl,
        HttpClient httpClient,
        RecyclableMemoryStreamManager streamManager)
    {
        _connection = new ClientConnection(channel, logger, options, httpClient, sessionRegistry, () => sessionRegistry.Sessions);
        _sessionRegistry = sessionRegistry;
        _serializer = serializer;
        _logger = logger;
        _defaultBatchSize = defaultBatchSize;
        _options = options;
        _dataUrl = dataUrl;
        
        _streamManager = streamManager;
        
        // SRP: Extract signaling logic
        _signaling = new TransferSignaling<TransferSession<T>, Send<T>, Receive<T>>(
            channel,
            sessionRegistry,
            NewSession,
            session => new HttpSend<T>(session.Session, session.Send),
            session => new HttpReceive<T>(session.Session, session.Receive),
            logger,
            defaultBatchSize,
            (t) => OnTransferStarted?.Invoke(t) ?? Task.CompletedTask,
            () => OnSendRequested
        );
        _signaling.Initialize();
    }

    public Task ConnectAsync(IEnumerable<string>? groups = null, CancellationToken cancellationToken = default)
        => _connection.ConnectAsync(groups, cancellationToken);

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
    /// <para>
    /// While the connection is being started again, the refresh is that start, made at once: the hub
    /// answers it as it answers a registration, so the identity holding no group is raised as a
    /// <see cref="HubRequestFailedException"/> here too. The client goes on starting the connection
    /// again whatever the outcome. That start first waits for an attempt the client is already making,
    /// and gives a hub that does not answer the stream's silence limit (30 s):
    /// <paramref name="cancellationToken"/> cuts both sooner, but not the token provider, which the
    /// refresh asks first.
    /// </para>
    /// </remarks>
    /// <exception cref="InvalidOperationException">
    /// The client is not connected: it has not connected, has disconnected, or its connection is being
    /// started again and the hub could not be reached (the cause is the inner exception). Whether to
    /// wait and call again is the caller's.
    /// </exception>
    public Task RefreshRegistrationAsync(CancellationToken cancellationToken = default)
        => _connection.RefreshRegistrationAsync(cancellationToken);

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
    /// registers by itself once the connection is back, which a hub that refuses it never lets happen:
    /// <see cref="RefreshRegistrationAsync"/> asks the hub, and tells that refusal from a hub out of reach.
    /// </exception>
    /// <exception cref="HttpRequestException">
    /// The hub could not be asked, or a 404 came back without <see cref="WireRoutes.RefusedHeader"/>:
    /// no TransportR route served the request.
    /// </exception>
    public Task<bool> IsRegisteredAsync(CancellationToken cancellationToken = default)
        => _connection.IsRegisteredAsync(cancellationToken);

    public Task DisconnectAsync() => _connection.DisconnectAsync();

    public Task<List<ClientInfo>> GetReceiversAsync(CancellationToken cancellationToken = default)
        => _connection.GetReceiversAsync(cancellationToken);

    public async Task<Send<T>> InitTransferAsync(Guid targetClientId, int? batchSize = null)
    {
        if (batchSize is not null && _options.ByteStreamBatchBytes is not null)
        {
            throw new ArgumentException("A batch size cannot be given in the byte-stream mode.", nameof(batchSize));
        }

        int size = batchSize ?? _defaultBatchSize;
        _logger.LogInformation("Initiating Transfer to ReceiverId={TargetId} (Size={Size})...", targetClientId, size);

        var (transferId, affinity) = await _connection.OpenTransferAsync(targetClientId, size);
        return await _signaling.StartRequestedSendAsync(transferId, size, targetClientId, affinity);
    }

    public IReadOnlyList<TransferInfo> GetTransfers() => _connection.GetTransfers();

    public async ValueTask DisposeAsync()
    {
        await _connection.DisposeAsync();
        GC.SuppressFinalize(this);
    }
    
    // --- Internal Logic ---

    /// <summary>
    /// A session with the sending and receiving ends the client builds for it. Building them is
    /// where a client that cannot carry its item type is refused.
    /// </summary>
    private TransferSession<T> NewSession(string transferId, int batchSize, string? affinity)
    {
        var (send, receive) = SessionPorts.Create(_options, _serializer, _streamManager, batchSize);
        var session = new DataSession(this, transferId, batchSize, _dataUrl, affinity, send.Source, receive.OpenSink);
        return new TransferSession<T>(session, send, receive);
    }

    /// <summary>
    /// <c>null</c> in the byte-stream mode (<see cref="ClientOptions.ByteStreamBatchBytes"/>), where
    /// the payload crosses the wire as it is.
    /// </summary>
    internal IBatchSerializer<T>? Serializer => _serializer;

    #region ISessionContext Implementation

    ILogger ISessionConfig.Logger => _logger;
    HttpClient IStreamTransport.HttpClient => _connection.HttpClient;
    ClientOptions ISessionConfig.Options => _options;

    void IControlPlane.RemoveSession(string transferId)
    {
        _sessionRegistry.TryRemove(transferId, out _);
    }

    Task IControlPlane.ReportTransferAsync(string transferId, string? affinity, long items, long acks, CancellationToken cancellationToken)
        => _connection.ReportTransferAsync(transferId, affinity, items, acks, cancellationToken);

    Task<bool> IControlPlane.ProbeAsync(string transferId, string? affinity, CancellationToken cancellationToken)
        => _connection.ProbeAsync(transferId, affinity, cancellationToken);

    Task<string?> IStreamTransport.GetAccessTokenAsync(bool forceRefresh)
    {
        return _connection.Channel.GetAccessTokenAsync(forceRefresh);
    }

    #endregion
}
