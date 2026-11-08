using Microsoft.Extensions.Logging;
using Microsoft.IO;
using TransportR.Abstractions;
using TransportR.Client.Batching;
using TransportR.Client.Configuration;
using TransportR.Client.Internal;
using TransportR.Client.Sessions;
using TransportR.Interfaces;
using TransportR.Models;

namespace TransportR.Client.Services;

/// <summary>
/// Client for sending and receiving a stream of bytes: the transfer carries the bytes as they are, with
/// no serializer between the caller and the wire.
/// </summary>
/// <remarks>
/// <para>
/// No boundary survives: neither the one between two calls of the sender nor the one between two
/// batches. Both ends of a transfer must be byte-stream clients, and a receiver that is not refuses the
/// transfer, which ends it on both sides rather than delivering bytes it cannot read. Nothing is
/// compressed.
/// </para>
/// <para>
/// Acts as a coordinator between ClientConnection, SessionRegistry, and DataSessions, as
/// <see cref="TransportRClient{T}"/> does for items.
/// </para>
/// </remarks>
public sealed class ByteStreamClient : IDataClient, ISessionContext, IAsyncDisposable
{
    // What the hub's transfer request still names: a byte client cuts its batches by size, and has no count.
    private const int RequestedBatchSize = 1000;

    /// <inheritdoc />
    public Guid ClientId => _connection.ClientId;

    /// <summary>The client's control plane. Internal so that the tests can reach the hub through it.</summary>
    internal ControlChannel Channel => _connection.Channel;

    private readonly ClientConnection _connection;
    private readonly SessionRegistry<ByteTransferSession> _sessionRegistry;
    private readonly ILogger _logger;
    private readonly ClientOptions _options;
    private readonly int _batchBytes;
    private readonly string _dataUrl;
    private readonly RecyclableMemoryStreamManager _streamManager;
    private readonly TransferSignaling<ByteTransferSession, ByteSend, ByteReceive> _signaling;

    /// <summary>
    /// Event fired when a new transfer is initiated targeting this receiver.
    /// </summary>
    /// <remarks>
    /// The handler reads the transfer from the handle it is given and owns it: nothing else disposes it,
    /// and a handle left alive keeps its session registered with the client and reporting to the hub for
    /// as long as the client lives.
    /// </remarks>
    public event Func<ByteReceive, Task>? OnTransferStarted;

    /// <summary>
    /// Event fired when the application hosting the hub opens a transfer from this client, rather
    /// than this client calling <see cref="InitTransferAsync"/>.
    /// </summary>
    /// <remarks>
    /// The handler owns the handle it is given, as with <see cref="OnTransferStarted"/>: it sends through
    /// it, completes it and disposes it. It runs inside the handshake, which is bounded by a
    /// timeout, so it hands the handle off and returns rather than sending from within. With no
    /// handler attached the client refuses such transfers, and the host's request fails.
    /// </remarks>
    public event Func<ByteSend, Task>? OnSendRequested;

    // Internal so that only the builder instantiates it.
    internal ByteStreamClient(
        ControlChannel channel,
        SessionRegistry<ByteTransferSession> sessionRegistry,
        ILogger logger,
        int batchBytes,
        ClientOptions options,
        string dataUrl,
        HttpClient httpClient,
        RecyclableMemoryStreamManager streamManager)
    {
        _connection = new ClientConnection(channel, logger, options, httpClient, sessionRegistry, () => sessionRegistry.Sessions);
        _sessionRegistry = sessionRegistry;
        _logger = logger;
        _options = options;
        _batchBytes = batchBytes;
        _dataUrl = dataUrl;
        _streamManager = streamManager;

        _signaling = new TransferSignaling<ByteTransferSession, ByteSend, ByteReceive>(
            channel,
            sessionRegistry,
            NewSession,
            session => new HttpByteSend(session.Session, session.Send.SendBytesAsync),
            session => new HttpByteReceive(session.Session, session.Receive.ReadAllAsync),
            logger,
            RequestedBatchSize,
            transfer => OnTransferStarted?.Invoke(transfer) ?? Task.CompletedTask,
            () => OnSendRequested);
        _signaling.Initialize();
    }

    /// <inheritdoc />
    public Task ConnectAsync(IEnumerable<string>? groups = null, CancellationToken cancellationToken = default)
        => _connection.ConnectAsync(groups, cancellationToken);

    /// <inheritdoc cref="TransportRClient{T}.RefreshRegistrationAsync"/>
    public Task RefreshRegistrationAsync(CancellationToken cancellationToken = default)
        => _connection.RefreshRegistrationAsync(cancellationToken);

    /// <inheritdoc cref="TransportRClient{T}.IsRegisteredAsync"/>
    public Task<bool> IsRegisteredAsync(CancellationToken cancellationToken = default)
        => _connection.IsRegisteredAsync(cancellationToken);

    /// <inheritdoc />
    public Task DisconnectAsync() => _connection.DisconnectAsync();

    /// <summary>
    /// Gets a list of receivers that this client is authorized to send data to.
    /// </summary>
    /// <param name="cancellationToken">Token to cancel the request.</param>
    /// <returns>List of authorized receiver clients.</returns>
    /// <exception cref="HubRequestFailedException">The hub refused the request.</exception>
    public Task<List<ClientInfo>> GetReceiversAsync(CancellationToken cancellationToken = default)
        => _connection.GetReceiversAsync(cancellationToken);

    /// <summary>
    /// Initiates a new transfer to the specified target client.
    /// </summary>
    /// <param name="targetClientId">The ID of the receiver client.</param>
    /// <returns>A handle to write the bytes through.</returns>
    /// <exception cref="HubRequestFailedException">
    /// The hub refused the transfer, or the handshake failed: the receiver is unknown, unreachable or
    /// refused it, or it timed out.
    /// </exception>
    public async Task<ByteSend> InitTransferAsync(Guid targetClientId)
    {
        _logger.LogInformation("Initiating Transfer to ReceiverId={TargetId} (BatchBytes={BatchBytes})...", targetClientId, _batchBytes);

        var (transferId, affinity) = await _connection.OpenTransferAsync(targetClientId, RequestedBatchSize);
        return await _signaling.StartRequestedSendAsync(transferId, RequestedBatchSize, targetClientId, affinity);
    }

    /// <inheritdoc />
    public IReadOnlyList<TransferInfo> GetTransfers() => _connection.GetTransfers();

    public async ValueTask DisposeAsync()
    {
        await _connection.DisposeAsync();
        GC.SuppressFinalize(this);
    }

    // --- Internal Logic ---

    private ByteTransferSession NewSession(string transferId, int batchSize, string? affinity)
    {
        var send = new ByteBatchSource(_batchBytes, _options.BatchFlushIntervalMs, _streamManager);
        var receive = new ByteReceivePort(_options.ReceiveBufferCapacity);
        var session = new DataSession(this, transferId, batchSize, _dataUrl, affinity, send, receive.OpenSink);
        return new ByteTransferSession(session, send, receive);
    }

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
