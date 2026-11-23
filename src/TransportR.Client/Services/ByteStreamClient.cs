using Microsoft.Extensions.Logging;
using Microsoft.IO;
using TransportR.Client.Batching;
using TransportR.Client.Configuration;
using TransportR.Client.Internal;
using TransportR.Client.Services;
using TransportR.Client.Sessions;

namespace TransportR.Client;

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
public sealed class ByteStreamClient : IDataClient, IAsyncDisposable
{
    // The batch size its sessions carry: a byte client cuts its batches by size, and has no count.
    private const int RequestedBatchSize = 1000;

    /// <inheritdoc />
    public Guid ClientId => _connection.ClientId;

    /// <summary>The client's control plane. Internal so that the tests can reach the hub through it.</summary>
    internal ControlChannel Channel => _connection.Channel;

    /// <summary>What the client's sessions are given as their context. Internal so that the tests can reach it.</summary>
    internal ClientConnection Connection => _connection;

    private readonly ClientConnection _connection;
    private readonly ILogger _logger;
    private readonly ClientOptions _options;
    private readonly int _batchBytes;
    private readonly string _dataUrl;
    private readonly TransferSignaling<ByteClientSession, ByteSend, ByteReceive> _signaling;

    private Func<ByteReceive, Task>? _onTransferStarted;
    private Func<ByteSend, Task>? _onSendRequested;

    // Internal so that only the builder instantiates it.
    internal ByteStreamClient(
        ControlChannel channel,
        SessionRegistry<ByteClientSession> sessionRegistry,
        ILogger logger,
        int batchBytes,
        ClientOptions options,
        string dataUrl,
        HttpClient httpClient)
    {
        _connection = new ClientConnection(channel, logger, options, httpClient, dataUrl, sessionRegistry, () => sessionRegistry.Sessions,
            transferId => sessionRegistry.TryRemove(transferId, out _));
        _logger = logger;
        _options = options;
        _batchBytes = batchBytes;
        _dataUrl = dataUrl;

        _signaling = new TransferSignaling<ByteClientSession, ByteSend, ByteReceive>(
            channel,
            sessionRegistry,
            NewSession,
            session => new HttpByteSend(session.Session, session.Send.SendBytesAsync),
            session => new HttpByteReceive(session.Session, session.Receive.ReadAllAsync),
            logger,
            RequestedBatchSize,
            transfer => Volatile.Read(ref _onTransferStarted)?.Invoke(transfer) ?? Task.CompletedTask,
            () => Volatile.Read(ref _onSendRequested));
        _signaling.Initialize();
    }

    /// <summary>
    /// Registers the handler called when a new transfer is initiated targeting this receiver.
    /// </summary>
    /// <param name="handler">The handler reading from each transfer handle.</param>
    /// <remarks>
    /// Only one handler may be registered per client. Calling this method more than once throws
    /// <see cref="InvalidOperationException"/>. The handler owns the handle and must dispose it;
    /// what a failure of the handler does, before it returns or after, is as for
    /// <see cref="IDataReceiver{T}.OnTransferStarted"/>.
    /// </remarks>
    /// <exception cref="ArgumentNullException"><paramref name="handler"/> is <c>null</c>.</exception>
    /// <exception cref="InvalidOperationException">A handler is already registered.</exception>
    public void OnTransferStarted(Func<ByteReceive, Task> handler)
    {
        ArgumentNullException.ThrowIfNull(handler);
        if (Interlocked.CompareExchange(ref _onTransferStarted, handler, null) is not null)
        {
            throw new InvalidOperationException("A handler is already registered for OnTransferStarted.");
        }
    }

    /// <summary>
    /// Registers the handler called when the application hosting the hub opens a transfer from this
    /// client, rather than this client calling <see cref="InitTransferAsync"/>.
    /// </summary>
    /// <param name="handler">The handler sending through each transfer handle.</param>
    /// <remarks>
    /// Only one handler may be registered per client. Calling this method more than once throws
    /// <see cref="InvalidOperationException"/>. The handler owns the handle, as with
    /// <see cref="OnTransferStarted"/>: it sends through it, completes it and disposes it; what a
    /// failure of the handler does, before it returns or after, is as for
    /// <see cref="IDataSender{T}.OnSendRequested"/>. With no handler attached the client refuses such
    /// transfers, and the host's request fails.
    /// </remarks>
    /// <exception cref="ArgumentNullException"><paramref name="handler"/> is <c>null</c>.</exception>
    /// <exception cref="InvalidOperationException">A handler is already registered.</exception>
    public void OnSendRequested(Func<ByteSend, Task> handler)
    {
        ArgumentNullException.ThrowIfNull(handler);
        if (Interlocked.CompareExchange(ref _onSendRequested, handler, null) is not null)
        {
            throw new InvalidOperationException("A handler is already registered for OnSendRequested.");
        }
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

    /// <inheritdoc cref="IDataSender{T}.GetReceiversAsync(string, CancellationToken)"/>
    public Task<List<ClientInfo>> GetReceiversAsync(string group, CancellationToken cancellationToken = default)
        => _connection.GetReceiversAsync(group, cancellationToken);

    /// <summary>
    /// Initiates a new transfer to the specified target client.
    /// </summary>
    /// <param name="targetClientId">The ID of the receiver client.</param>
    /// <param name="cancellationToken">Token to cancel the transfer initiation.</param>
    /// <returns>A handle to write the bytes through.</returns>
    /// <exception cref="HubRequestFailedException">
    /// The hub refused the transfer, or the handshake failed: the receiver is unknown, unreachable or
    /// refused it, or it timed out.
    /// </exception>
    public async Task<ByteSend> InitTransferAsync(Guid targetClientId, CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Initiating Transfer to ReceiverId={TargetId} (BatchBytes={BatchBytes})...", targetClientId, _batchBytes);

        var (transferId, affinity) = await _connection.OpenTransferAsync(targetClientId, cancellationToken);
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

    /// <summary>
    /// A session with the ends the client builds for it, and the stream manager that serves its batches:
    /// the session holds the only reference, so the memory it pooled goes with it.
    /// </summary>
    private ByteClientSession NewSession(string transferId, int batchSize, string? affinity, TransferParties parties)
    {
        var send = new ByteBatchSource(_batchBytes, _options.BatchFlushIntervalMs, new RecyclableMemoryStreamManager());
        var receive = new ByteReceivePort(_options.ReceiveBufferCapacity);
        var session = new DataSession(_connection, transferId, batchSize, _dataUrl, affinity, parties, send, receive.OpenSink);
        return new ByteClientSession(session, send, receive);
    }
}
