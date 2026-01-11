using Microsoft.AspNetCore.SignalR.Client;
using Microsoft.Extensions.Logging;
using TransportR.Abstractions;
using TransportR.Client.SignalR.Internal;
using TransportR.Client.SignalR.Sessions;
using TransportR.Interfaces;

namespace TransportR.Client.SignalR.Services;

/// <summary>
/// Handles SignalR events and coordinating session lifecycle (creation/removal)
/// based on Hub signals.
/// </summary>
internal class SignalRTransferSignaling<T>
{
    private readonly HubConnectionManager _connectionManager;
    private readonly SessionRegistry<T> _sessionRegistry;
    private readonly ISessionContext _context;
    private readonly ILogger _logger;
    private readonly int _defaultBatchSize;
    private readonly string _dataUrl;
    private readonly Func<Receive<T>, Task>? _onTransferStarted;

    public SignalRTransferSignaling(
        HubConnectionManager connectionManager,
        SessionRegistry<T> sessionRegistry,
        ISessionContext context,
        ILogger logger,
        int defaultBatchSize,
        string dataUrl,
        Func<Receive<T>, Task>? onTransferStarted)
    {
        _connectionManager = connectionManager;
        _sessionRegistry = sessionRegistry;
        _context = context;
        _logger = logger;
        _defaultBatchSize = defaultBatchSize;
        _dataUrl = dataUrl;
        _onTransferStarted = onTransferStarted;
    }

    public void Initialize()
    {
        _connectionManager.Initialize(RegisterHubHandlers);
    }

    private void RegisterHubHandlers(HubConnection connection)
    {
        // Handler for GetReadyToReceive
        connection.On<string, Guid, bool>(nameof(ICommandClient.GetReadyToReceive), async (transferId, senderId) =>
        {
            _logger.LogInformation("Client: Received GetReadyToReceive request for Transfer={TransferId} from SenderId={SenderId}", transferId, senderId);
            return await HandleGetReadyToReceive(transferId, senderId);
        });

        // Handler for GetReadyToSend
        connection.On<string, int, Guid, bool>(nameof(ICommandClient.GetReadyToSend), async (transferId, size, receiverId) =>
        {
            _logger.LogInformation("Client: Received GetReadyToSend request for Transfer={TransferId} to ReceiverId={ReceiverId}", transferId, receiverId);
            return await HandleGetReadyToSend(transferId, size, receiverId);
        });

        connection.On<string>(nameof(ICommandClient.CancelTransfer), async (transferId) =>
        {
             _logger.LogInformation("Client: Received CancelTransfer request for {TransferId}", transferId);
             await HandleCancelTransfer(transferId);
        });

        // Handler for Pong
        connection.On<string>(nameof(ICommandClient.Pong), (transferId) =>
        {
            _logger.LogDebug("Client: Received Pong for {TransferId}", transferId);
            if (_sessionRegistry.TryGetSession(transferId, out var session) && session != null)
            {
                session.OnPongReceived();
            }
        });


        // Handler for SignalR reconnection
        connection.Reconnected += async (connectionId) =>
        {
            _logger.LogInformation("SignalR reconnected (ConnectionId={ConnectionId}). Triggering stream restart for {Count} active sessions.", 
                connectionId, _sessionRegistry.Count);
            
            foreach (var session in _sessionRegistry.Sessions)
            {
                try
                {
                    session.TriggerReconnect();
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Failed to trigger reconnect for session {TransferId}", session.TransferId);
                }
            }
            
            await Task.CompletedTask;
        };
    }

    private async Task<bool> HandleGetReadyToReceive(string transferId, Guid senderId)
    {
        _logger.LogDebug("Starting receive session: TransferId={TransferId}, SenderId={SenderId}", transferId, senderId);
        
        // Create session
        var session = _sessionRegistry.GetOrAdd(transferId, id => new DataSession<T>(_context, id, _defaultBatchSize, _dataUrl));
        await session.StartReceiverAsync();
        
        // HttpReceive is likely in TransportR.Client.SignalR.Internal
        var transfer = new HttpReceive<T>(session);
        if (_onTransferStarted != null)
        {
            await _onTransferStarted(transfer);
        }
        return true;
    }

    private async Task<bool> HandleGetReadyToSend(string transferId, int size, Guid receiverId)
    {
        _logger.LogDebug("Starting send session: TransferId={TransferId}, ReceiverId={ReceiverId}, BatchSize={Size}", transferId, receiverId, size);
        
        if (_sessionRegistry.ContainsKey(transferId))
        {
            throw new InvalidOperationException($"Session {transferId} already exists. Cannot overwrite.");
        }

        var session = _sessionRegistry.GetOrAdd(transferId, id => new DataSession<T>(_context, id, size, _dataUrl));
        await session.StartSenderAsync();
        return true;
    }

    private async Task HandleCancelTransfer(string transferId)
    {
        if (_sessionRegistry.TryRemove(transferId, out var session) && session != null)
        {
            _logger.LogWarning("Cancelling Transfer {TransferId} as requested by Hub.", transferId);
            await session.DisposeAsync();
        }
    }
}
