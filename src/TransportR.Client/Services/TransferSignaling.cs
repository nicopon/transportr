using System.Text.Json;
using Microsoft.Extensions.Logging;
using TransportR.Abstractions;
using TransportR.Client.Internal;
using TransportR.Client.Sessions;
using TransportR.Protocol;

namespace TransportR.Client.Services;

/// <summary>
/// Handles the events of the hub's stream, and coordinates session lifecycle (creation/removal)
/// based on them.
/// </summary>
internal class TransferSignaling<T>
{
    private readonly ControlChannel _channel;
    private readonly SessionRegistry<T> _sessionRegistry;
    private readonly ISessionContext<T> _context;
    private readonly ILogger _logger;
    private readonly int _defaultBatchSize;
    private readonly string _dataUrl;
    private readonly Func<Receive<T>, Task>? _onTransferStarted;
    private readonly Func<Func<Send<T>, Task>?> _sendRequestedHandler;

    /// <param name="sendRequestedHandler">
    /// Returns the handler currently attached for transfers the host opens, or null when there is
    /// none. Asked on every request, because the answer decides whether the transfer is refused
    /// before a session exists.
    /// </param>
    public TransferSignaling(
        ControlChannel channel,
        SessionRegistry<T> sessionRegistry,
        ISessionContext<T> context,
        ILogger logger,
        int defaultBatchSize,
        string dataUrl,
        Func<Receive<T>, Task>? onTransferStarted,
        Func<Func<Send<T>, Task>?> sendRequestedHandler)
    {
        _sendRequestedHandler = sendRequestedHandler;
        _channel = channel;
        _sessionRegistry = sessionRegistry;
        _context = context;
        _logger = logger;
        _defaultBatchSize = defaultBatchSize;
        _dataUrl = dataUrl;
        _onTransferStarted = onTransferStarted;
    }

    public void Initialize()
    {
        _channel.EventReceived = OnEventAsync;
        _channel.Reopened = OnReopenedAsync;
    }

    private Task OnEventAsync(string eventType, string data)
    {
        switch (eventType)
        {
            case ControlPlane.ReadyToReceive:
                var ready = Read<ReadyToReceiveEvent>(data);
                _logger.LogInformation("Client: Received GetReadyToReceive request for Transfer={TransferId} from SenderId={SenderId}", ready.TransferId, ready.SenderId);
                return HandleGetReadyToReceive(ready);

            case ControlPlane.RequestSend:
                var send = Read<RequestSendEvent>(data);
                _logger.LogInformation("Client: Received RequestSend request for Transfer={TransferId} to ReceiverId={ReceiverId}", send.TransferId, send.ReceiverId);
                return HandleRequestSend(send);

            case ControlPlane.CancelTransfer:
                var cancel = Read<CancelTransferEvent>(data);
                _logger.LogInformation("Client: Received CancelTransfer request for {TransferId}", cancel.TransferId);
                return HandleCancelTransfer(cancel.TransferId);

            default:
                _logger.LogDebug("Client: Ignoring an event of the hub it does not know: {EventType}", eventType);
                return Task.CompletedTask;
        }
    }

    private Task OnReopenedAsync()
    {
        _logger.LogInformation("The control stream is back. Triggering stream restart for {Count} active sessions.", _sessionRegistry.Count);

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

        return Task.CompletedTask;
    }

    private async Task HandleGetReadyToReceive(ReadyToReceiveEvent ready)
    {
        _logger.LogDebug("Starting receive session: TransferId={TransferId}, SenderId={SenderId}", ready.TransferId, ready.SenderId);

        // The handshake runs on the instance holding this client's stream, which is where the bridge
        // will be: the session takes the client's own affinity as it is now.
        var affinity = _channel.CookieHeader();
        CallbackOutcome outcome;
        try
        {
            var session = _sessionRegistry.GetOrAdd(ready.TransferId, id => new DataSession<T>(_context, id, _defaultBatchSize, _dataUrl, affinity));
            await session.StartReceiverAsync();

            var transfer = new HttpReceive<T>(session);
            if (_onTransferStarted != null)
            {
                await _onTransferStarted(transfer);
            }

            outcome = CallbackOutcome.Accepted;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Could not take Transfer {TransferId}", ready.TransferId);
            outcome = CallbackOutcome.Failed;
        }

        await ReplyAsync(ready.RequestId, ready.TransferId, outcome, affinity);
    }

    /// <summary>
    /// The send session of a transfer this client asked for, started from the id the hub returned:
    /// the hub never calls the sender back during that handshake.
    /// </summary>
    /// <param name="affinity">What reached the instance that ran the handshake, and holds the bridge.</param>
    public async Task<IDataSession<T>> StartRequestedSendAsync(string transferId, int size, Guid receiverId, string? affinity)
    {
        _logger.LogDebug("Starting send session: TransferId={TransferId}, ReceiverId={ReceiverId}, BatchSize={Size}", transferId, receiverId, size);

        if (_sessionRegistry.ContainsKey(transferId))
        {
            throw new InvalidOperationException($"Session {transferId} already exists. Cannot overwrite.");
        }

        var session = _sessionRegistry.GetOrAdd(transferId, id => new DataSession<T>(_context, id, size, _dataUrl, affinity));
        await session.StartSenderAsync();
        return session;
    }

    private async Task HandleRequestSend(RequestSendEvent request)
    {
        var handler = _sendRequestedHandler();
        if (handler == null)
        {
            _logger.LogWarning("Refusing Transfer {TransferId}: it was opened by the host, and no OnSendRequested handler is attached.", request.TransferId);
            await ReplyAsync(request.RequestId, request.TransferId, CallbackOutcome.Refused, _channel.CookieHeader());
            return;
        }

        // The host opens a transfer only between two clients whose streams its instance holds.
        var affinity = _channel.CookieHeader();
        CallbackOutcome outcome;
        try
        {
            outcome = await StartHostRequestedSendAsync(request, handler, affinity);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Could not take Transfer {TransferId}", request.TransferId);
            outcome = CallbackOutcome.Failed;
        }

        await ReplyAsync(request.RequestId, request.TransferId, outcome, affinity);
    }

    private async Task<CallbackOutcome> StartHostRequestedSendAsync(RequestSendEvent request, Func<Send<T>, Task> handler, string? affinity)
    {
        if (_sessionRegistry.ContainsKey(request.TransferId))
        {
            throw new InvalidOperationException($"Session {request.TransferId} already exists. Cannot overwrite.");
        }

        _logger.LogDebug("Starting requested send session: TransferId={TransferId}, ReceiverId={ReceiverId}, BatchSize={Size}", request.TransferId, request.ReceiverId, request.BatchSize);

        var session = _sessionRegistry.GetOrAdd(request.TransferId, id => new DataSession<T>(_context, id, request.BatchSize, _dataUrl, affinity));
        await session.StartSenderAsync();

        var transfer = new HttpSend<T>(session);
        try
        {
            await handler(transfer);
        }
        catch
        {
            // The hub abandons the transfer on this failure and cancels the receiver. The handle's
            // owner is the handler that just failed, so nothing else would release this side;
            // should the handler dispose it later, a second dispose does nothing.
            await transfer.DisposeAsync();
            throw;
        }

        return CallbackOutcome.Accepted;
    }

    private async Task ReplyAsync(Guid requestId, string transferId, CallbackOutcome outcome, string? affinity)
    {
        try
        {
            using var response = await _channel.SendAsync(HttpMethod.Post, $"{WireRoutes.RepliesSegment}/{requestId}",
                new CallbackReply(outcome), affinity, CancellationToken.None);
            response.EnsureSuccessStatusCode();
        }
        catch (Exception ex)
        {
            // The hub's handshake waits out its timeout and abandons the transfer.
            _logger.LogWarning("Could not answer the hub about Transfer {TransferId}: {Message}", transferId, ex.Message);
        }
    }

    private async Task HandleCancelTransfer(string transferId)
    {
        if (_sessionRegistry.TryRemove(transferId, out var session) && session != null)
        {
            _logger.LogWarning("Cancelling Transfer {TransferId} as requested by Hub.", transferId);
            await session.EndByHubAsync();
        }
    }

    private static TEvent Read<TEvent>(string data) =>
        JsonSerializer.Deserialize<TEvent>(data, ControlPlane.Json)
        ?? throw new JsonException($"The hub sent an empty {typeof(TEvent).Name}.");
}
