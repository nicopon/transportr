using System.Text.Json;
using Microsoft.Extensions.Logging;
using TransportR.Client.Batching;
using TransportR.Client.Internal;
using TransportR.Client.Sessions;
using TransportR.Protocol;

namespace TransportR.Client.Services;

/// <summary>
/// Handles the events of the hub's stream, and coordinates session lifecycle (creation/removal)
/// based on them.
/// </summary>
/// <remarks>
/// Knows neither what a transfer carries nor how its ends are built: the client makes the sessions and
/// the handles that go with them.
/// </remarks>
/// <typeparam name="TSession">The session the client keeps, with the ends it builds for it.</typeparam>
/// <typeparam name="TSend">The handle a sender is given.</typeparam>
/// <typeparam name="TReceive">The handle a receiver is given.</typeparam>
internal class TransferSignaling<TSession, TSend, TReceive>
    where TSession : ClientSession
    where TSend : IAsyncDisposable
    where TReceive : IAsyncDisposable
{
    private readonly ControlChannel _channel;
    private readonly SessionRegistry<TSession> _sessionRegistry;
    private readonly Func<string, int, string?, TransferParties, TSession> _newSession;
    private readonly Func<TSession, TSend> _makeSend;
    private readonly Func<TSession, TReceive> _makeReceive;
    private readonly ILogger _logger;
    private readonly int _defaultBatchSize;
    private Func<TReceive, Task>? _onTransferStarted;
    private Func<TSend, Task>? _onSendRequested;

    /// <param name="newSession">
    /// Makes the session of a transfer, given its id, its batch size, the affinity that reached the
    /// instance holding its bridge and its two clients as this end holds them. Building the sending and
    /// receiving ends is where a client that cannot carry what it was asked to is refused.
    /// </param>
    /// <param name="makeSend">The handle of a session a sender starts.</param>
    /// <param name="makeReceive">The handle of a session a receiver starts.</param>
    public TransferSignaling(
        ControlChannel channel,
        SessionRegistry<TSession> sessionRegistry,
        Func<string, int, string?, TransferParties, TSession> newSession,
        Func<TSession, TSend> makeSend,
        Func<TSession, TReceive> makeReceive,
        ILogger logger,
        int defaultBatchSize)
    {
        _channel = channel;
        _sessionRegistry = sessionRegistry;
        _newSession = newSession;
        _makeSend = makeSend;
        _makeReceive = makeReceive;
        _logger = logger;
        _defaultBatchSize = defaultBatchSize;
    }

    public void SetTransferStartedHandler(Func<TReceive, Task> handler) =>
        SetSingleHandler(ref _onTransferStarted, handler, nameof(IDataReceiver<object>.OnTransferStarted));

    public void SetSendRequestedHandler(Func<TSend, Task> handler) =>
        SetSingleHandler(ref _onSendRequested, handler, nameof(IDataSender<object>.OnSendRequested));

    private static void SetSingleHandler<THandler>(ref THandler? slot, THandler handler, string methodName)
        where THandler : class
    {
        ArgumentNullException.ThrowIfNull(handler);
        if (Interlocked.CompareExchange(ref slot, handler, null) is not null)
        {
            throw new InvalidOperationException($"A handler is already registered for {methodName}.");
        }
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
            var parties = new TransferParties(ready.SenderId, _channel.ClientId);
            var session = _sessionRegistry.GetOrAdd(ready.TransferId, id => _newSession(id, _defaultBatchSize, affinity, parties));
            await session.Session.StartReceiverAsync();

            var transfer = _makeReceive(session);
            if (Volatile.Read(ref _onTransferStarted) is { } handler)
            {
                Watch(handler(transfer), transfer, ready.TransferId);
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
    public async Task<TSend> StartRequestedSendAsync(string transferId, int size, Guid receiverId, string? affinity)
    {
        _logger.LogDebug("Starting send session: TransferId={TransferId}, ReceiverId={ReceiverId}, BatchSize={Size}", transferId, receiverId, size);

        if (_sessionRegistry.ContainsKey(transferId))
        {
            throw new InvalidOperationException($"Session {transferId} already exists. Cannot overwrite.");
        }

        var parties = new TransferParties(_channel.ClientId, receiverId);
        var session = _sessionRegistry.GetOrAdd(transferId, id => _newSession(id, size, affinity, parties));
        await session.Session.StartSenderAsync();
        return _makeSend(session);
    }

    private async Task HandleRequestSend(RequestSendEvent request)
    {
        var handler = Volatile.Read(ref _onSendRequested);
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

    private async Task<CallbackOutcome> StartHostRequestedSendAsync(RequestSendEvent request, Func<TSend, Task> handler, string? affinity)
    {
        if (_sessionRegistry.ContainsKey(request.TransferId))
        {
            throw new InvalidOperationException($"Session {request.TransferId} already exists. Cannot overwrite.");
        }

        _logger.LogDebug("Starting requested send session: TransferId={TransferId}, ReceiverId={ReceiverId}", request.TransferId, request.ReceiverId);

        var parties = new TransferParties(_channel.ClientId, request.ReceiverId);
        var session = _sessionRegistry.GetOrAdd(request.TransferId, id => _newSession(id, _defaultBatchSize, affinity, parties));
        await session.Session.StartSenderAsync();

        var transfer = _makeSend(session);
        try
        {
            Watch(handler(transfer), transfer, request.TransferId);
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

    /// <summary>
    /// Reads the task a handler returned as it stands when the handler returns, which is when the
    /// hub is answered: a handler may consume or send in its body without holding the handshake.
    /// </summary>
    /// <remarks>
    /// A task already faulted or cancelled is raised here, and the transfer refused. A task still
    /// running cannot be refused any more if it fails or is cancelled later: the failure is logged and
    /// the handle released in the handler's place, which abandons a transfer that has not ended, and
    /// the peer fails on it. A transfer that has ended keeps its outcome, and releasing a handle the
    /// handler had released already does nothing.
    /// </remarks>
    private void Watch(Task handled, IAsyncDisposable handle, string transferId)
    {
        if (handled.IsCompleted)
        {
            // Raises the fault, or the cancellation as a TaskCanceledException.
            handled.GetAwaiter().GetResult();
            return;
        }

        _ = ReleaseOnFailureAsync(handled, handle, transferId);
    }

    private async Task ReleaseOnFailureAsync(Task handled, IAsyncDisposable handle, string transferId)
    {
        try
        {
            await handled;
            return;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "The handler of Transfer {TransferId} failed after it returned; releasing its handle", transferId);
        }

        // Nobody awaits this task: what the release raises would go unobserved.
        try
        {
            await handle.DisposeAsync();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Releasing the handle of Transfer {TransferId} failed", transferId);
        }
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
            await session.Session.EndByHubAsync();
        }
    }

    private static TEvent Read<TEvent>(string data) =>
        JsonSerializer.Deserialize<TEvent>(data, ControlPlane.Json)
        ?? throw new JsonException($"The hub sent an empty {typeof(TEvent).Name}.");
}
