using TransportR.Interfaces;
using TransportR.Protocol;
using TransportR.Hub.SignalR.Access;
using TransportR.Hub.SignalR.Services;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Microsoft.AspNetCore.Server.Kestrel.Core.Features;


namespace TransportR.Hub.SignalR.Controllers;

[ApiController]
[Route(WireRoutes.StreamBase)]
public class StreamController : ControllerBase
{
    private readonly ITransportRServer _brokerServer;
    private readonly ITransferManager _transferManager;
    private readonly IStateStore _stateStore;
    private readonly IClientIdentityProvider _identityProvider;
    private readonly IBridgeManager _bridgeManager;
    private readonly AccessPolicy _policy;
    private readonly ILogger<StreamController> _logger;

    public StreamController(
        ITransportRServer brokerServer,
        ITransferManager transferManager,
        IStateStore stateStore,
        IClientIdentityProvider identityProvider,
        IBridgeManager bridgeManager,
        AccessPolicy policy,
        ILogger<StreamController> logger)
    {
        _brokerServer = brokerServer;
        _transferManager = transferManager;
        _stateStore = stateStore;
        _identityProvider = identityProvider;
        _bridgeManager = bridgeManager;
        _policy = policy;
        _logger = logger;
    }

    /// <summary>
    /// Validates that the transfer exists and that the caller is the participant this stream
    /// belongs to. Every rejection is a 404: the client already treats NotFound as a terminal
    /// "transfer gone", and a 403 would turn the route into an existence oracle for transfer ids.
    /// </summary>
    private async Task<bool> ValidateTransfer(string transferId, ConnectionType type, bool isWrite)
    {
        var participants = await _transferManager.GetParticipantsAsync(transferId);
        if (participants == null)
        {
            _logger.LogWarning("Security: Stream access denied - transfer {TransferId} not found", transferId);
            return false;
        }

        // Dev mode arms no authorization on this route, so there is no attested identity to compare
        // and the data plane stays open. SecurityArchitecture.md states that of dev mode.
        if (User?.Identity?.IsAuthenticated != true)
        {
            return true;
        }

        // A participant holds two streams in opposite directions: the sender writes Data and reads
        // Control, the receiver reads Data and writes Control. So the role is the method crossed
        // with the connection type, not the method alone. The header is not trusted here — it only
        // selects which participant the caller is then required to prove it is.
        var isSenderStream = isWrite ^ (type == ConnectionType.Control);
        var expectedId = isSenderStream ? participants.SenderId : participants.ReceiverId;

        var expected = await _stateStore.GetClientAsync(expectedId);
        var caller = _identityProvider.ResolveIdentity(User, providedClientId: null, providedGroups: null);
        var callerSubject = caller?.Subject;

        // Past this route an authenticated caller must have been attested: a registration or a token
        // without a subject is refused here even though two absent subjects are equal elsewhere.
        // Lifetime is the host's middleware's call, so only a token that grants nothing is refused.
        if (expected?.Subject == null
            || caller == null
            || callerSubject == null
            || _policy.Evaluate(caller) == AccessVerdict.NoGroups
            || AccessPolicy.SameIdentity(expected.Subject, callerSubject) != AccessVerdict.Allowed)
        {
            _logger.LogWarning(
                "Security: Stream access denied - caller '{CallerSubject}' is not the {Role} of transfer {TransferId} (expected '{ExpectedSubject}')",
                callerSubject ?? "<none>", isSenderStream ? "sender" : "receiver", transferId, expected?.Subject ?? "<none>");
            return false;
        }

        return true;
    }

    [HttpPost("{transferId}/send")]
    [DisableRequestSizeLimit]
    public async Task Send(string transferId)
    {
        var type = ParseConnectionType(Request);

        if (!await ValidateTransfer(transferId, type, isWrite: true))
        {
            Response.StatusCode = 404;
            return;
        }

        DisableKestrelLimits();
        try
        {
            await _brokerServer.HandleSendAsync(transferId, type, Request.Body, HttpContext.RequestAborted);
        }
        catch (OperationCanceledException) { }
        catch (Exception ex)
        {
            // An IOException is a peer's connection failing, which the server has already reported as
            // a warning; anything else is a fault of the hub and keeps its trace.
            if (ex is not IOException)
            {
                _logger.LogError(ex, "Error in StreamController.Send (Transfer: {TransferId})", transferId);
            }

            // A status written while the client is still sending its body reaches it late or not at
            // all over HTTP/1.1; a connection that closes is what it reliably reacts to.
            HttpContext.Abort();
        }
    }

    [HttpGet("{transferId}/receive")]
    public async Task Receive(string transferId)
    {
        var type = ParseConnectionType(Request);

        if (!await ValidateTransfer(transferId, type, isWrite: false))
        {
            Response.StatusCode = 404;
            return;
        }

        // A source that finished its data stream has nothing more to send, and its bridge says so to
        // every sink that comes. Answered with an empty 200, a sink that lost the end of the stream
        // takes it for a stream that broke and reopens it, without end.
        if (type == ConnectionType.Data && DataStreamFinished(transferId))
        {
            Response.StatusCode = 410;
            return;
        }

        DisableKestrelLimits();
        Response.StatusCode = 200;
        Response.ContentType = "application/octet-stream";
        await Response.StartAsync();

        try
        {
            await _brokerServer.HandleReceiveAsync(transferId, type, Response.Body, HttpContext.RequestAborted);
        }
        catch (OperationCanceledException) { }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error in StreamController.Receive (Transfer: {TransferId})", transferId);
        }
    }
    
    private bool DataStreamFinished(string transferId) =>
        _bridgeManager.Bridges.TryGetValue(IBridgeManager.KeyFor(transferId, ConnectionType.Data), out var bridge)
        && bridge.CompletionTask.IsCompletedSuccessfully;

    private ConnectionType ParseConnectionType(HttpRequest request)
    {
        var connectionTypeString = request.Headers["X-Connection-Type"].ToString();
        return Enum.TryParse<ConnectionType>(connectionTypeString, true, out var type) ? type : ConnectionType.Data;
    }

    /// <summary>
    /// Disables Kestrel's minimum data rate limits for this request.
    /// Streaming connections can be paused or slow, which would otherwise trigger Kestrel's Slowloris protection
    /// and disconnect the client. We explicitly disable this protection for data streams.
    /// </summary>
    private void DisableKestrelLimits()
    {
        var minRequestBodyDataRateFeature = HttpContext.Features.Get<IHttpMinRequestBodyDataRateFeature>();
        if (minRequestBodyDataRateFeature != null)
        {
            minRequestBodyDataRateFeature.MinDataRate = null;
        }

        var minResponseDataRateFeature = HttpContext.Features.Get<IHttpMinResponseDataRateFeature>();
        if (minResponseDataRateFeature != null)
        {
            minResponseDataRateFeature.MinDataRate = null;
            _logger.LogDebug("[StreamController] MinResponseDataRate Disabled for Request {TraceIdentifier}", HttpContext.TraceIdentifier);
        }
        else
        {
            // If the feature is not found, Kestrel limits might not be active (e.g. running behind IIS or different server)
            _logger.LogDebug("[StreamController] IHttpMinResponseDataRateFeature NOT FOUND for Request {TraceIdentifier}", HttpContext.TraceIdentifier);
        }
    }
}
