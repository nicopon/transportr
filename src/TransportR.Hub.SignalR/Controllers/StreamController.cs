using TransportR.Interfaces;
using TransportR.Models;
using TransportR.Protocol;
using TransportR.Hub.SignalR.Access;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Microsoft.AspNetCore.Server.Kestrel.Core.Features;


namespace TransportR.Hub.SignalR.Controllers;

[ApiController]
[Route(WireRoutes.StreamBase)]
internal class StreamController : ControllerBase
{
    private readonly ITransportRServer _brokerServer;
    private readonly ITransferManager _transferManager;
    private readonly IStateStore _stateStore;
    private readonly IClientIdentityProvider _identityProvider;
    private readonly IBridgeManager _bridgeManager;
    private readonly AccessPolicy _policy;
    private readonly ITransferTerminator _terminator;
    private readonly ILogger<StreamController> _logger;

    public StreamController(
        ITransportRServer brokerServer,
        ITransferManager transferManager,
        IStateStore stateStore,
        IClientIdentityProvider identityProvider,
        IBridgeManager bridgeManager,
        AccessPolicy policy,
        ITransferTerminator terminator,
        ILogger<StreamController> logger)
    {
        _brokerServer = brokerServer;
        _transferManager = transferManager;
        _stateStore = stateStore;
        _identityProvider = identityProvider;
        _bridgeManager = bridgeManager;
        _policy = policy;
        _terminator = terminator;
        _logger = logger;
    }

    /// <summary>
    /// Validates that the transfer exists and that the caller is the participant this stream
    /// belongs to. Every rejection is a 404: the client already treats NotFound as a terminal
    /// "transfer gone", and a 403 would turn the route into an existence oracle for transfer ids.
    /// </summary>
    private Task<bool> ValidateTransfer(string transferId, ConnectionType type, bool isWrite)
    {
        // A participant holds two streams in opposite directions: the sender writes Data and reads
        // Control, the receiver reads Data and writes Control. So the role is the method crossed
        // with the connection type, not the method alone. The header is not trusted here - it only
        // selects which participant the caller is then required to prove it is.
        var isSenderStream = isWrite ^ (type == ConnectionType.Control);
        return Admit(transferId, isSenderStream ? Role.Sender : Role.Receiver);
    }

    private enum Role { Sender, Receiver, Either }

    private async Task<bool> Admit(string transferId, Role role)
    {
        var participants = await _transferManager.GetParticipantsAsync(transferId);
        if (participants == null)
        {
            // A client reopening a stream of a transfer the hub retired or ended meets this, twice per
            // reopening; it says nothing about the caller's right to be there, which is judged below.
            _logger.LogInformation("Stream request for transfer {TransferId} refused: the hub does not know it (finished, terminated or never opened)", transferId);
            return false;
        }

        // Dev mode arms no authorization on this route, so there is no attested identity to compare
        // and the data plane stays open. SecurityArchitecture.md states that of dev mode.
        if (User?.Identity?.IsAuthenticated != true)
        {
            return true;
        }

        var caller = _identityProvider.ResolveIdentity(User, providedClientId: null, providedGroups: null);
        var callerSubject = caller?.Subject;
        var roleName = role == Role.Either ? "sender or receiver" : role.ToString().ToLowerInvariant();

        // Past this route an authenticated caller must have been attested: a registration or a token
        // without a subject is refused here even though two absent subjects are equal elsewhere.
        // Lifetime is the host's middleware's call, so only a token that grants nothing is refused.
        // Each refusal says which of the three it is: a line that lists what was expected, empty, is
        // read as a subject mismatch for a comparison that never ran.
        if (caller == null || callerSubject == null)
        {
            _logger.LogWarning(
                "Security: Stream access denied - the caller of transfer {TransferId} has no attested identity to compare (the identity could not be resolved, or its token carries no subject)",
                transferId);
            return false;
        }

        if (_policy.Evaluate(caller) == AccessVerdict.NoGroups)
        {
            _logger.LogWarning(
                "Security: Stream access denied - caller '{CallerSubject}' holds no group, so it is not the {Role} of transfer {TransferId}",
                callerSubject, roleName, transferId);
            return false;
        }

        var expectedIds = role switch
        {
            Role.Sender => new[] { participants.SenderId },
            Role.Receiver => new[] { participants.ReceiverId },
            _ => new[] { participants.SenderId, participants.ReceiverId }
        };

        var expectedSubjects = new List<string>();
        foreach (var expectedId in expectedIds)
        {
            var expected = await _stateStore.GetClientAsync(expectedId);
            if (expected?.Subject != null
                && AccessPolicy.SameIdentity(expected.Subject, callerSubject) == AccessVerdict.Allowed)
            {
                return true;
            }

            expectedSubjects.Add($"'{expected?.Subject ?? "<none>"}'");
        }

        _logger.LogWarning(
            "Security: Stream access denied - caller '{CallerSubject}' is not the {Role} of transfer {TransferId} (expected {ExpectedSubjects})",
            callerSubject, roleName, transferId, string.Join(" or ", expectedSubjects));
        return false;
    }

    [HttpPost("{transferId}/send")]
    [DisableRequestSizeLimit]
    public async Task Send(string transferId)
    {
        if (!TryParseConnectionType(Request, transferId, out var type))
        {
            Response.StatusCode = 400;
            return;
        }

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

    /// <summary>
    /// Ends the transfer at a participant's request, the way a host terminating a run does: state and
    /// bridges removed, both participants told to cancel. This is for the sender whose Abort cannot
    /// reach the wire because the data stream in front of it is full: the in-stream signal is the one
    /// the design relies on, and this request is only what is left when the stream cannot carry it.
    /// </summary>
    [HttpPost("{transferId}/" + WireRoutes.AbortSegment)]
    public async Task<IActionResult> Abort(string transferId)
    {
        if (!await Admit(transferId, Role.Either))
        {
            return NotFound();
        }

        // Only the replica that carried the transfer's streams can end it: it holds the bridges, and the
        // participants' connections that are to be told are on it. Behind a proxy with affinity, a request
        // that lost its cookie reaches another replica, which knows the transfer from the shared state
        // alone; acting there would delete that state and tell nobody.
        if (!_bridgeManager.Bridges.ContainsKey(IBridgeManager.KeyFor(transferId, ConnectionType.Data))
            && !_bridgeManager.Bridges.ContainsKey(IBridgeManager.KeyFor(transferId, ConnectionType.Control)))
        {
            _logger.LogWarning("Abort request for transfer {TransferId} refused: this hub holds none of its bridges, so the request did not follow its streams", transferId);
            return NotFound();
        }

        _logger.LogInformation("Transfer {TransferId} ended at the request of one of its participants", transferId);
        await _terminator.TerminateAsync(transferId);
        return NoContent();
    }

    [HttpGet("{transferId}/receive")]
    public async Task Receive(string transferId)
    {
        if (!TryParseConnectionType(Request, transferId, out var type))
        {
            Response.StatusCode = 400;
            return;
        }

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
        && bridge.State == BridgeState.Completed;

    // Matched by name: Enum.TryParse also takes a number, defined or not, and a comma-separated list
    // that it folds into one value.
    private bool TryParseConnectionType(HttpRequest request, string transferId, out ConnectionType type)
    {
        var header = request.Headers["X-Connection-Type"].ToString();
        foreach (var candidate in Enum.GetValues<ConnectionType>())
        {
            if (string.Equals(candidate.ToString(), header, StringComparison.OrdinalIgnoreCase))
            {
                type = candidate;
                return true;
            }
        }

        _logger.LogWarning("Stream request for transfer {TransferId} refused: X-Connection-Type '{Header}' does not name a connection type", transferId, header);
        type = default;
        return false;
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
