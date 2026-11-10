using System.Globalization;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Server.Kestrel.Core.Features;
using Microsoft.Extensions.Logging;
using TransportR.Hub.Access;
using TransportR.Hub.Streams;
using TransportR.Interfaces;
using TransportR.Models;
using TransportR.Protocol;


namespace TransportR.Hub.Controllers;

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
    private readonly UploadEpochs _epochs;
    private readonly TimeProvider _time;
    private readonly ILogger<StreamController> _logger;

    public StreamController(
        ITransportRServer brokerServer,
        ITransferManager transferManager,
        IStateStore stateStore,
        IClientIdentityProvider identityProvider,
        IBridgeManager bridgeManager,
        AccessPolicy policy,
        ITransferTerminator terminator,
        UploadEpochs epochs,
        TimeProvider time,
        ILogger<StreamController> logger)
    {
        _brokerServer = brokerServer;
        _transferManager = transferManager;
        _stateStore = stateStore;
        _identityProvider = identityProvider;
        _bridgeManager = bridgeManager;
        _policy = policy;
        _terminator = terminator;
        _epochs = epochs;
        _time = time;
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

    /// <summary>
    /// Takes one segment of a write stream (see <see cref="DataPlane"/>). Answered 200 once it reached
    /// the peer, 202 when it was held at the bound with part of it relayed, both with
    /// <see cref="DataPlane.RelayedHeader"/>; 409 when the hub holds no epoch it follows on from.
    /// </summary>
    [HttpPost("{transferId}/send")]
    [DisableRequestSizeLimit]
    public async Task Send(
        string transferId,
        [FromQuery(Name = DataPlane.EpochParameter)] Guid? epoch,
        [FromQuery(Name = DataPlane.OffsetParameter)] long? offset,
        [FromQuery(Name = DataPlane.FinalParameter)] bool final,
        [FromQuery(Name = DataPlane.HeartbeatParameter)] int? heartbeatMs)
    {
        if (!TryParseConnectionType(Request, transferId, out var type))
        {
            Response.StatusCode = 400;
            return;
        }

        var bridgeKey = IBridgeManager.KeyFor(transferId, type);
        if (epoch is not { } epochId || offset is not >= 0 || !_epochs.TryGet(bridgeKey, epochId, out var upload))
        {
            upload = await OpenEpochAsync(transferId, type, bridgeKey, epoch, offset, heartbeatMs);
            if (upload == null) return;
        }
        else if (AccessPolicy.SameIdentity(upload.Subject, AttestedSubject()) != AccessVerdict.Allowed)
        {
            // The opening was admitted; a segment only has to come from the identity that opened it.
            _logger.LogWarning("Security: Stream access denied - a segment of {BridgeKey} does not come from the identity that opened its stream", bridgeKey);
            this.Refuse();
            return;
        }

        DisableKestrelLimits();
        SegmentOutcome outcome;
        try
        {
            outcome = await upload.AcceptAsync(offset!.Value, Request.Body, final, HttpContext.RequestAborted);
        }
        catch (OperationCanceledException)
        {
            return;
        }
        catch (Exception ex) when (ex is IOException or BadHttpRequestException)
        {
            // The client's own connection failed under its segment; it sends it again or reopens.
            _logger.LogInformation("BRIDGE: A segment of {BridgeKey} broke off ({Message})", bridgeKey, ex.Message);
            return;
        }

        Response.Headers[DataPlane.RelayedHeader] = outcome.Relayed.ToString(CultureInfo.InvariantCulture);
        if (outcome.Status == SegmentStatus.Conflict)
        {
            _logger.LogInformation("BRIDGE: Segment of {BridgeKey} at offset {Offset} refused: its stream is over or the segment does not follow on from it", bridgeKey, offset);
        }

        Response.StatusCode = outcome.Status switch
        {
            SegmentStatus.Relayed => StatusCodes.Status200OK,
            SegmentStatus.Held => StatusCodes.Status202Accepted,
            _ => StatusCodes.Status409Conflict,
        };
    }

    /// <summary>
    /// Takes one opening of a write stream carried by a WebSocket (see <see cref="DataPlane"/>). The
    /// caller is admitted once, here; what becomes of its rights afterwards is the access sweep's, as for
    /// a read stream. A request that is not an upgrade is a front end that did not relay one: answered
    /// 426, it never opens anything, and the client goes on with segments.
    /// </summary>
    [HttpGet("{transferId}/send")]
    [MiddlewareFilter(typeof(WebSocketPipeline))]
    public async Task SendOverWebSocket(
        string transferId,
        [FromQuery(Name = DataPlane.HeartbeatParameter)] int? heartbeatMs)
    {
        if (!HttpContext.WebSockets.IsWebSocketRequest)
        {
            _logger.LogWarning("Write stream of transfer {TransferId} not opened over a WebSocket: the request reached the hub without an upgrade", transferId);
            Response.Headers.Upgrade = "websocket";
            Response.StatusCode = StatusCodes.Status426UpgradeRequired;
            return;
        }

        if (!TryParseConnectionType(Request, transferId, out var type))
        {
            Response.StatusCode = StatusCodes.Status400BadRequest;
            return;
        }

        if (!await ValidateTransfer(transferId, type, isWrite: true))
        {
            this.Refuse();
            return;
        }

        var bridgeKey = IBridgeManager.KeyFor(transferId, type);
        if (heartbeatMs is not > 0)
        {
            _logger.LogWarning("WebSocket of {BridgeKey} refused: its opening announces no heartbeat interval", bridgeKey);
            Response.StatusCode = StatusCodes.Status400BadRequest;
            return;
        }

        var heartbeat = TimeSpan.FromMilliseconds(heartbeatMs.Value);
        using var socket = await HttpContext.WebSockets.AcceptWebSocketAsync(new WebSocketAcceptContext { KeepAliveInterval = heartbeat });

        // Only the sender's silence is judged, for the reason given when an epoch opens.
        using var input = new WebSocketInput(socket, bridgeKey, type == ConnectionType.Data ? heartbeat : null, _time, _logger);
        if (!await input.AnswerProbeAsync(HttpContext.RequestAborted))
        {
            return;
        }

        input.JudgeSilence();
        if (!await input.WaitForFirstByteAsync(HttpContext.RequestAborted))
        {
            return;
        }

        _epochs.Abandon(bridgeKey);
        _logger.LogInformation("BRIDGE: The write stream of {BridgeKey} is carried by a WebSocket", bridgeKey);

        try
        {
            await _brokerServer.HandleSendAsync(transferId, type, input, HttpContext.RequestAborted);
        }
        catch (OperationCanceledException)
        {
        }
        catch (Exception ex)
        {
            // An IOException is a peer's connection failing, which the server has already reported as a
            // warning; anything else is a fault of the hub and keeps its trace.
            if (ex is not IOException)
            {
                _logger.LogError(ex, "Error in the write stream of {BridgeKey}", bridgeKey);
            }
        }

        await input.EndAsync(HttpContext.RequestAborted);
    }

    /// <summary>
    /// Opens the epoch a segment names when this hub holds none by that id: the transfer is checked
    /// and the caller admitted here, once per epoch. Returns null once the response is written.
    /// </summary>
    private async Task<UploadEpoch?> OpenEpochAsync(string transferId, ConnectionType type, string bridgeKey, Guid? epoch, long? offset, int? heartbeatMs)
    {
        if (!await ValidateTransfer(transferId, type, isWrite: true))
        {
            this.Refuse();
            return null;
        }

        if (epoch is not { } epochId || offset is not >= 0)
        {
            _logger.LogWarning("Segment of {BridgeKey} refused: it names no epoch or no offset", bridgeKey);
            Response.StatusCode = StatusCodes.Status400BadRequest;
            return null;
        }

        // A 404 would read as the transfer gone: the client reopens on a 409 instead.
        if (offset != 0)
        {
            _logger.LogInformation("BRIDGE: Segment of {BridgeKey} at offset {Offset} refused: this hub holds no stream {Epoch}", bridgeKey, offset, epochId);
            Response.StatusCode = StatusCodes.Status409Conflict;
            return null;
        }

        // Only the sender's silence is judged: a dead sender leaves an Active bridge the reaper skips,
        // while the receiver's heartbeat keeps the transfer alive. The receiver writes what its read
        // loop gives it, which a consumer holding on to a batch parks, so its silence says nothing; a
        // dead receiver shows at its data GET, and the sender then fails for want of acknowledgements.
        TimeSpan? heartbeat = null;
        if (type == ConnectionType.Data)
        {
            if (heartbeatMs is not > 0)
            {
                _logger.LogWarning("Segment of {BridgeKey} refused: the opening of its stream announces no heartbeat interval", bridgeKey);
                Response.StatusCode = StatusCodes.Status400BadRequest;
                return null;
            }

            heartbeat = TimeSpan.FromMilliseconds(heartbeatMs.Value);
        }

        return _epochs.Open(
            bridgeKey,
            epochId,
            AttestedSubject(),
            heartbeat,
            (input, token) => _brokerServer.HandleSendAsync(transferId, type, input, token));
    }

    // Dev mode attests nothing, and two absent subjects are the same identity.
    private string? AttestedSubject() =>
        User?.Identity?.IsAuthenticated == true
            ? _identityProvider.ResolveIdentity(User, providedClientId: null, providedGroups: null)?.Subject
            : null;

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
            return this.Refuse();
        }

        // Only the replica that carried the transfer's streams can end it: it holds the bridges, and the
        // participants' connections that are to be told are on it. Behind a proxy with affinity, a request
        // that lost its cookie reaches another replica, which knows the transfer from the shared state
        // alone; acting there would delete that state and tell nobody.
        if (!_bridgeManager.Bridges.ContainsKey(IBridgeManager.KeyFor(transferId, ConnectionType.Data))
            && !_bridgeManager.Bridges.ContainsKey(IBridgeManager.KeyFor(transferId, ConnectionType.Control)))
        {
            _logger.LogWarning("Abort request for transfer {TransferId} refused: this hub holds none of its bridges, so the request did not follow its streams", transferId);
            return this.Refuse();
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
            this.Refuse();
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
        // A proxy that buffers responses holds back the few bytes a stream can end on, an
        // acknowledgement or a Pong, until its buffer fills, which this stream may never do.
        Response.Headers["X-Accel-Buffering"] = "no";
        HttpContext.Features.Get<IHttpResponseBodyFeature>()?.DisableBuffering();
        await Response.StartAsync();
        // Kestrel holds the headers until the first write, and a stream may have nothing to write
        // for a heartbeat interval; the client's request waits on them until then.
        await Response.Body.FlushAsync();

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
