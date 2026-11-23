using System.Globalization;
using System.Security.Claims;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.AspNetCore.Server.Kestrel.Core.Features;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using TransportR.Hub.Access;
using TransportR.Hub.Diagnostics;
using TransportR.Hub.Interfaces;
using TransportR.Hub.Models;
using TransportR.Hub.Services;
using TransportR.Hub.Streams;
using TransportR.Protocol;

namespace TransportR.Hub.Endpoints;

internal sealed class StreamEndpoints
{
    private readonly ITransportRServer _brokerServer;
    private readonly ITransferManager _transferManager;
    private readonly IStateStore _stateStore;
    private readonly IClientIdentityProvider _identityProvider;
    private readonly BridgeManager _bridgeManager;
    private readonly AccessPolicy _policy;
    private readonly ITransferTerminator _terminator;
    private readonly UploadEpochs _epochs;
    private readonly IHubProgressMonitor _progressMonitor;
    private readonly TimeSpan _inactivityTimeout;
    private readonly TimeProvider _time;
    private readonly ILogger<StreamEndpoints> _logger;
    private readonly HubMetrics? _metrics;

    public StreamEndpoints(
        ITransportRServer brokerServer,
        ITransferManager transferManager,
        IStateStore stateStore,
        IClientIdentityProvider identityProvider,
        BridgeManager bridgeManager,
        AccessPolicy policy,
        ITransferTerminator terminator,
        UploadEpochs epochs,
        IHubProgressMonitor progressMonitor,
        IOptions<DataPlaneConfiguration> dataPlane,
        TimeProvider time,
        ILogger<StreamEndpoints> logger,
        HubMetrics? metrics = null)
    {
        _brokerServer = brokerServer;
        _transferManager = transferManager;
        _stateStore = stateStore;
        _identityProvider = identityProvider;
        _bridgeManager = bridgeManager;
        _policy = policy;
        _terminator = terminator;
        _epochs = epochs;
        _progressMonitor = progressMonitor;
        _inactivityTimeout = TimeSpan.FromSeconds(dataPlane.Value.TransferInactivityTimeoutSeconds);
        _time = time;
        _logger = logger;
        _metrics = metrics;
    }

    private Task<TransferParticipants?> ValidateTransferAsync(ClaimsPrincipal user, string transferId, ConnectionType type, bool isWrite)
    {
        // A participant holds two streams in opposite directions: the sender writes Data and reads
        // Control, the receiver reads Data and writes Control. So the role is the method crossed
        // with the connection type, not the method alone. The header is not trusted here - it only
        // selects which participant the caller is then required to prove it is.
        var isSenderStream = isWrite ^ (type == ConnectionType.Control);
        return AdmitAsync(user, transferId, isSenderStream ? Role.Sender : Role.Receiver);
    }

    private enum Role { Sender, Receiver, Either }

    private async Task<TransferParticipants?> AdmitAsync(ClaimsPrincipal user, string transferId, Role role)
    {
        var participants = await _transferManager.GetParticipantsAsync(transferId);
        if (participants == null)
        {
            // A client reopening a stream of a transfer the hub retired or ended meets this, twice per
            // reopening; it says nothing about the caller's right to be there, which is judged below.
            _logger.LogInformation("Stream request for transfer {TransferId} refused: the hub does not know it (finished, terminated or never opened)", transferId);
            return null;
        }

        // Dev mode arms no authorization on this route, so there is no attested identity to compare
        // and the data plane stays open. SecurityArchitecture.md states that of dev mode.
        if (user?.Identity?.IsAuthenticated != true)
        {
            return participants;
        }

        var caller = _identityProvider.ResolveIdentity(user, providedClientId: null, providedGroups: null);
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
            return null;
        }

        if (_policy.Evaluate(caller) == AccessVerdict.NoGroups)
        {
            _logger.LogWarning(
                "Security: Stream access denied - caller '{CallerSubject}' holds no group, so it is not the {Role} of transfer {TransferId}",
                callerSubject, roleName, transferId);
            return null;
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
                return participants;
            }

            expectedSubjects.Add($"'{expected?.Subject ?? "<none>"}'");
        }

        _logger.LogWarning(
            "Security: Stream access denied - caller '{CallerSubject}' is not the {Role} of transfer {TransferId} (expected {ExpectedSubjects})",
            callerSubject, roleName, transferId, string.Join(" or ", expectedSubjects));
        return null;
    }

    /// <summary>
    /// Takes one segment of a write stream (see <see cref="DataPlane"/>). Answered 200 once it reached
    /// the peer, 202 when it was held at the bound with part of it relayed, both with
    /// <see cref="DataPlane.RelayedHeader"/>; 409 when the hub holds no epoch it follows on from.
    /// </summary>
    public async Task SendAsync(
        HttpContext context,
        string transferId,
        Guid? epoch,
        long? offset,
        bool final = false,
        int? heartbeatMs = null)
    {
        if (!TryParseConnectionType(context.Request, transferId, out var type))
        {
            context.Response.StatusCode = StatusCodes.Status400BadRequest;
            return;
        }

        var bridgeKey = BridgeManager.KeyFor(transferId, type);
        if (epoch is not { } epochId || offset is not >= 0 || !_epochs.TryGet(bridgeKey, epochId, out var upload))
        {
            upload = await OpenEpochAsync(context, transferId, type, bridgeKey, epoch, offset, heartbeatMs);
            if (upload == null) return;
        }
        else if (AccessPolicy.SameIdentity(upload.Subject, AttestedSubject(context.User)) != AccessVerdict.Allowed)
        {
            // The opening was admitted; a segment only has to come from the identity that opened it.
            _logger.LogWarning("Security: Stream access denied - a segment of {BridgeKey} does not come from the identity that opened its stream", bridgeKey);
            Refusal.Write(context.Response);
            return;
        }

        DisableKestrelLimits(context);
        SegmentOutcome outcome;
        try
        {
            outcome = await upload.AcceptAsync(offset!.Value, context.Request.Body, final, context.RequestAborted);
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

        context.Response.Headers[DataPlane.RelayedHeader] = outcome.Relayed.ToString(CultureInfo.InvariantCulture);
        if (outcome.Status == SegmentStatus.Conflict)
        {
            _logger.LogInformation("BRIDGE: Segment of {BridgeKey} at offset {Offset} refused: its stream is over or the segment does not follow on from it", bridgeKey, offset);
        }

        context.Response.StatusCode = outcome.Status switch
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
    public async Task SendOverWebSocketAsync(HttpContext context, string transferId, int? heartbeatMs)
    {
        if (!context.WebSockets.IsWebSocketRequest)
        {
            _logger.LogWarning("Write stream of transfer {TransferId} not opened over a WebSocket: the request reached the hub without an upgrade", transferId);
            context.Response.Headers.Upgrade = "websocket";
            context.Response.StatusCode = StatusCodes.Status426UpgradeRequired;
            return;
        }

        if (!TryParseConnectionType(context.Request, transferId, out var type))
        {
            context.Response.StatusCode = StatusCodes.Status400BadRequest;
            return;
        }

        if (await ValidateTransferAsync(context.User, transferId, type, isWrite: true) is not { } participants)
        {
            Refusal.Write(context.Response);
            return;
        }

        var bridgeKey = BridgeManager.KeyFor(transferId, type);
        if (!AnnouncedHeartbeat.TryAccept(heartbeatMs, _inactivityTimeout, out var heartbeat))
        {
            _logger.LogWarning("WebSocket of {BridgeKey} refused: its opening announces no heartbeat interval the hub accepts (announced {HeartbeatMs} ms)",
                bridgeKey, heartbeatMs);
            context.Response.StatusCode = StatusCodes.Status400BadRequest;
            return;
        }

        using var socket = await context.WebSockets.AcceptWebSocketAsync(new WebSocketAcceptContext { KeepAliveInterval = heartbeat });

        // Only the sender's silence is judged, for the reason given when an epoch opens.
        using var input = new WebSocketInput(socket, bridgeKey, type == ConnectionType.Data ? heartbeat : null, _time, _logger);
        if (!await input.AnswerProbeAsync(context.RequestAborted))
        {
            return;
        }

        input.JudgeSilence();
        if (!await input.WaitForFirstByteAsync(context.RequestAborted))
        {
            return;
        }

        _epochs.Abandon(bridgeKey);
        AnnounceWriteStream(transferId, type, participants, WriteTransport.WebSocket);

        try
        {
            await _brokerServer.HandleSendAsync(transferId, type, input, context.RequestAborted);
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

        await input.EndAsync(context.RequestAborted);
    }

    /// <summary>
    /// The write-transport probe over a WebSocket: the opening of a write stream with no transfer behind
    /// it, which tells the host before any transfer that the path carries one. Admitted as the presence
    /// heartbeat is, on the client's stream of events; the hub answers the probe, announces the WebSocket,
    /// and closes once the client does. Answered 426, and nothing announced, when it is not an upgrade.
    /// </summary>
    public async Task ProbeOverWebSocketAsync(HttpContext context, string? connection)
    {
        if (!context.WebSockets.IsWebSocketRequest)
        {
            _logger.LogWarning("Write-transport probe on stream {ConnectionId} not made over a WebSocket: the request reached the hub without an upgrade", connection);
            context.Response.Headers.Upgrade = "websocket";
            context.Response.StatusCode = StatusCodes.Status426UpgradeRequired;
            return;
        }

        if (await AdmitProbeAsync(context.User, connection) is not { } clientId)
        {
            Refusal.Write(context.Response);
            return;
        }

        using var socket = await context.WebSockets.AcceptWebSocketAsync();
        using var input = new WebSocketInput(socket, $"the write-transport probe of client {clientId}", heartbeatInterval: null, _time, _logger);
        if (!await input.AnswerProbeAsync(context.RequestAborted))
        {
            return;
        }

        AnnounceProbe(clientId, WriteTransport.WebSocket);
        await input.EndProbeAsync();
    }

    /// <summary>
    /// The write-transport probe over segments: an empty <c>POST</c>, admitted as
    /// <see cref="ProbeOverWebSocketAsync"/> is, announced and answered 204.
    /// </summary>
    public async Task<IResult> ProbeOverSegmentsAsync(HttpContext context, string? connection)
    {
        if (await AdmitProbeAsync(context.User, connection) is not { } clientId)
        {
            return Refusal.Result();
        }

        AnnounceProbe(clientId, WriteTransport.Segments);
        return Results.NoContent();
    }

    // No transfer stands behind a probe, so the client's registration is all there is to admit it on.
    private async Task<Guid?> AdmitProbeAsync(ClaimsPrincipal user, string? connection)
    {
        if (connection is { Length: > 0 }
            && await RegisteredCaller.OfStreamAsync(_stateStore, _identityProvider, user, connection) is { } client)
        {
            return client.ClientId;
        }

        _logger.LogInformation("Write-transport probe on stream {ConnectionId} refused: the stream holds no registration of the caller", connection);
        return null;
    }

    /// <summary>
    /// Opens the epoch a segment names when this hub holds none by that id: the transfer is checked
    /// and the caller admitted here, once per epoch. Returns null once the response is written.
    /// </summary>
    private async Task<UploadEpoch?> OpenEpochAsync(
        HttpContext context,
        string transferId,
        ConnectionType type,
        string bridgeKey,
        Guid? epoch,
        long? offset,
        int? heartbeatMs)
    {
        if (await ValidateTransferAsync(context.User, transferId, type, isWrite: true) is not { } participants)
        {
            Refusal.Write(context.Response);
            return null;
        }

        if (epoch is not { } epochId || offset is not >= 0)
        {
            _logger.LogWarning("Segment of {BridgeKey} refused: it names no epoch or no offset", bridgeKey);
            context.Response.StatusCode = StatusCodes.Status400BadRequest;
            return null;
        }

        // A 404 would read as the transfer gone: the client reopens on a 409 instead.
        if (offset != 0)
        {
            _logger.LogInformation("BRIDGE: Segment of {BridgeKey} at offset {Offset} refused: this hub holds no stream {Epoch}", bridgeKey, offset, epochId);
            context.Response.StatusCode = StatusCodes.Status409Conflict;
            return null;
        }

        // Only the sender's silence is judged: a dead sender leaves an Active bridge the reaper skips,
        // while the receiver's heartbeat keeps the transfer alive. The receiver writes what its read
        // loop gives it, which a consumer holding on to a batch parks, so its silence says nothing; a
        // dead receiver shows at its data GET, and the sender then fails for want of acknowledgements.
        TimeSpan? heartbeat = null;
        if (type == ConnectionType.Data)
        {
            if (!AnnouncedHeartbeat.TryAccept(heartbeatMs, _inactivityTimeout, out var announced))
            {
                _logger.LogWarning("Segment of {BridgeKey} refused: the opening of its stream announces no heartbeat interval the hub accepts (announced {HeartbeatMs} ms)",
                    bridgeKey, heartbeatMs);
                context.Response.StatusCode = StatusCodes.Status400BadRequest;
                return null;
            }

            heartbeat = announced;
        }

        AnnounceWriteStream(transferId, type, participants, WriteTransport.Segments);
        return _epochs.Open(
            bridgeKey,
            epochId,
            AttestedSubject(context.User),
            heartbeat,
            (input, token) => _brokerServer.HandleSendAsync(transferId, type, input, token));
    }

    /// <summary>
    /// Says, to the log and to the monitor, what carries a write stream. Both transports come through
    /// here at the point where the hub commits to the stream, so a transport that never carried a byte
    /// is not announced. The client is the transfer's participant for the stream, not the attested
    /// subject, which dev mode does not have.
    /// </summary>
    private void AnnounceWriteStream(string transferId, ConnectionType type, TransferParticipants participants, WriteTransport transport)
    {
        var clientId = type == ConnectionType.Data ? participants.SenderId : participants.ReceiverId;
        var bridgeKey = BridgeManager.KeyFor(transferId, type);
        _logger.LogInformation("BRIDGE: The write stream of {BridgeKey} (client {ClientId}) is carried by {Transport}", bridgeKey, clientId, transport);
        _metrics?.RecordWriteStreamOpened(type, transport);

        try
        {
            _progressMonitor.OnWriteStreamOpened(new WriteStreamOpened(transferId, clientId, type, transport));
        }
        catch (Exception ex)
        {
            _logger.LogWarning("The progress monitor failed on the opening of the write stream of {BridgeKey} ({Message}); the stream goes on", bridgeKey, ex.Message);
        }
    }

    /// <summary>
    /// Says, to the log and to the monitor, what carried a client's write-transport probe: what the hub
    /// saw, not what the client says.
    /// </summary>
    private void AnnounceProbe(Guid clientId, WriteTransport transport)
    {
        _logger.LogInformation("Write-transport probe of client {ClientId}: the path to the hub carries {Transport}", clientId, transport);

        try
        {
            _progressMonitor.OnWriteTransportProbed(new WriteTransportProbed(clientId, transport));
        }
        catch (Exception ex)
        {
            _logger.LogWarning("The progress monitor failed on the write-transport probe of client {ClientId} ({Message}); the probe is answered all the same", clientId, ex.Message);
        }
    }

    // Dev mode attests nothing, and two absent subjects are the same identity.
    private string? AttestedSubject(ClaimsPrincipal user) =>
        user?.Identity?.IsAuthenticated == true
            ? _identityProvider.ResolveIdentity(user, providedClientId: null, providedGroups: null)?.Subject
            : null;

    /// <summary>
    /// Ends the transfer at a participant's request, the way a host terminating a run does: state and
    /// bridges removed, both participants told to cancel. This is for the sender whose Abort cannot
    /// reach the wire because the data stream in front of it is full: the in-stream signal is the one
    /// the design relies on, and this request is only what is left when the stream cannot carry it.
    /// </summary>
    public async Task<IResult> AbortAsync(HttpContext context, string transferId)
    {
        if (await AdmitAsync(context.User, transferId, Role.Either) is null)
        {
            return Refusal.Result();
        }

        // Only the replica that carried the transfer's streams can end it: it holds the bridges, and the
        // participants' connections that are to be told are on it. Behind a proxy with affinity, a request
        // that lost its cookie reaches another replica, which knows the transfer from the shared state
        // alone; acting there would delete that state and tell nobody.
        if (!_bridgeManager.Bridges.ContainsKey(BridgeManager.KeyFor(transferId, ConnectionType.Data))
            && !_bridgeManager.Bridges.ContainsKey(BridgeManager.KeyFor(transferId, ConnectionType.Control)))
        {
            _logger.LogWarning("Abort request for transfer {TransferId} refused: this hub holds none of its bridges, so the request did not follow its streams", transferId);
            return Refusal.Result();
        }

        _logger.LogInformation("Transfer {TransferId} ended at the request of one of its participants", transferId);
        await _terminator.TerminateAsync(transferId, TransferEndReasons.Abort, context.RequestAborted);
        return Results.NoContent();
    }

    public async Task ReceiveAsync(HttpContext context, string transferId)
    {
        if (!TryParseConnectionType(context.Request, transferId, out var type))
        {
            context.Response.StatusCode = StatusCodes.Status400BadRequest;
            return;
        }

        if (await ValidateTransferAsync(context.User, transferId, type, isWrite: false) is null)
        {
            Refusal.Write(context.Response);
            return;
        }

        // A source that finished its data stream has nothing more to send, and its bridge says so to
        // every sink that comes. Answered with an empty 200, a sink that lost the end of the stream
        // takes it for a stream that broke and reopens it, without end.
        if (type == ConnectionType.Data && DataStreamFinished(transferId))
        {
            context.Response.StatusCode = StatusCodes.Status410Gone;
            return;
        }

        DisableKestrelLimits(context);
        context.Response.StatusCode = StatusCodes.Status200OK;
        context.Response.ContentType = "application/octet-stream";
        // A proxy that buffers responses holds back the few bytes a stream can end on, an
        // acknowledgement or a Pong, until its buffer fills, which this stream may never do.
        context.Response.Headers[StreamedResponse.ProxyBufferingHeader] = "no";
        context.Features.Get<IHttpResponseBodyFeature>()?.DisableBuffering();
        await context.Response.StartAsync();
        // Kestrel holds the headers until the first write, and a stream may have nothing to write
        // for a heartbeat interval; the client's request waits on them until then.
        await context.Response.Body.FlushAsync();

        try
        {
            await _brokerServer.HandleReceiveAsync(transferId, type, context.Response.Body, context.RequestAborted);
        }
        catch (OperationCanceledException) { }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error in StreamEndpoints.ReceiveAsync (Transfer: {TransferId})", transferId);
        }
    }

    private bool DataStreamFinished(string transferId) =>
        _bridgeManager.Bridges.TryGetValue(BridgeManager.KeyFor(transferId, ConnectionType.Data), out var bridge)
        && bridge.State == BridgeState.Completed;

    // Matched by name: Enum.TryParse also takes a number, defined or not, and a comma-separated list
    // that it folds into one value.
    private bool TryParseConnectionType(HttpRequest request, string transferId, out ConnectionType type)
    {
        var header = request.Headers[DataPlane.ConnectionTypeHeader].ToString();
        foreach (var candidate in Enum.GetValues<ConnectionType>())
        {
            if (string.Equals(candidate.ToString(), header, StringComparison.OrdinalIgnoreCase))
            {
                type = candidate;
                return true;
            }
        }

        _logger.LogWarning("Stream request for transfer {TransferId} refused: {HeaderName} '{Header}' does not name a connection type", transferId, DataPlane.ConnectionTypeHeader, header);
        type = default;
        return false;
    }

    /// <summary>
    /// Disables Kestrel's minimum data rate and request body size limits for this request.
    /// Streaming connections can be paused or slow, which would otherwise trigger Kestrel's Slowloris
    /// protection and disconnect the client.
    /// </summary>
    private void DisableKestrelLimits(HttpContext context)
    {
        var maxRequestBodySizeFeature = context.Features.Get<IHttpMaxRequestBodySizeFeature>();
        if (maxRequestBodySizeFeature is { IsReadOnly: false })
        {
            maxRequestBodySizeFeature.MaxRequestBodySize = null;
        }

        var minRequestBodyDataRateFeature = context.Features.Get<IHttpMinRequestBodyDataRateFeature>();
        if (minRequestBodyDataRateFeature != null)
        {
            minRequestBodyDataRateFeature.MinDataRate = null;
        }

        var minResponseDataRateFeature = context.Features.Get<IHttpMinResponseDataRateFeature>();
        if (minResponseDataRateFeature != null)
        {
            minResponseDataRateFeature.MinDataRate = null;
            _logger.LogDebug("[StreamEndpoints] MinResponseDataRate Disabled for Request {TraceIdentifier}", context.TraceIdentifier);
        }
        else
        {
            // If the feature is not found, Kestrel limits might not be active (e.g. running behind IIS or different server)
            _logger.LogDebug("[StreamEndpoints] IHttpMinResponseDataRateFeature NOT FOUND for Request {TraceIdentifier}", context.TraceIdentifier);
        }
    }
}
