using System.Net.ServerSentEvents;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using TransportR.Abstractions;
using TransportR.Configuration;
using TransportR.Hub.Access;
using TransportR.Hub.Configuration;
using TransportR.Hub.Services;
using TransportR.Interfaces;
using TransportR.Models;
using TransportR.Protocol;
using TransportR.Services;

namespace TransportR.Hub.Controllers;

/// <summary>
/// The control plane: what a client asks of the hub, and the stream of events the hub reaches it by.
/// </summary>
/// <remarks>
/// <para>
/// Every request is authenticated by the host as a data stream is, with the token the client holds
/// at that moment, and names the client it acts for in its path. Every refusal is a 404, as on the
/// data plane: a 403 would tell a caller which client ids exist. The reason is in the hub log.
/// </para>
/// <para>
/// A request about a transfer reaches the instance holding the transfer's bridge, and any other the
/// instance holding the client's stream, through the affinity the client presents on each: neither
/// depends on the other, so a client's transfers on two instances do not pull each other along.
/// </para>
/// </remarks>
[ApiController]
[Route(WireRoutes.ClientBase)]
internal class ClientController : ControllerBase
{
    private readonly IStateStore _stateStore;
    private readonly IClientIdentityProvider _identityProvider;
    private readonly AccessPolicy _policy;
    private readonly IAccessReconciler _reconciler;
    private readonly ClientStreams _streams;
    private readonly TransferInitiator _initiator;
    private readonly ITransferManager _transferManager;
    private readonly AffinityOptions _affinityOptions;
    private readonly TimeSpan _inactivityTimeout;
    private readonly TimeProvider _time;
    private readonly ILogger<ClientController> _logger;

    public ClientController(
        IStateStore stateStore,
        IClientIdentityProvider identityProvider,
        AccessPolicy policy,
        IAccessReconciler reconciler,
        ClientStreams streams,
        TransferInitiator initiator,
        ITransferManager transferManager,
        IOptions<AffinityOptions> affinityOptions,
        IOptions<DataPlaneConfiguration> dataPlane,
        TimeProvider timeProvider,
        ILogger<ClientController> logger)
    {
        _stateStore = stateStore;
        _identityProvider = identityProvider;
        _policy = policy;
        _reconciler = reconciler;
        _streams = streams;
        _initiator = initiator;
        _transferManager = transferManager;
        _affinityOptions = affinityOptions.Value;
        _inactivityTimeout = TimeSpan.FromSeconds(dataPlane.Value.TransferInactivityTimeoutSeconds);
        _time = timeProvider;
        _logger = logger;
    }

    /// <summary>
    /// Registers the client and holds its stream of events open: the registration lasts as long as the
    /// stream, and the end of the stream marks the client disconnected.
    /// </summary>
    /// <remarks>
    /// Refused when the identity cannot be resolved, holds no group, or the client id is registered under
    /// a different attested subject, including a registration whose stream dropped but is still within
    /// its grace period. Registering again under the same subject replaces the registration. Each stream
    /// registers from the principal of its own request; a renewal speaks for it afterwards.
    /// </remarks>
    [HttpGet("{clientId:guid}/" + WireRoutes.EventsSegment)]
    public async Task Events(
        Guid clientId,
        [FromQuery(Name = ControlPlane.GroupsParameter)] string[]? groups,
        [FromQuery(Name = ControlPlane.HeartbeatParameter)] int? heartbeatMs)
    {
        if (!AnnouncedHeartbeat.TryAccept(heartbeatMs, _inactivityTimeout, out var presenceInterval))
        {
            _logger.LogWarning("Registration of {ClientId} refused: the stream announces no presence heartbeat interval the hub accepts (announced {HeartbeatMs} ms)",
                clientId, heartbeatMs);
            Response.StatusCode = StatusCodes.Status400BadRequest;
            return;
        }

        var identity = _identityProvider.ResolveIdentity(User, clientId, groups is { Length: > 0 } ? groups : null);
        if (identity == null || identity.ClientId != clientId)
        {
            _logger.LogWarning("Registration of {ClientId} refused: unable to resolve a client identity for it (resolved {Resolved})",
                clientId, identity?.ClientId.ToString() ?? "none");
            this.Refuse();
            return;
        }

        // A token that grants nothing is a valid token and no registration. Renewal is the one place
        // it means something else: there it withdraws what the client had.
        if (_policy.Evaluate(identity) == AccessVerdict.NoGroups)
        {
            _logger.LogWarning("Registration of {ClientId} refused: the identity holds no group", clientId);
            this.Refuse();
            return;
        }

        // The store overwrites on save, so without this a caller who learned another client's id
        // from discovery takes over its registration: transfers aimed at it land here, and the data
        // plane checks streams against the subject just written. A registration still within its
        // disconnection grace is protected too, since its owner is expected to reclaim it. Dev mode
        // attests no subject, so both sides are null and nothing is refused there.
        var existing = await _stateStore.GetClientAsync(clientId);
        if (existing != null && AccessPolicy.SameIdentity(existing.Subject, identity.Subject) != AccessVerdict.Allowed)
        {
            _logger.LogWarning("Registration refused: ClientId {ClientId} is registered under subject {ExistingSubject}, caller attests {Subject}",
                clientId, existing.Subject, identity.Subject);
            this.Refuse();
            return;
        }

        // Held before the registration is written, and for as long as the stream is: this is what lets
        // a token that is not renewed close it. An expired principal is no reason to refuse it here;
        // the next sweep closes it like any other.
        var stream = _streams.Open(clientId, presenceInterval);
        try
        {
            await _stateStore.SaveClientAsync(new ClientRegistration(
                clientId,
                stream.ConnectionId,
                identity.Groups.ToHashSet(),
                _affinityOptions.Enabled ? AffinityToken() : null,
                identity.TokenExpiry,
                _time.GetUtcNow().UtcDateTime,
                Subject: identity.Subject));

            _logger.LogInformation("Client {ClientId} connected with groups [{Groups}] (expires: {Expiry})",
                clientId, string.Join(", ", identity.Groups), identity.TokenExpiry?.ToString("u") ?? "N/A");

            // An event goes out at once, the registration: a proxy forwards a response with its first
            // byte of body, and one that never sees it answers the client with an error of its own.
            Response.ContentType = "text/event-stream";
            Response.Headers.CacheControl = "no-cache";
            Response.Headers["X-Accel-Buffering"] = "no";
            HttpContext.Features.Get<IHttpResponseBodyFeature>()?.DisableBuffering();

            await SseFormatter.WriteAsync(stream.ReadAllAsync(HttpContext.RequestAborted), Response.Body, HttpContext.RequestAborted);
        }
        catch (OperationCanceledException) when (HttpContext.RequestAborted.IsCancellationRequested)
        {
        }
        catch (IOException)
        {
            // The client went while an event was being written; its departure is recorded below.
        }
        finally
        {
            var reason = stream.EndReason ?? "the client left";
            _streams.Remove(stream);

            // Not deleted: the client usually comes back within seconds under the same id, and
            // deleting the registration would break a transfer about to resume. Marked, so that
            // discovery can say so; a new registration carries no mark, which clears it. The store
            // decides whether the mark still applies, since a newer stream may already have replaced
            // this one.
            await _stateStore.MarkDisconnectedAsync(stream.ConnectionId, _time.GetUtcNow().UtcDateTime);
            _logger.LogInformation("Client {ClientId} disconnected ({Reason}): stream {ConnectionId}", clientId, reason, stream.ConnectionId);
        }
    }

    /// <summary>
    /// The client's presence heartbeat, independent of any transfer: 204 while the hub holds a
    /// registration for that stream, 404 when the client must register again.
    /// </summary>
    /// <remarks>
    /// A hub that restarted, a registration evicted after its grace period, or another stream of the
    /// same identity having taken the registration over all leave the client unknown: discovery
    /// returns nothing for it and nobody can open a transfer against it.
    /// </remarks>
    [HttpPost("{clientId:guid}/" + WireRoutes.PresenceSegment + "/{connectionId}")]
    public async Task<IActionResult> Presence(Guid clientId, string connectionId)
    {
        // Resolving the client by its stream is itself the keep-alive: a store with server-side
        // expiry pushes the registration's TTL back on this lookup.
        var client = await _stateStore.GetClientByConnectionIdAsync(connectionId);
        if (client == null || client.ClientId != clientId || !Attests(client))
        {
            _logger.LogInformation("Heartbeat from {ClientId} on stream {ConnectionId}, which holds no registration. Asking the client to register again.",
                clientId, connectionId);
            return this.Refuse();
        }

        _streams.Touch(clientId);
        return NoContent();
    }

    /// <summary>
    /// The clients this one may send to. A registration whose stream is down is still listed, with its
    /// disconnection marker: a caller that finds nobody fails outright, whereas one that tries a dead
    /// client retries.
    /// </summary>
    [HttpGet("{clientId:guid}/" + WireRoutes.ReceiversSegment)]
    public async Task<IActionResult> Receivers(Guid clientId)
    {
        if (RefuseIfOnlyTheHostOpensTransfers(clientId, nameof(Receivers))) return this.Refuse();

        var source = await AdmitAsync(clientId);
        if (source == null) return this.Refuse();

        var verdict = _policy.Evaluate(source);
        if (verdict != AccessVerdict.Allowed)
        {
            _logger.LogWarning("Receivers refused for client {ClientId}: {Verdict}", clientId, verdict);
            return Ok(new List<ClientInfo>());
        }

        var all = await _stateStore.GetAllClientsAsync();

        // Self excluded, then only the targets the same decision as a transfer lets this caller reach.
        var receivers = all
            .Where(c => c.ClientId != clientId && _policy.MayReach(source, c) == AccessVerdict.Allowed)
            .Select(ToClientInfo)
            .ToList();

        _logger.LogDebug("Receivers: [{SourceGroups}] -> {Count} authorized receivers",
            string.Join(", ", source.Groups), receivers.Count);

        return Ok(receivers);
    }

    /// <summary>
    /// Opens a transfer from this client and answers with its id once the receiver is ready: a 409
    /// carries the reason it was not, written for the client. A receiver whose stream another instance
    /// holds is answered with a 421 that carries the affinity reaching that instance, for the client to
    /// send this request again with it.
    /// </summary>
    [HttpPost("{clientId:guid}/" + WireRoutes.TransfersSegment)]
    public async Task<IActionResult> InitTransfer(Guid clientId, [FromBody] InitTransferRequest request)
    {
        if (RefuseIfOnlyTheHostOpensTransfers(clientId, nameof(InitTransfer))) return this.Refuse();

        var source = await AdmitAsync(clientId);
        if (source == null) return this.Refuse();

        // The handshake is the client's to time, within what the hub keeps a transfer's session for.
        // Past that range a timeout would hold the handshake open, -1 with no deadline at all, or fail
        // after the participants were written.
        var longestTimeoutMs = TransferManager.SessionTtl.TotalMilliseconds;
        if (request.TimeoutMs < 0 || request.TimeoutMs > longestTimeoutMs)
        {
            _logger.LogWarning("InitTransfer refused for {ClientId}: handshake timeout {TimeoutMs} ms is outside 0..{LongestTimeoutMs} ms",
                clientId, request.TimeoutMs, longestTimeoutMs);
            return BadRequest(new HubError("The transfer request is not acceptable."));
        }

        var target = await _stateStore.GetClientAsync(request.TargetClientId);
        if (target == null)
        {
            _logger.LogWarning("InitTransfer failed: Target {TargetClientId} not found.", request.TargetClientId);
            return Conflict(new HubError(TransferInitiator.TargetUnavailable));
        }

        try
        {
            _initiator.ValidateTransferRequest(source, target);

            if (Redirect(target) is { } redirect) return StatusCode(StatusCodes.Status421MisdirectedRequest, redirect);

            var transferId = await _initiator.HandshakeAsync(source, target, request.TimeoutMs,
                requestSender: false, HttpContext.RequestAborted);
            return Ok(new InitTransferResponse(transferId));
        }
        catch (HubRequestFailedException ex)
        {
            return Conflict(new HubError(ex.Message));
        }
        catch (OperationCanceledException) when (HttpContext.RequestAborted.IsCancellationRequested)
        {
            return new EmptyResult();
        }
    }

    /// <summary>The client's answer to an event that asked for one.</summary>
    [HttpPost("{clientId:guid}/" + WireRoutes.RepliesSegment + "/{requestId:guid}")]
    public async Task<IActionResult> Reply(Guid clientId, Guid requestId, [FromBody] CallbackReply reply)
    {
        if (await AdmitAsync(clientId) == null) return this.Refuse();

        if (!_streams.TryReply(clientId, requestId, reply.Outcome))
        {
            _logger.LogInformation("Reply of {ClientId} to {RequestId} came after the handshake had ended, or to another instance", clientId, requestId);
            return this.Refuse();
        }

        return NoContent();
    }

    /// <summary>
    /// A participant's statistics, which also show the transfer active. The role is the participant's,
    /// not the caller's to say.
    /// </summary>
    [HttpPost("{clientId:guid}/" + WireRoutes.TransfersSegment + "/{transferId}/" + WireRoutes.HeartbeatSegment)]
    public async Task<IActionResult> TransferHeartbeat(Guid clientId, string transferId, [FromBody] TransferProgress progress)
    {
        var participants = await AdmitParticipantAsync(clientId, transferId);
        if (participants == null) return this.Refuse();

        if (participants.SenderId == clientId)
        {
            await _transferManager.UpdateSenderStatsAsync(transferId, progress.Items, progress.Acks);
        }
        else
        {
            await _transferManager.UpdateReceiverStatsAsync(transferId, progress.Items, progress.Acks);
        }

        return NoContent();
    }

    /// <summary>
    /// What a session asks before it reopens its streams: that the hub answers is what it wants to
    /// know, and it keeps the transfer alive while the session reconnects.
    /// </summary>
    [HttpPost("{clientId:guid}/" + WireRoutes.TransfersSegment + "/{transferId}/" + WireRoutes.PingSegment)]
    public async Task<IActionResult> Ping(Guid clientId, string transferId)
    {
        if (await AdmitParticipantAsync(clientId, transferId) == null) return this.Refuse();

        await _transferManager.KeepAliveAsync(transferId);
        return NoContent();
    }

    /// <summary>
    /// Where a client presents a renewed token: a stream's principal is fixed when it opens, so the
    /// stream cannot carry one. A valid token of the registered subject that grants no group is not a
    /// refusal: it is the identity provider withdrawing every right, and it is applied.
    /// </summary>
    [HttpPut("{clientId:guid}/" + WireRoutes.TokenSegment)]
    public async Task<IActionResult> Renew(Guid clientId)
    {
        // Dev mode arms no authorization on this route and attests nothing to renew.
        if (User?.Identity?.IsAuthenticated != true)
        {
            return this.Refuse();
        }

        var identity = _identityProvider.ResolveIdentity(User, providedClientId: clientId, providedGroups: null);
        if (identity == null || identity.Subject == null || identity.ClientId != clientId)
        {
            _logger.LogWarning("Token renewal refused for {ClientId}: the caller attests no identity for it", clientId);
            return this.Refuse();
        }

        // The host's middleware accepted the token within its own clock skew; the hub holds it to its own.
        if (_policy.Evaluate(identity) == AccessVerdict.TokenExpired)
        {
            _logger.LogWarning("Token renewal refused for {ClientId}: the token has already expired", clientId);
            return this.Refuse();
        }

        var renewed = await _stateStore.RenewClientAsync(clientId, identity.Subject, identity.Groups, identity.TokenExpiry);
        if (renewed == null)
        {
            _logger.LogWarning("Token renewal refused for {ClientId}: no registration, or it belongs to another subject", clientId);
            return this.Refuse();
        }

        _logger.LogInformation("Token renewed for client {ClientId}, groups [{Groups}], new expiry: {Expiry}",
            clientId, string.Join(", ", renewed.Groups), renewed.TokenExpiry?.ToString("u") ?? "N/A");
        _streams.Touch(clientId);

        // What the new token no longer grants applies now, to what this client has running.
        await _reconciler.ReconcileAsync(clientId, HttpContext.RequestAborted);

        return Ok(new TokenRenewalResponse(renewed.TokenExpiry));
    }

    /// <summary>
    /// The registration of <paramref name="clientId"/>, when the caller is that client: the subject its
    /// token attests is the registration's. Hearing from it keeps its stream open.
    /// </summary>
    private async Task<ClientRegistration?> AdmitAsync(Guid clientId)
    {
        var client = await _stateStore.GetClientAsync(clientId);
        if (client == null || !Attests(client))
        {
            _logger.LogWarning("Request of {ClientId} refused: {Reason}", clientId,
                client == null ? "no registration" : "the caller does not attest its subject");
            return null;
        }

        _streams.Touch(clientId);
        return client;
    }

    private async Task<TransferParticipants?> AdmitParticipantAsync(Guid clientId, string transferId)
    {
        if (await AdmitAsync(clientId) == null) return null;

        var participants = await _transferManager.GetParticipantsAsync(transferId);
        if (participants == null || (participants.SenderId != clientId && participants.ReceiverId != clientId))
        {
            _logger.LogInformation("Request of {ClientId} about transfer {TransferId} refused: {Reason}", clientId, transferId,
                participants == null ? "the hub does not know the transfer" : "the client takes no part in it");
            return null;
        }

        return participants;
    }

    // Dev mode arms no authorization here, and an unauthenticated caller attests nothing to compare.
    // Past that, an authenticated caller must have been attested, as on the data plane.
    private bool Attests(ClientRegistration client)
    {
        if (User?.Identity?.IsAuthenticated != true) return true;

        var caller = _identityProvider.ResolveIdentity(User, client.ClientId, providedGroups: null);
        return caller?.Subject != null
            && caller.ClientId == client.ClientId
            && AccessPolicy.SameIdentity(client.Subject, caller.Subject) == AccessVerdict.Allowed;
    }

    private bool RefuseIfOnlyTheHostOpensTransfers(Guid clientId, string request)
    {
        if (_policy.MayClientOpenTransfers() == AccessVerdict.Allowed) return false;

        _logger.LogWarning("{Request} refused for client {ClientId}: only the host opens transfers", request, clientId);
        return true;
    }

    /// <summary>
    /// The affinity to send the request again with, when the receiver's stream is on another instance.
    /// None when this instance holds it, when affinity is off or unknown for the receiver, or when the
    /// request already carries it: a request that came back here with it would only come back again,
    /// and the handshake says the receiver is not reachable instead.
    /// </summary>
    private AffinityRedirect? Redirect(ClientRegistration target)
    {
        if (!_affinityOptions.Enabled || string.IsNullOrEmpty(target.AffinityToken)) return null;
        if (_streams.Holds(target.ConnectionId) || AffinityToken() == target.AffinityToken) return null;

        var cookieName = _affinityOptions.CustomCookieName
            ?? _affinityOptions.KnownCookieNames.FirstOrDefault()
            ?? "affinity";

        _logger.LogInformation("Affinity redirect: receiver {TargetClientId} is held by the instance of {ReceiverAffinity}",
            target.ClientId, target.AffinityToken);

        return new AffinityRedirect(cookieName, target.AffinityToken);
    }

    private string? AffinityToken()
    {
        var cookies = Request.Cookies;

        if (!string.IsNullOrEmpty(_affinityOptions.CustomCookieName) &&
            cookies.TryGetValue(_affinityOptions.CustomCookieName, out var customValue))
        {
            return customValue;
        }

        foreach (var cookieName in _affinityOptions.KnownCookieNames)
        {
            if (cookies.TryGetValue(cookieName, out var value))
            {
                return value;
            }
        }

        return null;
    }

    /// <summary>
    /// What discovery is allowed to reveal of a registration: identity, groups, and whether its stream
    /// is currently up. Nothing about the hub itself.
    /// </summary>
    private static ClientInfo ToClientInfo(ClientRegistration client) => new()
    {
        ClientId = client.ClientId,
        Groups = client.Groups,
        DisconnectedAtUtc = client.DisconnectedAtUtc
    };
}
