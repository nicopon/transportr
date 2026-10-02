using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using TransportR.Hub.SignalR.Access;
using TransportR.Interfaces;
using TransportR.Protocol;

namespace TransportR.Hub.SignalR.Controllers;

/// <summary>
/// Where a client presents a renewed token. A connection's principal is fixed when it opens, so
/// nothing sent over it can carry one; this is an HTTP request, which the host's own authentication
/// validates as it does a data stream, and nothing here validates a token itself.
/// </summary>
[ApiController]
[Route(WireRoutes.ClientBase)]
public class ClientController : ControllerBase
{
    private readonly IStateStore _stateStore;
    private readonly IClientIdentityProvider _identityProvider;
    private readonly AccessPolicy _policy;
    private readonly IAccessReconciler _reconciler;
    private readonly ILogger<ClientController> _logger;

    public ClientController(
        IStateStore stateStore,
        IClientIdentityProvider identityProvider,
        AccessPolicy policy,
        IAccessReconciler reconciler,
        ILogger<ClientController> logger)
    {
        _stateStore = stateStore;
        _identityProvider = identityProvider;
        _policy = policy;
        _reconciler = reconciler;
        _logger = logger;
    }

    /// <summary>
    /// Every refusal is a 404, as on the data plane: a 403 would tell a caller which client ids exist.
    /// A valid token of the registered subject that grants no group is not a refusal: it is the
    /// identity provider withdrawing every right, and it is applied.
    /// </summary>
    [HttpPut("{clientId:guid}/token")]
    public async Task<IActionResult> Renew(Guid clientId)
    {
        // Dev mode arms no authorization on this route and attests nothing to renew.
        if (User?.Identity?.IsAuthenticated != true)
        {
            return NotFound();
        }

        var identity = _identityProvider.ResolveIdentity(User, providedClientId: clientId, providedGroups: null);
        if (identity == null || identity.Subject == null || identity.ClientId != clientId)
        {
            _logger.LogWarning("Token renewal refused for {ClientId}: the caller attests no identity for it", clientId);
            return NotFound();
        }

        // The host's middleware accepted the token within its own clock skew; the hub holds it to its own.
        if (_policy.Evaluate(identity) == AccessVerdict.TokenExpired)
        {
            _logger.LogWarning("Token renewal refused for {ClientId}: the token has already expired", clientId);
            return NotFound();
        }

        var renewed = await _stateStore.RenewClientAsync(clientId, identity.Subject, identity.Groups, identity.TokenExpiry);
        if (renewed == null)
        {
            _logger.LogWarning("Token renewal refused for {ClientId}: no registration, or it belongs to another subject", clientId);
            return NotFound();
        }

        _logger.LogInformation("Token renewed for client {ClientId}, groups [{Groups}], new expiry: {Expiry}",
            clientId, string.Join(", ", renewed.Groups), renewed.TokenExpiry?.ToString("u") ?? "N/A");

        // What the new token no longer grants applies now, to what this client has running.
        await _reconciler.ReconcileAsync(clientId, HttpContext.RequestAborted);

        return Ok(new TokenRenewalResponse(renewed.TokenExpiry));
    }
}
