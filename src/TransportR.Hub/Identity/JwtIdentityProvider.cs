using System.Security.Claims;
using Microsoft.Extensions.Logging;
using TransportR.Hub;

namespace TransportR.Identity;

/// <summary>
/// Resolves client identity from JWT claims.
/// </summary>
/// <remarks>
/// Keeps the client id the caller names, and reads the subject from the 'sub' claim, groups from
/// 'transportr:group:*' scopes and token expiry from the 'exp' claim. A token with no such scope
/// resolves to an identity with no groups rather than to null.
/// </remarks>
public class JwtIdentityProvider : IClientIdentityProvider
{
    private const string GroupScopePrefix = "transportr:group:";

    private readonly ILogger<JwtIdentityProvider>? _logger;

    public JwtIdentityProvider(ILogger<JwtIdentityProvider>? logger = null)
    {
        _logger = logger;
    }

    public ClientIdentity? ResolveIdentity(
        ClaimsPrincipal? user,
        Guid? providedClientId,
        IEnumerable<string>? providedGroups)
    {
        if (user?.Identity?.IsAuthenticated != true)
        {
            _logger?.LogDebug("JwtIdentityProvider: User not authenticated");
            return null;
        }

        // Extract ALL TransportR-specific scopes for multi-group support
        // The scope claim is a space-delimited string (RFC 8693 §4.2, RFC 9068 §2.2.3), which is how
        // OpenIddict emits it; some issuers emit an array instead, which the
        // JwtBearer handler turns into one claim per element. Both shapes are split the same way.
        var groups = user.Claims
            .Where(c => c.Type == "scope")
            .SelectMany(c => c.Value.Split(' ', StringSplitOptions.RemoveEmptyEntries))
            .Where(scope => scope.StartsWith(GroupScopePrefix, StringComparison.Ordinal))
            .Select(scope => scope[GroupScopePrefix.Length..])
            .ToHashSet();

        // An empty set is an answer, not a failure: a valid token that grants nothing is how an
        // identity provider withdraws every right, and only the caller knows whether that is a
        // refusal (Connect) or the withdrawal itself (renewal).
        if (groups.Count == 0)
        {
            _logger?.LogWarning("JwtIdentityProvider: No TransportR group scopes found in token");
        }

        // ASP.NET Core's JwtBearer handler maps 'sub' onto NameIdentifier unless the host sets
        // MapInboundClaims=false, so the same claim arrives under either name depending on a
        // setting this library does not own.
        var subject = user.FindFirst("sub")?.Value
                   ?? user.FindFirst(ClaimTypes.NameIdentifier)?.Value;

        if (string.IsNullOrEmpty(subject))
        {
            // Without an attested subject the data plane has nothing to check a stream caller
            // against, and would refuse every stream after a Connect that appeared to succeed.
            _logger?.LogWarning("JwtIdentityProvider: Token carries no subject claim");
            return null;
        }

        // Never read from the subject, even one that parses as a Guid: the client draws its own id and
        // never learns its token's subject, so an id taken from it is one no request of the client names.
        // What a registration belongs to is the subject, which every control route compares. The data
        // plane names no client and reads only the subject and the groups.
        var clientId = providedClientId ?? Guid.NewGuid();

        // Extract token expiration
        DateTime? tokenExpiry = null;
        var expClaim = user.Claims.FirstOrDefault(c => c.Type == "exp");
        if (expClaim != null && long.TryParse(expClaim.Value, out var expUnix))
        {
            tokenExpiry = DateTimeOffset.FromUnixTimeSeconds(expUnix).UtcDateTime;
        }

        // Log if client tried to provide different groups
        if (providedGroups != null)
        {
            var provided = providedGroups.ToHashSet();
            if (!provided.SetEquals(groups))
            {
                _logger?.LogWarning("Security: Client provided groups [{Provided}] but JWT specifies [{JwtGroups}]",
                    string.Join(", ", provided), string.Join(", ", groups));
            }
        }

        // Every control request and every segment resolves an identity: the line is per request, and its
        // arguments are built only when someone reads it.
        if (_logger?.IsEnabled(LogLevel.Debug) == true)
        {
            _logger.LogDebug("JwtIdentityProvider: Resolved identity - ClientId={ClientId}, Subject={Subject}, Groups=[{Groups}], Expiry={Expiry}",
                clientId, subject, string.Join(", ", groups), tokenExpiry?.ToString("u") ?? "N/A");
        }

        return new ClientIdentity(clientId, groups, tokenExpiry, subject);
    }
}
