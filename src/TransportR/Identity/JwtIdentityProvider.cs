using System.Security.Claims;
using Microsoft.Extensions.Logging;
using TransportR.Interfaces;
using TransportR.Models;

namespace TransportR.Identity;

/// <summary>
/// Resolves client identity from JWT claims.
/// </summary>
/// <remarks>
/// Extracts client ID from 'sub' claim, groups from 'transportr:group:*' scopes,
/// and token expiry from 'exp' claim.
/// </remarks>
public class JwtIdentityProvider : IClientIdentityProvider
{
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
        var groups = user.Claims
            .Where(c => c.Type == "scope" && c.Value.StartsWith("transportr:group:"))
            .Select(c => c.Value.Replace("transportr:group:", ""))
            .ToHashSet();

        if (groups.Count == 0)
        {
            _logger?.LogWarning("JwtIdentityProvider: No TransportR group scopes found in token");
            return null;
        }

        // Extract clientId from 'sub' claim or use provided/generate new
        Guid clientId;
        var subClaim = user.Claims.FirstOrDefault(c => c.Type == "sub");
        if (subClaim != null && Guid.TryParse(subClaim.Value, out var parsedId))
        {
            clientId = parsedId;
        }
        else
        {
            // Use provided or generate new (sub claim is often not a Guid like "sender")
            clientId = providedClientId ?? Guid.NewGuid();
            _logger?.LogDebug("JwtIdentityProvider: ClientId {ClientId} (sub was not a Guid)", clientId);
        }

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

        _logger?.LogInformation("JwtIdentityProvider: Resolved identity - ClientId={ClientId}, Groups=[{Groups}], Expiry={Expiry}",
            clientId, string.Join(", ", groups), tokenExpiry?.ToString("u") ?? "N/A");

        return new ClientIdentity(clientId, groups, tokenExpiry);
    }
}
