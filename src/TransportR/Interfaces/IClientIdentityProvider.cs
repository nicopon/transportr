using TransportR.Models;

namespace TransportR.Interfaces;

/// <summary>
/// Provides client identity resolution from connection context.
/// </summary>
/// <remarks>
/// Implement this interface to customize how client identity is resolved.
/// Built-in implementations include JwtIdentityProvider (for JWT auth) and
/// DevIdentityProvider (for development mode without auth).
/// </remarks>
public interface IClientIdentityProvider
{
    /// <summary>
    /// Resolves client identity from the provided claims principal and optional client-provided values.
    /// </summary>
    /// <param name="user">The claims principal from the connection context.</param>
    /// <param name="providedClientId">Optional client-provided ID (used in dev mode).</param>
    /// <param name="providedGroups">Optional client-provided groups (used in dev mode).</param>
    /// <returns>The resolved client identity, or null if identity cannot be resolved.</returns>
    ClientIdentity? ResolveIdentity(
        System.Security.Claims.ClaimsPrincipal? user,
        Guid? providedClientId,
        IEnumerable<string>? providedGroups);
}
