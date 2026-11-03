namespace TransportR.Models;

/// <summary>
/// Represents a resolved client identity.
/// </summary>
/// <param name="Subject">
/// The identity the IdP attests to, as it appears in the token. Null in dev mode, where nothing is
/// attested. The data plane compares against this rather than <paramref name="ClientId"/>, because
/// <paramref name="ClientId"/> is the one the client chose.
/// </param>
public record ClientIdentity(
    Guid ClientId,
    IReadOnlySet<string> Groups,
    DateTime? TokenExpiry,
    string? Subject = null);
