namespace TransportR.Hub;

/// <summary>
/// Registration data for a connected client, stored in state store.
/// </summary>
/// <param name="Subject">
/// The identity the IdP attested to when the client registered. This is what the data plane compares a stream
/// caller against; a registration carrying null cannot be claimed by any authenticated caller.
/// </param>
public record ClientRegistration(
    Guid ClientId,
    string ConnectionId,
    HashSet<string> Groups,
    string? AffinityToken,
    DateTime? TokenExpiry,
    DateTime RegisteredAt,
    DateTime? DisconnectedAtUtc = null,
    string? Subject = null);
