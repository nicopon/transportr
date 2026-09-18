namespace TransportR.Models;

/// <summary>
/// Registration data for a connected client, stored in state store.
/// </summary>
public record ClientRegistration(
    Guid ClientId,
    string ConnectionId,
    HashSet<string> Groups,
    string? AffinityToken,
    DateTime? TokenExpiry,
    DateTime RegisteredAt,
    DateTime? DisconnectedAtUtc = null);
