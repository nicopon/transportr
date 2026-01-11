namespace TransportR.Models;

/// <summary>
/// Represents a resolved client identity.
/// </summary>
public record ClientIdentity(
    Guid ClientId,
    IReadOnlySet<string> Groups,
    DateTime? TokenExpiry);
