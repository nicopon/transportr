namespace TransportR;

/// <summary>
/// What the hub answers to a renewal: the expiry it recorded, read off the token its own host
/// validated. A client whose token it cannot read (an encrypted or opaque one) arms its renewal on
/// this value, which is also the one the hub holds it to.
/// </summary>
/// <param name="TokenExpiry">Null when the token carries no expiry, and the hub enforces none.</param>
public sealed record TokenRenewalResponse(DateTime? TokenExpiry);
