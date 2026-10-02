namespace TransportR.Protocol;

/// <summary>
/// The endpoints both ends of a transfer have to agree on.
/// </summary>
/// <remarks>
/// They live here, in the assembly the client and the hub both reference, because a route written
/// as a literal on each side is a contract nothing checks: the two can drift and only a running
/// transfer finds out.
/// </remarks>
public static class WireRoutes
{
    /// <summary>The SignalR control plane, relative to the hub's base URL.</summary>
    public const string CommandHub = "/hub/command";

    /// <summary>
    /// The data plane's base path, relative to the hub's base URL. The two streams hang off it as
    /// <c>{transferId}/send</c> and <c>{transferId}/receive</c>.
    /// </summary>
    public const string StreamBase = "/api/stream";

    /// <summary>
    /// The path a client renews its token on, relative to the hub's base URL:
    /// <c>PUT {clientId}/token</c> under this base, answered with a <see cref="TokenRenewalResponse"/>.
    /// A reverse proxy that filters by path has to let it through as it does <see cref="StreamBase"/>.
    /// </summary>
    public const string ClientBase = "/api/client";
}
