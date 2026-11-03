
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
    /// <summary>
    /// The data plane's base path, relative to the hub's base URL. The two streams hang off it as
    /// <c>{transferId}/send</c> and <c>{transferId}/receive</c>.
    /// </summary>
    public const string StreamBase = "/api/stream";

    /// <summary>
    /// The last segment of <c>POST {StreamBase}/{transferId}/abort</c>: a participant asking the hub
    /// to end the transfer on a request of its own, for the case where the in-stream <c>Abort</c>
    /// cannot overtake the data in front of it. Answered 204; every refusal is a 404 carrying
    /// <see cref="RefusedHeader"/>, as on the streams.
    /// </summary>
    public const string AbortSegment = "abort";

    /// <summary>
    /// The control plane's base path, relative to the hub's base URL. Every request of a client hangs
    /// off it as <c>{clientId}/…</c>, the renewal of its token (<c>PUT {clientId}/token</c>, answered
    /// with a <see cref="TokenRenewalResponse"/>) as much as the segments below. A reverse proxy that
    /// filters by path has to let it through as it does <see cref="StreamBase"/>, and must not buffer
    /// the <see cref="EventsSegment"/> stream.
    /// </summary>
    public const string ClientBase = "/api/client";

    /// <summary>
    /// <c>GET {ClientBase}/{clientId}/events</c>: the stream of server-sent events by which the hub
    /// reaches a client. Opening it registers the client, and its end is the client's disconnection.
    /// </summary>
    public const string EventsSegment = "events";

    /// <summary>
    /// <c>POST {ClientBase}/{clientId}/presence/{connectionId}</c>: the client's presence heartbeat,
    /// answered 204 while the hub holds a registration for that stream and 404 once it does not.
    /// </summary>
    public const string PresenceSegment = "presence";

    /// <summary><c>GET {ClientBase}/{clientId}/receivers</c>: the clients this one may send to.</summary>
    public const string ReceiversSegment = "receivers";

    /// <summary>
    /// <c>POST {ClientBase}/{clientId}/transfers</c> opens a transfer to another client;
    /// <c>{transferId}/heartbeat</c> and <c>{transferId}/ping</c> under it report on one and probe the
    /// hub before its streams reopen.
    /// </summary>
    public const string TransfersSegment = "transfers";

    /// <summary>The last segment of a transfer's statistics report, under <see cref="TransfersSegment"/>.</summary>
    public const string HeartbeatSegment = "heartbeat";

    /// <summary>The last segment of the probe a session makes before it reopens its streams.</summary>
    public const string PingSegment = "ping";

    /// <summary>
    /// <c>POST {ClientBase}/{clientId}/replies/{requestId}</c>: a client's answer to an event that
    /// asks for one, which a handshake waits for.
    /// </summary>
    public const string RepliesSegment = "replies";

    /// <summary>
    /// The response header on every 404 the hub answers itself, on either plane, with the value
    /// <c>1</c> whatever the reason. A 404 without it comes from no TransportR route: a proxy that does
    /// not route the path, or a wrong base URL, and never a refusal.
    /// </summary>
    public const string RefusedHeader = "TransportR-Refused";
}
