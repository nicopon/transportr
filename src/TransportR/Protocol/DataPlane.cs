namespace TransportR.Protocol;

/// <summary>
/// The names on the data plane that are not paths: those of the segments a write stream is sent
/// in and of the write-transport probe, and the close statuses of a stream carried by a WebSocket.
/// Shared for the reason <see cref="WireRoutes"/> is.
/// </summary>
/// <remarks>
/// <para>
/// A write stream is a succession of short <c>POST {transferId}/send</c> requests rather than one
/// long one, because a front end that buffers a request body holds back the few bytes a stream can
/// end on until the request is over. Each request carries the next bytes of one opening of the
/// stream, its epoch, at their offset in it. The hub reads these names and never the bytes.
/// </para>
/// <para>
/// Where the path allows it, a write stream is instead one WebSocket, <c>GET {transferId}/send</c>
/// with an upgrade, which waits on no round trip per segment. A front end can accept the upgrade and
/// still hold bytes back, so each opening proves the path before the stream starts: the client sends
/// an empty text message and the hub answers with one. Past that the socket carries binary messages
/// only, which the hub copies without reading.
/// </para>
/// </remarks>
internal static class DataPlane
{
    /// <summary>
    /// The request header, on every request of a stream, naming which of a transfer's two connections it
    /// belongs to: a <see cref="ConnectionType"/> by name.
    /// </summary>
    public const string ConnectionTypeHeader = "X-Connection-Type";

    /// <summary>
    /// The query parameter naming the epoch a segment belongs to, a <see cref="Guid"/> the client
    /// draws for each opening of the stream. A segment at offset 0 of an epoch the hub does not hold
    /// opens it and replaces the previous one.
    /// </summary>
    public const string EpochParameter = "epoch";

    /// <summary>The query parameter giving the offset, in the epoch's stream, of the segment's first byte.</summary>
    public const string OffsetParameter = "offset";

    /// <summary>
    /// The query parameter, <c>true</c>, marking the last segment of an epoch: once relayed, the
    /// stream it carries ends, which is its clean end.
    /// </summary>
    public const string FinalParameter = "final";

    /// <summary>
    /// The query parameter that announces, in milliseconds, on the segment that opens an epoch of the
    /// sender's data stream, how often the sender writes when it has nothing to say: the hub ends an
    /// epoch it has had no segment from for two of them. The receiver's control stream announces none.
    /// Every WebSocket announces it, since a socket's keepalive runs at that interval, and only the
    /// sender's data stream is judged on it.
    /// </summary>
    public const string HeartbeatParameter = "heartbeatMs";

    /// <summary>
    /// The response header on every segment the hub took, with how many bytes of the epoch it has
    /// written to the peer. The client sends again from there what is not covered.
    /// </summary>
    public const string RelayedHeader = "TransportR-Relayed";

    /// <summary>
    /// The query parameter of the write-transport probe naming the stream of events the client is
    /// registered on, which is all the probe is admitted on.
    /// </summary>
    public const string ConnectionParameter = "connection";

    /// <summary>
    /// The status of the <c>Close</c> that ends a write stream carried by a WebSocket: the client's when
    /// its stream is over, the hub's once the bridge has closed it. A private code, because the hub
    /// retires a transfer on that end: a front end that cuts a connection with a <c>Close</c> of its
    /// own, 1000 or 1001, has to read as the cut it is.
    /// </summary>
    public const int EndOfStreamCloseStatus = 4000;

    /// <summary>
    /// The status of the <c>Close</c> by which the hub says it holds a stream no longer: replaced, over,
    /// or its peer lost. What a 409 says to a segment, and read the same way.
    /// </summary>
    public const int StreamOverCloseStatus = 4409;
}
