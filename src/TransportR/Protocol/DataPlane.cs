namespace TransportR.Protocol;

/// <summary>
/// The names on the data plane that are not paths: those of the segments a write stream is sent
/// in. Shared for the reason <see cref="WireRoutes"/> is.
/// </summary>
/// <remarks>
/// A write stream is a succession of short <c>POST {transferId}/send</c> requests rather than one
/// long one, because a front end that buffers a request body holds back the few bytes a stream can
/// end on until the request is over. Each request carries the next bytes of one opening of the
/// stream, its epoch, at their offset in it. The hub reads these names and never the bytes.
/// </remarks>
internal static class DataPlane
{
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
    /// </summary>
    public const string HeartbeatParameter = "heartbeatMs";

    /// <summary>
    /// The response header on every segment the hub took, with how many bytes of the epoch it has
    /// written to the peer. The client sends again from there what is not covered.
    /// </summary>
    public const string RelayedHeader = "TransportR-Relayed";
}
