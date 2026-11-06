namespace TransportR.Models;

/// <summary>
/// What carries a client's write stream: the sender's data, the receiver's acknowledgements. Read
/// streams are long GETs whatever this says.
/// </summary>
public enum WriteTransport
{
    /// <summary>One WebSocket per opening of the stream.</summary>
    WebSocket,

    /// <summary>
    /// A succession of short POST requests, which pass wherever a request does, at the price of one
    /// segment per round trip.
    /// </summary>
    Segments,
}

/// <summary>
/// Which <see cref="WriteTransport"/> a client may use.
/// </summary>
public enum WriteTransportPolicy
{
    /// <summary>
    /// A WebSocket where the path to the hub carries one, segments otherwise. The default. A session
    /// that falls back stays on segments until it ends, says why in its log and on
    /// <c>TransferInfo.WriteTransportFallback</c>, and the next transfer tries again.
    /// </summary>
    PreferWebSocket,

    /// <summary>
    /// A WebSocket, or the transfer fails: for a deployment that cannot live with the rate of segments
    /// and would rather be told.
    /// </summary>
    RequireWebSocket,

    /// <summary>Segments, without trying anything else.</summary>
    SegmentsOnly,
}
