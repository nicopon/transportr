namespace TransportR.Client;

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
