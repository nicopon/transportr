namespace TransportR.Client.SignalR.Configuration;

/// <summary>
/// Configuration options for TransportR client.
/// </summary>
public class ClientOptions
{
    /// <summary>
    /// Timeout for Pong response during reconnection. Default: 10 seconds.
    /// </summary>
    public TimeSpan PongTimeout { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Buffer size for HTTP stream writes. Default: 512KB.
    /// </summary>
    public int StreamBufferSize { get; set; } = 512 * 1024;

    /// <summary>
    /// Interval for per-transfer heartbeat reporting. Default: 1 second.
    /// </summary>
    /// <remarks>
    /// This reports transfer statistics and only runs while a transfer does. It does not keep the
    /// client's registration alive — see <see cref="PresenceHeartbeatInterval"/>.
    /// </remarks>
    public TimeSpan HeartbeatInterval { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Interval at which the client tells the hub it is still there, independently of any
    /// transfer. Default: 60 seconds.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This is what keeps a registration alive while the client sits idle between transfers, and
    /// what detects that the hub no longer knows this connection — after a hub restart, an evicted
    /// registration, or a SignalR reconnection that produced a new connection id. In all three the
    /// client is connected but invisible to discovery until it registers again.
    /// </para>
    /// <para>
    /// Keep it comfortably below the hub's <c>ConnectedTtlSeconds</c>, which is what it refreshes.
    /// </para>
    /// </remarks>
    public TimeSpan PresenceHeartbeatInterval { get; set; } = TimeSpan.FromSeconds(60);

    /// <summary>
    /// Threshold before token expiry to trigger refresh. Default: 5 minutes.
    /// </summary>
    public TimeSpan TokenRefreshThreshold { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Timeout for batch accumulation in milliseconds. Default: 1000ms.
    /// </summary>
    public int BatchFlushIntervalMs { get; set; } = 1000;

    /// <summary>
    /// Maximum number of batches allowed in flight — flushed by the accumulator but not yet
    /// acknowledged by the receiver. <c>null</c> (the default) means unbounded.
    /// </summary>
    /// <remarks>
    /// <para>
    /// When set, the producer blocks in <c>SendAsync</c> once the window is full and resumes as
    /// acknowledgements come back. This is the only backpressure path from the receiver to the
    /// producer: TCP throttles the HTTP write, but nothing upstream of it. Left unbounded, a slow
    /// receiver makes the sender hold the entire source in memory.
    /// </para>
    /// <para>
    /// <b>Sizing.</b> The window counts <i>batches</i>, so the memory it bounds is roughly
    /// <c>MaxInFlightBatches × batch size</c>. A window too small costs throughput — the producer
    /// idles waiting for acknowledgements — and the right value depends on how large a batch is,
    /// which only the caller knows. This is why there is no default: for item-sized POCOs the
    /// unbounded behaviour is a memory spike, but for multi-megabyte payloads it is the process.
    /// </para>
    /// </remarks>
    public int? MaxInFlightBatches { get; set; }

    /// <summary>
    /// How much the receiver may hold for a consumer that is behind, before the incoming stream
    /// is made to wait. <c>null</c> (the default) means unbounded.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This is the receiving end of the same backpressure <see cref="MaxInFlightBatches"/>
    /// applies at the other, and the two compose: a full receive buffer parks the read loop,
    /// which withholds the acknowledgement that would reopen the sender's window. Left unset at
    /// both ends, nothing anywhere pushes back and a slow consumer is paid for in memory.
    /// </para>
    /// <para>
    /// <b>Unbounded by default, for the same reason the send window is.</b> The right figure
    /// depends on how large an item is, which only the caller knows, and a default would be a
    /// guess dressed as a safeguard. Set it.
    /// </para>
    /// <para>
    /// <b>Two dimensions, because neither alone is a bound.</b> A count of items says nothing
    /// when an item is a multi-megabyte Arrow record batch; a byte budget is a clumsy way to
    /// hold ten thousand small records. Declare both and the first one reached binds. The byte
    /// figure is <i>wire</i> bytes — see <see cref="BufferCapacity"/> for what that does and does
    /// not measure.
    /// </para>
    /// <para>
    /// A single chunk larger than the byte bound is still admitted, so lowering this throttles a
    /// transfer and cannot stall one.
    /// </para>
    /// </remarks>
    public BufferCapacity? ReceiveBufferCapacity { get; set; }
}
