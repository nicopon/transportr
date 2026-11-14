namespace TransportR.Client;

/// <summary>
/// How much a transfer buffer may hold before the side filling it is made to wait, expressed in
/// items, in bytes, or in both. A <c>null</c> capacity, not an instance of this, is how a caller
/// says they want no bound.
/// </summary>
/// <remarks>
/// <para>
/// <b>Two dimensions, because one unit cannot serve both kinds of payload.</b> A count of items
/// is the natural bound when an item is a record — ten thousand orders is a memory figure anyone
/// can reason about. It stops being a bound at all when an item is an Arrow record batch of
/// several megabytes, where the same ten thousand is tens of gigabytes. A count of bytes is the
/// natural bound there, and a poor one for small records, where the per-item overhead the wire
/// does not carry dominates. Both are declared, and whichever is reached first is the one that
/// binds.
/// </para>
/// <para>
/// <b>Bytes here are wire bytes</b> — the serialized payload as it crossed the network, not the
/// footprint of the decoded items on the heap. This is what every format can report for free and
/// the same number at both ends; it is not a measure of managed memory. Arrow IPC bodies are
/// close to their decoded buffers, less whatever the LZ4 codec saved (see the compression switch
/// on the Arrow serializer); MessagePack records decode to several times their wire size. Size
/// the bound with that ratio in mind, as you would any buffer.
/// </para>
/// <para>
/// <b>A chunk that exceeds either dimension outright is still admitted into an empty buffer.</b>
/// Refusing it would be a permanent stall rather than backpressure: nothing will ever free space
/// for a chunk that does not fit an empty buffer, and a chunk cannot be admitted in parts — a
/// batch is the smallest unit the protocol can resend. The consequence is that these bounds
/// throttle but never block, which is what makes them safe to apply by default.
/// </para>
/// </remarks>
public readonly record struct BufferCapacity
{
    private BufferCapacity(int? maxItems, long? maxBytes)
    {
        MaxItems = maxItems;
        MaxBytes = maxBytes;
    }

    /// <summary>
    /// Maximum number of buffered items, or <c>null</c> when this dimension is not bounded.
    /// </summary>
    public int? MaxItems { get; }

    /// <summary>
    /// Maximum number of buffered wire bytes, or <c>null</c> when this dimension is not bounded.
    /// </summary>
    public long? MaxBytes { get; }

    /// <summary>
    /// Bounds the buffer by item count alone.
    /// </summary>
    public static BufferCapacity Items(int maxItems)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(maxItems, 0, nameof(maxItems));
        return new BufferCapacity(maxItems, null);
    }

    /// <summary>
    /// Bounds the buffer by wire bytes alone.
    /// </summary>
    public static BufferCapacity Bytes(long maxBytes)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(maxBytes, 0, nameof(maxBytes));
        return new BufferCapacity(null, maxBytes);
    }

    /// <summary>
    /// Bounds the buffer by both dimensions. Whichever is reached first is the one that binds.
    /// </summary>
    public static BufferCapacity ItemsAndBytes(int maxItems, long maxBytes)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(maxItems, 0, nameof(maxItems));
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(maxBytes, 0, nameof(maxBytes));
        return new BufferCapacity(maxItems, maxBytes);
    }

    /// <summary>
    /// Builds a capacity from two optional dimensions, which is the shape configuration binding
    /// produces. Throws when both are absent — that is <c>null</c>, not a capacity.
    /// </summary>
    public static BufferCapacity Of(int? maxItems, long? maxBytes) => (maxItems, maxBytes) switch
    {
        ({ } items, { } bytes) => ItemsAndBytes(items, bytes),
        ({ } items, null) => Items(items),
        (null, { } bytes) => Bytes(bytes),
        _ => throw new ArgumentException(
            "A capacity needs at least one of the two dimensions. Use null for no bound.", nameof(maxItems))
    };

    /// <summary>
    /// True when neither dimension is set, which is the value a default-constructed instance
    /// carries. There is no "unbounded" capacity: a caller who wants no bound says so by not
    /// configuring one, rather than by handing over a capacity that bounds nothing.
    /// </summary>
    public bool IsEmpty => MaxItems is null && MaxBytes is null;
}
