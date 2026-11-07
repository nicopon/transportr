namespace TransportR.Client.Batching;

/// <summary>
/// Accumulates items into batches of at most a count, and publishes them to a channel for the send
/// worker.
/// </summary>
/// <remarks>
/// A batch's in-flight slot is taken when it is handed off, before the worker serializes it.
/// Releasing gives nothing back: a batch in the channel is the worker's, which still sends it, and
/// the batch being filled holds no slot yet.
/// </remarks>
internal class BatchAccumulator<T> : Accumulator<List<T>>
{
    private readonly int _batchSize;
    private List<T> _items;

    public BatchAccumulator(int batchSize, int flushIntervalMs, SemaphoreSlim? inFlightSlots = null)
        : base(flushIntervalMs, inFlightSlots)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(batchSize, 0, nameof(batchSize));

        _batchSize = batchSize;
        _items = new List<T>(batchSize);
    }

    protected override bool HasPending => _items.Count > 0;

    /// <summary>
    /// Add an item to the accumulator. Async for proper backpressure handling.
    /// </summary>
    public async ValueTask AddAsync(T item, CancellationToken ct = default)
    {
        await Lock.WaitAsync(ct);
        try
        {
            _items.Add(item);
            if (_items.Count >= _batchSize)
            {
                await FlushAsync(ct);
            }
        }
        finally
        {
            Lock.Release();
        }
    }

    /// <summary>
    /// Add multiple items. Async for proper backpressure handling.
    /// </summary>
    public async ValueTask AddRangeAsync(IEnumerable<T> items, CancellationToken ct = default)
    {
        await Lock.WaitAsync(ct);
        try
        {
            foreach (var item in items)
            {
                _items.Add(item);
                if (_items.Count >= _batchSize)
                {
                    await FlushAsync(ct);
                }
            }
        }
        finally
        {
            Lock.Release();
        }
    }

    protected override ValueTask FlushRemainderAsync(CancellationToken ct) =>
        _items.Count > 0 ? FlushAsync(ct) : ValueTask.CompletedTask;

    /// <summary>
    /// Hands the current batch off, with a slot. Must be called under lock.
    /// </summary>
    protected override async ValueTask FlushAsync(CancellationToken ct)
    {
        if (_items.Count == 0) return;

        // Take an in-flight slot BEFORE handing the batch off. This is the only point at which the
        // producer can be made to wait for the receiver.
        await TakeSlotAsync(ct);

        var batchToSend = _items;
        _items = new List<T>(_batchSize);

        // The channel is unbounded, so this never blocks - the slot taken above is the bound.
        await HandOffAsync(batchToSend, ct);
    }
}
