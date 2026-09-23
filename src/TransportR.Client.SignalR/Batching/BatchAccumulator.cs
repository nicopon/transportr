using System.Threading.Channels;

namespace TransportR.Client.SignalR.Batching;

/// <summary>
/// Accumulates items into batches and publishes them to a channel for parallel processing.
/// </summary>
/// <remarks>
/// The channel itself is unbounded, so handing a batch off never blocks on the channel. Whatever
/// backpressure exists comes from the optional in-flight window passed to the constructor: a slot
/// is taken here, when a batch is handed off, and released by the caller when that batch is
/// acknowledged. Without it, the producer runs to the end of the source regardless of how far
/// behind the receiver is.
/// </remarks>
internal class BatchAccumulator<T> : IDisposable
{
    private readonly int _batchSize;
    private readonly int _flushIntervalMs;
    private readonly SemaphoreSlim _lock = new(1, 1);
    private List<T> _items;
    private DateTime _lastFlushTime;
    private int _batchId;
    private readonly CancellationTokenSource _disposeCts = new();
    private Task? _flushIntervalTask;

    // Optional in-flight window. Held on the PRODUCER side deliberately: gating the serialization
    // worker instead would only move the backlog into _batchChannel, where items are not yet
    // serialized and - for off-heap payloads - not yet released.
    private readonly SemaphoreSlim? _inFlightSlots;

    // Channel for batches - unbounded; the window above is what bounds the producer
    private readonly Channel<(List<T> Items, int BatchId)> _batchChannel;

    public BatchAccumulator(int batchSize, int flushIntervalMs, SemaphoreSlim? inFlightSlots = null)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(batchSize, 0, nameof(batchSize));
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(flushIntervalMs, 0, nameof(flushIntervalMs));

        _inFlightSlots = inFlightSlots;
        _batchSize = batchSize;
        _flushIntervalMs = flushIntervalMs;
        _items = new List<T>(batchSize);
        _lastFlushTime = DateTime.UtcNow;
        _batchId = 0;
        
        // Unbounded channel - producer never blocks, workers process as fast as they can
        _batchChannel = Channel.CreateUnbounded<(List<T>, int)>(new UnboundedChannelOptions
        {
            SingleReader = false, // Multiple workers can consume
            SingleWriter = true   // Only the accumulator writes
        });
    }
    
    /// <summary>
    /// Reader for consuming batches. Use with multiple workers for parallel processing.
    /// </summary>
    public ChannelReader<(List<T> Items, int BatchId)> BatchReader => _batchChannel.Reader;

    /// <summary>
    /// Start the background timeout flush loop.
    /// </summary>
    public void StartFlushInterval()
    {
        if (_flushIntervalTask != null) return;
        _flushIntervalTask = FlushIntervalLoop(_disposeCts.Token);
    }

    /// <summary>
    /// Add an item to the accumulator. Async for proper backpressure handling.
    /// </summary>
    public async ValueTask AddAsync(T item, CancellationToken ct = default)
    {
        await _lock.WaitAsync(ct);
        try
        {
            _items.Add(item);
            if (_items.Count >= _batchSize)
            {
                await FlushInternalAsync(ct);
            }
        }
        finally
        {
            _lock.Release();
        }
    }

    /// <summary>
    /// Add multiple items. Async for proper backpressure handling.
    /// </summary>
    public async ValueTask AddRangeAsync(IEnumerable<T> items, CancellationToken ct = default)
    {
        await _lock.WaitAsync(ct);
        try
        {
            foreach (var item in items)
            {
                _items.Add(item);
                if (_items.Count >= _batchSize)
                {
                    await FlushInternalAsync(ct);
                }
            }
        }
        finally
        {
            _lock.Release();
        }
    }

    /// <summary>
    /// Flush any remaining items and complete the channel.
    /// Call this when done adding items.
    /// </summary>
    public async ValueTask CompleteAsync(CancellationToken ct = default)
    {
        await _lock.WaitAsync(ct);
        try
        {
            if (_items.Count > 0)
            {
                await FlushInternalAsync(ct);
            }
        }
        finally
        {
            _lock.Release();
        }
        _batchChannel.Writer.TryComplete();
    }
    
    /// <summary>
    /// Flush current batch asynchronously, writing to channel.
    /// Must be called under lock.
    /// </summary>
    private async ValueTask FlushInternalAsync(CancellationToken ct)
    {
        if (_items.Count == 0) return;

        // Take an in-flight slot BEFORE handing the batch off. This is the only point at which the
        // producer can be made to wait for the receiver. Linked to _disposeCts so a blocked
        // producer is released when the accumulator goes away rather than hanging on a window that
        // will never reopen.
        if (_inFlightSlots != null)
        {
            using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, _disposeCts.Token);
            await _inFlightSlots.WaitAsync(linked.Token);
        }

        var batchToSend = _items;
        var currentBatchId = _batchId++;
        _items = new List<T>(_batchSize);
        _lastFlushTime = DateTime.UtcNow;

        // The channel is unbounded, so this never blocks - the slot taken above is the bound.
        await _batchChannel.Writer.WriteAsync((batchToSend, currentBatchId), ct);
    }

    private async Task FlushIntervalLoop(CancellationToken token)
    {
        while (!token.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(_flushIntervalMs / 2, token);

                await _lock.WaitAsync(token);
                try
                {
                    if (_items.Count > 0 && (DateTime.UtcNow - _lastFlushTime).TotalMilliseconds >= _flushIntervalMs)
                    {
                        await FlushInternalAsync(token);
                    }
                }
                finally
                {
                    _lock.Release();
                }
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }
    }

    /// <summary>
    /// Releases a producer parked on the in-flight window and accepts no further flush, without
    /// disposing anything: <see cref="Dispose"/> still runs when the session is released, and
    /// cancelling twice is harmless where disposing twice is not.
    /// </summary>
    public void Abandon()
    {
        try
        {
            _disposeCts.Cancel();
            _batchChannel.Writer.TryComplete();
        }
        catch (ObjectDisposedException)
        {
            // Already disposed, which released the producer already.
        }
    }

    public void Dispose()
    {
        // Cancel first: a producer parked on the in-flight window unwinds through the finally that
        // releases _lock, so _lock must still be usable at that point. SemaphoreSlim only needs
        // disposing when AvailableWaitHandle has been touched, which it never is here, so leaving
        // it alone costs nothing and removes the race outright.
        _disposeCts.Cancel();
        _batchChannel.Writer.TryComplete();
        _disposeCts.Dispose();
    }
}
