using System.Threading.Channels;

namespace TransportR.Client.SignalR.Batching;

/// <summary>
/// Accumulates items into batches and publishes them to a channel for parallel processing.
/// Uses a producer/consumer pattern with async backpressure.
/// </summary>
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
    
    // Channel for batches - unbounded to allow producer to run at full speed
    private readonly Channel<(List<T> Items, int BatchId)> _batchChannel;

    public BatchAccumulator(int batchSize, int flushIntervalMs)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(batchSize, 0, nameof(batchSize));
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(flushIntervalMs, 0, nameof(flushIntervalMs));
        
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
        
        var batchToSend = _items;
        var currentBatchId = _batchId++;
        _items = new List<T>(_batchSize);
        _lastFlushTime = DateTime.UtcNow;
        
        // Async write - will wait if channel is full (backpressure)
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

    public void Dispose()
    {
        _disposeCts.Cancel();
        _batchChannel.Writer.TryComplete();
        _lock.Dispose();
        _disposeCts.Dispose();
    }
}
