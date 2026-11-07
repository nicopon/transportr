using System.Threading.Channels;

namespace TransportR.Client.Batching;

/// <summary>
/// What both accumulators share: the lock over the batch being filled, the flush interval, the
/// in-flight window, the numbering, the channel the batches leave on, and the release. Each rule
/// decides where a batch is cut and when its slot is taken.
/// </summary>
/// <remarks>
/// The channel is unbounded, so handing a batch off never blocks on it. Whatever backpressure
/// exists comes from the optional in-flight window passed to the constructor: a rule takes a slot
/// through <see cref="TakeSlotAsync"/>, and the reader of the channel gives it back once the batch
/// is acknowledged. Without it, the producer runs to the end of the source regardless of how far
/// behind the receiver is.
/// </remarks>
internal abstract class Accumulator<TBatch> : IDisposable
{
    private readonly int _flushIntervalMs;
    private readonly CancellationTokenSource _disposeCts = new();

    // Read once, here: the source's Token throws once disposed, which Dispose does right after
    // cancelling, under a producer that may be about to link to it. The token itself stays readable,
    // and cancelled.
    private readonly CancellationToken _releaseToken;

    private readonly Channel<(TBatch Batch, int BatchId)> _batchChannel;
    private DateTime _lastFlushTime;
    private int _batchId;
    private Task? _flushIntervalTask;
    private int _disposed;

    // Optional in-flight window. Held on the PRODUCER side deliberately: gating the send worker
    // instead would only move the backlog into the channel, where items are not yet serialized
    // and - for off-heap payloads - not yet released.
    private readonly SemaphoreSlim? _inFlightSlots;

    protected Accumulator(int flushIntervalMs, SemaphoreSlim? inFlightSlots)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(flushIntervalMs, 0, nameof(flushIntervalMs));

        _flushIntervalMs = flushIntervalMs;
        _inFlightSlots = inFlightSlots;
        _releaseToken = _disposeCts.Token;
        _lastFlushTime = DateTime.UtcNow;

        _batchChannel = Channel.CreateUnbounded<(TBatch, int)>(new UnboundedChannelOptions
        {
            SingleReader = false, // The send worker, and a rule giving back what nobody read
            SingleWriter = true
        });
    }

    /// <summary>
    /// The batches, in order, each with the slot taken for it, which the reader gives back once the
    /// batch is acknowledged or fails.
    /// </summary>
    public ChannelReader<(TBatch Batch, int BatchId)> BatchReader => _batchChannel.Reader;

    /// <summary>
    /// Held by whoever touches the batch being filled: the producer, the flush interval, the release.
    /// </summary>
    protected SemaphoreSlim Lock { get; } = new(1, 1);

    protected bool HasWindow => _inFlightSlots != null;

    /// <summary>
    /// <c>true</c> once <see cref="Abandon"/> or <see cref="Dispose"/> has run.
    /// </summary>
    protected bool IsReleased => _disposeCts.IsCancellationRequested;

    /// <summary>
    /// Whether the batch being filled holds anything to hand off. Read under the lock.
    /// </summary>
    protected abstract bool HasPending { get; }

    /// <summary>
    /// Hands off the batch being filled. Called under the lock.
    /// </summary>
    protected abstract ValueTask FlushAsync(CancellationToken ct);

    /// <summary>
    /// Hands off or gives back the batch being filled, at the end of the source. Called under the lock.
    /// </summary>
    protected abstract ValueTask FlushRemainderAsync(CancellationToken ct);

    /// <summary>
    /// Gives back what the rule still holds, once the channel is complete. Run by
    /// <see cref="Abandon"/> and by <see cref="Dispose"/>, so twice for an accumulator abandoned and
    /// then disposed.
    /// </summary>
    protected virtual void OnReleased()
    {
    }

    /// <summary>
    /// Start the background timeout flush loop.
    /// </summary>
    public void StartFlushInterval()
    {
        if (_flushIntervalTask != null) return;
        _flushIntervalTask = FlushIntervalLoop(_releaseToken);
    }

    /// <summary>
    /// Hands off what is pending and completes the channel. Call this when done adding.
    /// </summary>
    public async ValueTask CompleteAsync(CancellationToken ct = default)
    {
        await Lock.WaitAsync(ct);
        try
        {
            await FlushRemainderAsync(ct);
        }
        finally
        {
            Lock.Release();
        }
        _batchChannel.Writer.TryComplete();
    }

    /// <summary>
    /// Takes a slot of the window, if there is one. Called under the lock.
    /// </summary>
    protected async ValueTask TakeSlotAsync(CancellationToken ct)
    {
        if (_inFlightSlots == null) return;

        // Linked to the release so a producer parked here is let go when the accumulator goes away
        // rather than hanging on a window that will never reopen.
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, _releaseToken);
        await _inFlightSlots.WaitAsync(linked.Token);
    }

    /// <summary>
    /// Publishes <paramref name="batch"/> under the next number. Called under the lock. The number is
    /// spent even when the channel refuses the batch, which it does once complete, or with
    /// <paramref name="ct"/> already cancelled.
    /// </summary>
    protected ValueTask HandOffAsync(TBatch batch, CancellationToken ct)
    {
        _lastFlushTime = DateTime.UtcNow;
        return _batchChannel.Writer.WriteAsync((batch, _batchId++), ct);
    }

    private async Task FlushIntervalLoop(CancellationToken token)
    {
        while (!token.IsCancellationRequested)
        {
            try
            {
                // At least a millisecond: half of one is no wait at all, nor is a free lock, and the
                // loop would then never give its thread back, the caller of StartFlushInterval first.
                await Task.Delay(Math.Max(1, _flushIntervalMs / 2), token);

                await Lock.WaitAsync(token);
                try
                {
                    if (HasPending && (DateTime.UtcNow - _lastFlushTime).TotalMilliseconds >= _flushIntervalMs)
                    {
                        await FlushAsync(token);
                    }
                }
                finally
                {
                    Lock.Release();
                }
            }
            catch (Exception ex) when (ex is OperationCanceledException or ChannelClosedException)
            {
                break;
            }
        }
    }

    /// <summary>
    /// Releases a producer parked on the in-flight window and accepts no further flush, then gives
    /// back what the rule holds. Disposes nothing: <see cref="Dispose"/> still runs when the session
    /// is released, and cancelling twice is harmless where disposing twice is not.
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
            // Already disposed, which released everything already.
            return;
        }
        OnReleased();
    }

    public void Dispose()
    {
        // CompleteSenderAsync disposes it and a session released meanwhile does too; the second
        // Cancel() on the disposed source would throw.
        if (Interlocked.Exchange(ref _disposed, 1) != 0) return;

        // Cancel first: a producer parked on the in-flight window unwinds through the finally that
        // releases the lock, so the lock must still be usable at that point. SemaphoreSlim only needs
        // disposing when AvailableWaitHandle has been touched, which it never is here, so leaving
        // it alone costs nothing and removes the race outright.
        _disposeCts.Cancel();
        _batchChannel.Writer.TryComplete();
        _disposeCts.Dispose();
        OnReleased();
    }
}
