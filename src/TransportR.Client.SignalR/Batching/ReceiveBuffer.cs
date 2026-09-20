using System.Runtime.CompilerServices;
using System.Threading.Channels;
using TransportR.Client.SignalR.Configuration;

namespace TransportR.Client.SignalR.Batching;

/// <summary>
/// The receiver's hand-off buffer: decoded items waiting for the consumer, bounded by
/// <see cref="BufferCapacity"/> in items, in wire bytes, or in both.
/// </summary>
/// <remarks>
/// <para>
/// <b>This is not a new backpressure mechanism, it is the unit of the existing one.</b> Filling
/// the buffer parks the read loop, which stops draining the HTTP response body, which is what
/// pushes back through TCP to the hub and on to the sender — and the acknowledgement for a chunk
/// is only emitted once that chunk is in here, so the sender's in-flight window closes too. All
/// of that already worked when the bound was a flat count of ten thousand items; what it could
/// not do was mean anything for a payload whose items are megabytes apiece.
/// </para>
/// <para>
/// <b>The item dimension is the channel's own bound</b>, which already knows how to park a writer
/// and wake it. <b>The byte dimension is the credit gate below</b>, because a channel cannot
/// weigh what it holds.
/// </para>
/// <para>
/// <b>Credit is taken for a whole chunk before it is decoded, and returned item by item as the
/// consumer takes them.</b> Taking it after decoding would bound the same number while holding
/// one more decoded chunk than necessary — the peak would be the bound plus two chunks instead of
/// plus one — which for Arrow is exactly the memory the bound exists to deny.
/// </para>
/// </remarks>
internal sealed class ReceiveBuffer<T>
{
    private readonly record struct Entry(T Item, long Cost);

    private readonly Channel<Entry> _channel;
    private readonly long? _maxBytes;

    private readonly object _gate = new();
    private long _bufferedBytes;
    private bool _completed;
    private TaskCompletionSource? _spaceAvailable;

    /// <param name="capacity">
    /// <c>null</c> for no bound at all. An instance with neither dimension set is refused:
    /// <c>null</c> says "I want no bound", a default-constructed capacity says "I built this
    /// wrong", and conflating the two hides the second.
    /// </param>
    public ReceiveBuffer(BufferCapacity? capacity)
    {
        if (capacity is { IsEmpty: true })
            throw new ArgumentException(
                "A receive capacity needs at least one of the two dimensions bounded. Pass null "
              + "for an unbounded buffer.", nameof(capacity));

        _maxBytes = capacity?.MaxBytes;
        _channel = capacity?.MaxItems is { } maxItems
            ? Channel.CreateBounded<Entry>(new BoundedChannelOptions(maxItems)
            {
                FullMode = BoundedChannelFullMode.Wait
            })
            : Channel.CreateUnbounded<Entry>();
    }

    /// <summary>
    /// Bytes currently charged against the byte dimension. Exposed for tests and diagnostics.
    /// </summary>
    public long BufferedBytes
    {
        get { lock (_gate) return _bufferedBytes; }
    }

    /// <summary>
    /// Takes capacity for a chunk of <paramref name="wireBytes"/>, decodes it, and hands its items
    /// to the consumer.
    /// </summary>
    /// <remarks>
    /// Decoding is passed in rather than done by the caller so that the order — reserve, decode,
    /// publish, and give back whatever was reserved but never published — cannot be got wrong
    /// from outside, on a path where getting it wrong leaks capacity until the buffer wedges shut.
    /// </remarks>
    /// <param name="wireBytes">The chunk's payload length, as it arrived.</param>
    /// <param name="decode">Produces the chunk's items. Called once, after capacity is taken.</param>
    /// <param name="ct">Cancels a wait for capacity and the hand-off.</param>
    /// <returns>The number of items written to the buffer.</returns>
    public async Task<int> WriteChunkAsync(long wireBytes, Func<Task<List<T>?>> decode, CancellationToken ct)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(wireBytes, nameof(wireBytes));

        await AcquireAsync(wireBytes, ct).ConfigureAwait(false);

        List<T>? items;
        try
        {
            items = await decode().ConfigureAwait(false);
        }
        catch
        {
            Release(wireBytes);
            throw;
        }

        if (items is null || items.Count == 0)
        {
            Release(wireBytes);
            return 0;
        }

        // Split so the shares add up to exactly what was reserved: the remainder rides on the
        // first item. Anything that rounds loses capacity on every chunk and never gets it back.
        var share = wireBytes / items.Count;
        var firstShare = share + (wireBytes % items.Count);

        var written = 0;
        try
        {
            for (; written < items.Count; written++)
            {
                var cost = written == 0 ? firstShare : share;
                await _channel.Writer.WriteAsync(new Entry(items[written], cost), ct).ConfigureAwait(false);
            }
        }
        catch
        {
            // What made it in is the consumer's to release; what did not is ours.
            var published = written == 0 ? 0 : firstShare + (written - 1) * share;
            Release(wireBytes - published);
            throw;
        }

        return written;
    }

    /// <summary>
    /// Enumerates buffered items, returning each one's capacity once the consumer comes back for
    /// the next.
    /// </summary>
    /// <remarks>
    /// Returning the capacity on resumption rather than on hand-off is deliberate: the item the
    /// consumer is working on is still in memory, and a bound that stopped counting it would be
    /// short by one item — by a whole record batch, for an Arrow consumer holding one at a time.
    /// </remarks>
    public async IAsyncEnumerable<T> ReadAllAsync([EnumeratorCancellation] CancellationToken ct)
    {
        await foreach (var entry in _channel.Reader.ReadAllAsync(ct).ConfigureAwait(false))
        {
            try
            {
                yield return entry.Item;
            }
            finally
            {
                Release(entry.Cost);
            }
        }
    }

    /// <summary>
    /// Signals that no more chunks will arrive, and releases a writer parked on the byte gate.
    /// </summary>
    public void Complete()
    {
        lock (_gate)
        {
            _completed = true;
            _spaceAvailable?.TrySetResult();
            _spaceAvailable = null;
        }

        _channel.Writer.TryComplete();
    }

    /// <remarks>
    /// <para>
    /// The waiter is a single re-armed <see cref="TaskCompletionSource"/> rather than a queue: in
    /// this pipeline the read loop is the only writer, so there is never more than one. The loop
    /// re-checks the condition after every wake, so several writers would still be correct — they
    /// would only lose fairness, which is worth knowing before anyone parallelises the read path.
    /// </para>
    /// <para>
    /// <b>An empty buffer admits anything.</b> A chunk larger than the whole bound would otherwise
    /// wait for room that, by definition, nothing can ever free.
    /// </para>
    /// </remarks>
    private async ValueTask AcquireAsync(long bytes, CancellationToken ct)
    {
        if (_maxBytes is not { } max) return;

        while (true)
        {
            Task wait;
            lock (_gate)
            {
                if (_completed) throw new ChannelClosedException();

                if (_bufferedBytes == 0 || _bufferedBytes + bytes <= max)
                {
                    _bufferedBytes += bytes;
                    return;
                }

                _spaceAvailable ??= new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                wait = _spaceAvailable.Task;
            }

            await wait.WaitAsync(ct).ConfigureAwait(false);
        }
    }

    /// <remarks>
    /// <b>The lock is taken on every item the consumer takes, and that is deliberate.</b> An
    /// attempt to skip it — decrement interlocked, enter the lock only when a flag says a writer
    /// is parked — deadlocks: the consumer can decrement and read the flag inside the window
    /// where the writer has decided to park but not yet raised it, and if that was the last item
    /// the wake never comes. Making it sound needs a Dekker pair with an explicit memory
    /// barrier, which is not worth an unattributed few percent on a path where the byte bound
    /// usually never binds at all.
    /// </remarks>
    private void Release(long bytes)
    {
        if (_maxBytes is null || bytes == 0) return;

        lock (_gate)
        {
            _bufferedBytes -= bytes;
            _spaceAvailable?.TrySetResult();
            _spaceAvailable = null;
        }
    }
}
