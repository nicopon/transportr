using System.Runtime.CompilerServices;
using System.Threading.Channels;
using TransportR.Client.Configuration;

namespace TransportR.Client.Batching;

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
/// <b>A chunk goes in whole or not at all, and that is what the shape of this class is for.</b>
/// Capacity for every item is taken up front, so the hand-off below is a sequence of writes into
/// an unbounded channel that cannot block and cannot be cancelled. A chunk stopped half way
/// would be resent whole — a batch is the smallest thing the 30-byte header can name — and every
/// item already handed over would reach the consumer twice.
/// </para>
/// <para>
/// <b>Both dimensions are one credit gate, taken in two steps.</b> The chunk's wire size is known
/// from its header and is charged before decoding; its item count is not known until after. Both
/// are charged before the first item is published, which is what keeps the hand-off atomic, and
/// both are returned item by item as the consumer takes them.
/// </para>
/// </remarks>
internal sealed class ReceiveBuffer<T>
{
    private readonly record struct Entry(T Item, long Cost);

    private readonly Channel<Entry> _channel = Channel.CreateUnbounded<Entry>();
    private readonly int? _maxItems;
    private readonly long? _maxBytes;

    private readonly object _gate = new();
    private int _bufferedItems;
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

        _maxItems = capacity?.MaxItems;
        _maxBytes = capacity?.MaxBytes;
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
    /// reserve, publish, and give back whatever was reserved but never published — cannot be got
    /// wrong from outside, on a path where getting it wrong either leaks capacity until the buffer
    /// wedges shut or hands the consumer part of a chunk that is about to be resent.
    /// </remarks>
    /// <param name="wireBytes">The chunk's payload length, as it arrived.</param>
    /// <param name="decode">Produces the chunk's items. Called once, after capacity is taken.</param>
    /// <param name="ct">
    /// Cancels a wait for capacity, which is the only waiting this method does. It does not reach
    /// the hand-off: once the first item of a chunk is published the rest follow unconditionally.
    /// </param>
    /// <returns>The number of items written to the buffer.</returns>
    public async Task<int> WriteChunkAsync(long wireBytes, Func<Task<List<T>?>> decode, CancellationToken ct)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(wireBytes, nameof(wireBytes));

        await AcquireAsync(items: 0, wireBytes, ct).ConfigureAwait(false);

        List<T>? items;
        try
        {
            items = await decode().ConfigureAwait(false);
        }
        catch
        {
            Release(items: 0, wireBytes);
            throw;
        }

        if (items is null || items.Count == 0)
        {
            Release(items: 0, wireBytes);
            return 0;
        }

        try
        {
            await AcquireAsync(items.Count, bytes: 0, ct).ConfigureAwait(false);
        }
        catch
        {
            Release(items: 0, wireBytes);
            throw;
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

                // TryWrite rather than WriteAsync: on an unbounded channel it only ever fails
                // because the buffer was completed under us, and saying so here keeps the fact
                // that this loop never waits from depending on how a channel overload behaves.
                if (!_channel.Writer.TryWrite(new Entry(items[written], cost)))
                    throw new ChannelClosedException();
            }
        }
        catch
        {
            // Only teardown reaches here, and nothing will be resent into a completed buffer.
            // What made it in is the consumer's to release; what did not is ours.
            var published = written == 0 ? 0 : firstShare + (written - 1) * share;
            Release(items.Count - written, wireBytes - published);
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
                Release(items: 1, entry.Cost);
            }
        }
    }

    /// <summary>
    /// Signals that no more chunks will arrive, and releases a writer parked on the credit gate.
    /// </summary>
    /// <param name="error">
    /// When set, <see cref="ReadAllAsync"/> throws it instead of ending quietly once every already
    /// buffered item has been consumed - how a consumer parked on the next item learns the transfer
    /// failed rather than simply ran out of input.
    /// </param>
    public void Complete(Exception? error = null)
    {
        lock (_gate)
        {
            _completed = true;
            _spaceAvailable?.TrySetResult();
            _spaceAvailable = null;
        }

        _channel.Writer.TryComplete(error);
    }

    /// <summary>
    /// Takes whatever was admitted and the consumer has not taken, and gives each entry to
    /// <paramref name="discard"/>, which owns it from there.
    /// </summary>
    /// <remarks>
    /// Meant for a released session, after <see cref="Complete"/>. It cannot belong to
    /// <see cref="Complete"/>: a consumer still reads what was admitted once the transfer is over.
    /// An entry leaves the channel once, to this method or to the consumer, so none is discarded
    /// that the consumer holds. Capacity is given back as for a consumed entry.
    /// </remarks>
    public void Drain(Action<T> discard)
    {
        while (_channel.Reader.TryRead(out var entry))
        {
            try
            {
                discard(entry.Item);
            }
            finally
            {
                Release(items: 1, entry.Cost);
            }
        }
    }

    /// <remarks>
    /// <para>
    /// The waiter is a single re-armed <see cref="TaskCompletionSource"/> rather than a queue: in
    /// this pipeline the read loop is the only writer, so there is never more than one. The loop
    /// re-checks the condition after every wake, so several writers would still be correct — they
    /// would only lose fairness, which is worth knowing before anyone parallelises the read path.
    /// </para>
    /// <para>
    /// <b>An empty buffer admits anything.</b> A chunk larger than the whole bound — in either
    /// dimension — would otherwise wait for room that, by definition, nothing can ever free.
    /// Emptiness is measured in items and not in bytes, because only the consumer taking an item
    /// gives capacity back, and because the byte reservation this method makes before a chunk is
    /// decoded would otherwise make the buffer look occupied to the item step that follows it.
    /// </para>
    /// </remarks>
    private async ValueTask AcquireAsync(int items, long bytes, CancellationToken ct)
    {
        if (_maxItems is null && _maxBytes is null) return;

        while (true)
        {
            Task wait;
            lock (_gate)
            {
                if (_completed) throw new ChannelClosedException();

                if (_bufferedItems == 0 || Fits(items, bytes))
                {
                    _bufferedItems += items;
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
    /// Called under <see cref="_gate"/>. An unset dimension does not bind, so a capacity carrying
    /// only one of the two is answered by that one alone.
    /// </remarks>
    private bool Fits(int items, long bytes)
        => (_maxItems is not { } maxItems || _bufferedItems + items <= maxItems)
        && (_maxBytes is not { } maxBytes || _bufferedBytes + bytes <= maxBytes);

    /// <remarks>
    /// <b>The lock is taken on every item the consumer takes, and that is deliberate.</b> An
    /// attempt to skip it — decrement interlocked, enter the lock only when a flag says a writer
    /// is parked — deadlocks: the consumer can decrement and read the flag inside the window
    /// where the writer has decided to park but not yet raised it, and if that was the last item
    /// the wake never comes. Making it sound needs a Dekker pair with an explicit memory
    /// barrier, which is not worth an unattributed few percent on a path where the bound
    /// usually never binds at all.
    /// </remarks>
    private void Release(int items, long bytes)
    {
        if (_maxItems is null && _maxBytes is null) return;
        if (items == 0 && bytes == 0) return;

        lock (_gate)
        {
            _bufferedItems -= items;
            _bufferedBytes -= bytes;
            _spaceAvailable?.TrySetResult();
            _spaceAvailable = null;
        }
    }
}
