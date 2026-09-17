using Apache.Arrow;
using Apache.Arrow.Compression;
using Apache.Arrow.Ipc;
using Apache.Arrow.Memory;
using TransportR.Interfaces;

namespace TransportR.Serialization.Arrow.Services;

/// <summary>
/// Moves Apache Arrow <see cref="RecordBatch"/> payloads over TransportR, as Arrow IPC.
/// </summary>
/// <remarks>
/// <para>
/// <b>Every chunk is a complete IPC stream</b> — schema, batches, end-of-stream marker — not a
/// fragment of one. The protocol deserializes chunks independently, buffers them when they arrive
/// out of order and replays them after a cut, so a chunk that depended on a schema sent once at
/// the start would decode into nothing the moment any of that happened. Repeating the schema is
/// the price of that independence, and it is small next to a batch.
/// </para>
///
/// <para>
/// <b>The end-of-stream marker is why a truncated chunk is refused rather than silently short.</b>
/// An Arrow stream that stops without <c>ffffffff 00000000</c> is not distinguishable from a
/// complete one by row count alone, and a reader that treats a clean EOF as the end returns a
/// prefix of the data at exit 0. Writing the marker on every chunk is what lets the other end tell
/// the two apart; a cut transfer is separately caught by the protocol's own completion sequence.
/// </para>
///
/// <para>
/// <b>This serializer OWNS the batches it is given, and disposes them once written.</b> That is
/// unlike the POCO serializers, and it is not a liberty: a <see cref="RecordBatch"/> holds
/// off-heap memory that something must release, and this is the only place that knows the batch
/// has been written. The sender does not re-serialize on a resend — <c>_unackedBatches</c> holds
/// the serialized BYTES, so a replay never comes back here — which is what makes disposing here
/// safe rather than a use-after-free waiting for a retry.
/// </para>
///
/// <para>
/// <b>Ownership of what comes back.</b> Deserialization allocates through the supplied
/// <see cref="MemoryAllocator"/>, so received batches are observable by a tracking pool — the
/// interface hands this a <see cref="Stream"/> and Arrow's stream reader takes an allocator, so
/// nothing here has to choose between a copy and losing that. Each batch returned is owned by the
/// caller.
/// </para>
/// </remarks>
public sealed class ArrowBatchSerializer : IBatchSerializer
{
    // One factory for the whole serializer: the interface requires implementations to be
    // thread-safe, and building a codec per chunk would allocate on the hot path.
    private static readonly CompressionCodecFactory _codecs = new();

    private readonly MemoryAllocator _allocator;
    private readonly IpcOptions _options;

    /// <param name="allocator">
    /// Where deserialized buffers come from. Passing one is what makes received batches
    /// observable; the default is Arrow's own native allocator.
    /// </param>
    /// <param name="compress">
    /// Whether to compress record batch bodies (Arrow IPC compression, LZ4 frame). The hub
    /// forwards an opaque payload either way — this is a decision between the two ends.
    /// </param>
    public ArrowBatchSerializer(MemoryAllocator? allocator = null, bool compress = true)
    {
        _allocator = allocator ?? new NativeMemoryAllocator();
        _options = new IpcOptions
        {
            WriteLegacyIpcFormat = false,
            CompressionCodec = compress ? CompressionCodecType.Lz4Frame : null,
            CompressionCodecFactory = compress ? _codecs : null,
        };
    }

    public async Task SerializeBatchAsync<T>(Stream destination, List<T> batch)
    {
        var batches = AsRecordBatches(batch);
        if (batches.Count == 0) return;

        // The schema comes off the first batch. A chunk holding batches of differing schemas
        // cannot be written as one IPC stream, and silently writing only the first would be a
        // data loss no reader could detect.
        var schema = batches[0].Schema;
        for (var i = 1; i < batches.Count; i++)
            if (!ReferenceEquals(batches[i].Schema, schema) && !SameFields(batches[i].Schema, schema))
                throw new InvalidOperationException(
                    "An Arrow chunk holds batches of two different schemas. One chunk is one IPC "
                  + "stream, so they cannot travel together; send them as separate batches.");

        try
        {
            using var writer = new ArrowStreamWriter(destination, schema, leaveOpen: true, _options);
            foreach (var recordBatch in batches)
                await writer.WriteRecordBatchAsync(recordBatch).ConfigureAwait(false);

            // Not optional: this is the marker that tells a reader the stream ended because it was
            // finished, rather than because the bytes stopped arriving.
            await writer.WriteEndAsync().ConfigureAwait(false);
        }
        finally
        {
            // In the finally, so a write that throws still releases the off-heap memory. Nothing
            // downstream holds these: the bytes are what gets kept for a resend.
            foreach (var recordBatch in batches) recordBatch.Dispose();
        }
    }

    public async Task<List<T>?> DeserializeBatchAsync<T>(Stream source)
    {
        if (typeof(T) != typeof(RecordBatch))
            throw new NotSupportedException(
                $"The Arrow serializer carries {nameof(RecordBatch)}, not {typeof(T).Name}. "
              + "Build the client with a serializer that matches the item type.");

        var batches = new List<RecordBatch>();
        var probe = new ArrowStreamTerminationProbe(source);
        var reader = new ArrowStreamReader(probe, _allocator, _codecs, leaveOpen: true);

        while (await reader.ReadNextRecordBatchAsync().ConfigureAwait(false) is { } recordBatch)
            batches.Add(recordBatch);

        // A chunk that stopped before its end-of-stream marker is NOT a shorter chunk. Without
        // this the rows that arrived are returned and the transfer reports success, which is the
        // silent loss the marker exists to prevent.
        if (probe.SourceRanDry && probe.BytesRead > 0)
        {
            foreach (var orphan in batches) orphan.Dispose();
            throw new EndOfStreamException(
                "An Arrow chunk ended without its end-of-stream marker: it was cut in transit. "
              + $"{batches.Count} batch(es) had been decoded and are discarded rather than "
              + "delivered as if they were the whole chunk.");
        }

        // Null means "nothing here", which the protocol treats as an empty chunk. An empty list
        // would mean the same thing but costs a caller an allocation to discover it.
        if (batches.Count == 0) return null;

        return (List<T>)(object)batches;
    }

    private static List<RecordBatch> AsRecordBatches<T>(List<T> batch)
    {
        if (batch is List<RecordBatch> already) return already;

        throw new NotSupportedException(
            $"The Arrow serializer carries {nameof(RecordBatch)}, not {typeof(T).Name}. "
          + "Build the client with a serializer that matches the item type.");
    }

    /// <summary>
    /// Field-by-field comparison, because two batches of the same shape carry two Schema
    /// instances — reference equality alone would refuse every legitimate multi-batch chunk.
    /// </summary>
    private static bool SameFields(Schema left, Schema right)
    {
        if (left.FieldsList.Count != right.FieldsList.Count) return false;

        for (var i = 0; i < left.FieldsList.Count; i++)
        {
            var a = left.FieldsList[i];
            var b = right.FieldsList[i];
            if (!string.Equals(a.Name, b.Name, StringComparison.Ordinal)) return false;
            if (a.DataType.TypeId != b.DataType.TypeId) return false;
            if (a.IsNullable != b.IsNullable) return false;
        }

        return true;
    }
}
