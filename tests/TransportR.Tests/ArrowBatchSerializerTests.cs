using System.Buffers;
using Apache.Arrow;
using Apache.Arrow.Memory;
using Apache.Arrow.Types;
using TransportR.Client.Batching;
using TransportR.Interfaces;
using TransportR.Serialization.Arrow.Services;

namespace TransportR.Tests;

/// <summary>
/// The Arrow serializer, through the protocol's own read path.
/// </summary>
/// <remarks>
/// The read path goes through <see cref="ItemChunkSink{T}.ReadBatchBodyFromMemoryAsync"/> rather
/// than calling the serializer directly, because how the body reaches it is part of what is under
/// test: it wraps the received memory in a stream, and it is that stream — not a copy of it — that
/// lets the reader allocate through a pool.
/// </remarks>
public class ArrowBatchSerializerTests
{
    private static RecordBatch Batch(int first, int count, string prefix = "v")
    {
        var ids = new Int32Array.Builder();
        var labels = new StringArray.Builder();
        for (var i = 0; i < count; i++)
        {
            ids.Append(first + i);
            labels.Append($"{prefix}{first + i}");
        }

        var schema = new Schema(
        [
            new Field("id", Int32Type.Default, nullable: false),
            new Field("label", StringType.Default, nullable: true),
        ], null);

        return new RecordBatch(schema, [ids.Build(), labels.Build()], count);
    }

    private static async Task<byte[]> WriteAsync(ArrowBatchSerializer serializer, List<RecordBatch> batches)
    {
        var buffer = new MemoryStream();
        await serializer.SerializeBatchAsync(buffer, batches);
        return buffer.ToArray();
    }

    private static Task<List<RecordBatch>?> ReadAsync(ArrowBatchSerializer serializer, byte[] bytes)
        => ItemChunkSink<RecordBatch>.ReadBatchBodyFromMemoryAsync(serializer, bytes);

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task A_Batch_Survives_The_Round_Trip(bool compress)
    {
        var serializer = new ArrowBatchSerializer(compress: compress);
        var bytes = await WriteAsync(serializer, [Batch(0, 100)]);

        var back = await ReadAsync(serializer, bytes);

        var batch = Assert.Single(back!);
        Assert.Equal(100, batch.Length);
        Assert.Equal(["id", "label"], batch.Schema.FieldsList.Select(f => f.Name).ToArray());
        Assert.Equal(0, ((Int32Array)batch.Column(0)).GetValue(0));
        Assert.Equal(99, ((Int32Array)batch.Column(0)).GetValue(99));
        Assert.Equal("v42", ((StringArray)batch.Column(1)).GetString(42));
    }

    /// <summary>
    /// Every chunk carries its own schema, so any chunk decodes on its own.
    /// </summary>
    /// <remarks>
    /// The protocol buffers chunks that arrive out of order and replays them after a cut. A
    /// serializer that sent the schema once, in the first chunk, would decode into nothing the
    /// first time either of those happened — and the failure would look like an empty transfer,
    /// not like a bug.
    /// </remarks>
    [Fact]
    public async Task Any_Chunk_Decodes_Without_The_Ones_Before_It()
    {
        var serializer = new ArrowBatchSerializer();

        var first = await WriteAsync(serializer, [Batch(0, 10)]);
        var second = await WriteAsync(serializer, [Batch(10, 10)]);

        // Deliberately out of order, and the second one first.
        var backSecond = Assert.Single((await ReadAsync(serializer, second))!);
        var backFirst = Assert.Single((await ReadAsync(serializer, first))!);

        Assert.Equal(10, ((Int32Array)backSecond.Column(0)).GetValue(0));
        Assert.Equal(0, ((Int32Array)backFirst.Column(0)).GetValue(0));
    }

    /// <summary>
    /// A chunk cut short is refused, not silently returned as the rows that did arrive.
    /// </summary>
    /// <remarks>
    /// This is the network form of a defect already paid for once: an Arrow stream missing only
    /// its <c>ffffffff 00000000</c> end-of-stream marker reads as a complete, shorter stream. A
    /// transport that let that through would hand a consumer a prefix of the data with no error —
    /// the exact silent loss the marker exists to prevent.
    /// </remarks>
    [Fact]
    public async Task A_Truncated_Chunk_Is_Refused()
    {
        var serializer = new ArrowBatchSerializer(compress: false);
        var complete = await WriteAsync(serializer, [Batch(0, 100)]);

        // Drop the last eight bytes: the end-of-stream marker, and nothing else.
        var truncated = complete[..^8];

        await Assert.ThrowsAnyAsync<Exception>(() => ReadAsync(serializer, truncated));
    }

    /// <summary>
    /// A chunk cut so early that not even its schema arrived is still a cut, not an empty chunk.
    /// </summary>
    /// <remarks>
    /// This is the dangerous middle. An empty chunk and a cut one both end without the marker, and
    /// the rule that separates them is whether any bytes arrived at all — so a chunk cut down to a
    /// handful of bytes is exactly where a rule stated one notch too loosely ("nothing decoded
    /// means empty") would report data loss as a routine flush.
    /// </remarks>
    [Fact]
    public async Task A_Chunk_Cut_Before_Its_Schema_Is_Still_A_Cut()
    {
        var serializer = new ArrowBatchSerializer(compress: false);
        var complete = await WriteAsync(serializer, [Batch(0, 100)]);

        await Assert.ThrowsAnyAsync<Exception>(() => ReadAsync(serializer, complete[..12]));
    }

    [Fact]
    public async Task Several_Batches_Of_One_Shape_Travel_In_One_Chunk()
    {
        var serializer = new ArrowBatchSerializer();
        var bytes = await WriteAsync(serializer, [Batch(0, 5), Batch(5, 5)]);

        var back = await ReadAsync(serializer, bytes);

        Assert.Equal(2, back!.Count);
        Assert.Equal(0, ((Int32Array)back[0].Column(0)).GetValue(0));
        Assert.Equal(5, ((Int32Array)back[1].Column(0)).GetValue(0));
    }

    /// <summary>
    /// One chunk is one IPC stream, so two schemas in one chunk is refused rather than truncated.
    /// </summary>
    [Fact]
    public async Task Two_Schemas_In_One_Chunk_Are_Refused()
    {
        var other = new RecordBatch(
            new Schema([new Field("different", StringType.Default, nullable: true)], null),
            [new StringArray.Builder().Append("x").Build()],
            1);

        var serializer = new ArrowBatchSerializer();

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => WriteAsync(serializer, [Batch(0, 5), other]));
    }

    [Fact]
    public async Task An_Empty_Chunk_Reads_Back_As_Nothing()
    {
        var serializer = new ArrowBatchSerializer();
        var bytes = await WriteAsync(serializer, []);

        Assert.Null(await ReadAsync(serializer, bytes));
    }

    /// <summary>
    /// The off-heap memory of a sent batch is released once it is written.
    /// </summary>
    /// <remarks>
    /// Arrow buffers are native, so a batch nobody disposes is not a leak the GC reports — it is
    /// RSS reclaimed only when a finalizer eventually runs. The sender keeps the serialized bytes
    /// for a resend, never the batch, so this is the last place that could release it.
    /// </remarks>
    [Fact]
    public async Task A_Sent_Batch_Releases_Its_Memory()
    {
        var pool = new CountingAllocator();
        var serializer = new ArrowBatchSerializer(pool);

        var ids = new Int32Array.Builder();
        for (var i = 0; i < 1000; i++) ids.Append(i);
        var batch = new RecordBatch(
            new Schema([new Field("id", Int32Type.Default, nullable: false)], null),
            [ids.Build(pool)],
            1000);

        Assert.True(pool.Allocated > 0, "the test allocated nothing, so it proves nothing");

        await WriteAsync(serializer, [batch]);

        Assert.Equal(pool.Allocated, pool.Released);
    }

    [Fact]
    public void A_Type_It_Does_Not_Carry_Is_Refused_By_The_Compiler()
    {
        // This replaces a test that asserted a NotSupportedException on a List<string>: that call
        // does not compile any more, which is the whole point of closing the interface. What is
        // still worth pinning is that the closure is the only one — a serializer that also
        // satisfied IBatchSerializer of some other type would put the runtime failure back.
        var carried = typeof(ArrowBatchSerializer).GetInterfaces()
            .Where(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IBatchSerializer<>))
            .Select(i => i.GetGenericArguments()[0])
            .ToArray();

        Assert.Equal([typeof(RecordBatch)], carried);
    }
}

/// <summary>Counts what Arrow takes and gives back, so a missing dispose is visible.</summary>
internal sealed class CountingAllocator : MemoryAllocator
{
    public long Allocated;
    public long Released;

    protected override IMemoryOwner<byte> AllocateInternal(int length, out int bytesAllocated)
    {
        var inner = System.Buffers.MemoryPool<byte>.Shared.Rent(length);
        bytesAllocated = length;
        Interlocked.Add(ref Allocated, length);
        return new Tracked(this, inner, length);
    }

    private sealed class Tracked(CountingAllocator owner, IMemoryOwner<byte> inner, int length) : IMemoryOwner<byte>
    {
        private int _disposed;
        public Memory<byte> Memory => inner.Memory[..length];

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0) return;
            Interlocked.Add(ref owner.Released, length);
            inner.Dispose();
        }
    }
}
