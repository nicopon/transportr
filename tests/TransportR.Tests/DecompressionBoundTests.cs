using System.Buffers;
using Apache.Arrow;
using Apache.Arrow.Memory;
using TransportR.Client;
using TransportR.Client.Batching;
using TransportR.Serialization.Arrow;
using TransportR.Serialization.Arrow.Services;
using TransportR.Serialization.Json;
using TransportR.Serialization.Json.Services;
using TransportR.Serialization.MessagePack;
using TransportR.Serialization.MessagePack.Services;
using TransportR.Serialization.Protobuf;
using TransportR.Serialization.Protobuf.Services;

namespace TransportR.Tests;

/// <summary>
/// A compressed batch is a few bytes on the wire and as many as its sender claims once decoded: the
/// bound of <c>WithMaxBatchBytes</c> has to hold on what a batch expands to, and the refusal has to
/// come before the rest is decompressed, not after.
/// </summary>
/// <remarks>
/// Every serializer is reached through the builder's own resolution, because that is where the
/// bound reaches it. The process-wide allocation counter is wide on purpose: tests run in parallel,
/// and decoding the whole batch would cost several times the margin.
/// </remarks>
public class DecompressionBoundTests
{
    private const int ItemBytes = 1 << 20;
    private const int Bound = 2 * ItemBytes;
    private const int ExpandedItems = 64;
    private const long Margin = 32L << 20;

    public static TheoryData<string> ItemSerializers => ["messagepack", "json", "protobuf"];

    public static TheoryData<string> CheckedBeforeCompression => ["json", "protobuf"];

    private static IBatchSerializer<string> Bounded(string kind, int bound = Bound)
    {
        var builder = new TransportRClientBuilder<string>().WithMaxBatchBytes(bound);
        return (kind switch
        {
            "messagepack" => builder.WithMessagePackSerialization(),
            "json" => builder.WithJsonSerialization(),
            "protobuf" => builder.WithProtobufSerialization(),
            _ => throw new ArgumentOutOfRangeException(nameof(kind)),
        }).ResolveSerializer();
    }

    private static IBatchSerializer<string> HostBuilt(string kind) => kind switch
    {
        "messagepack" => new MessagePackBatchSerializer<string>(),
        "json" => new JsonBatchSerializer<string>(),
        "protobuf" => new ProtobufBatchSerializer<string>(),
        _ => throw new ArgumentOutOfRangeException(nameof(kind)),
    };

    private static async Task<byte[]> WriteAsync<T>(IBatchSerializer<T> serializer, List<T> items)
    {
        var buffer = new MemoryStream();
        await serializer.SerializeBatchAsync(buffer, items);
        return buffer.ToArray();
    }

    private static List<string> Items(int count) => [.. Enumerable.Repeat(new string('a', ItemBytes), count)];

    [Theory]
    [MemberData(nameof(ItemSerializers))]
    public async Task ABatchExpandingPastTheBound_IsRefused_BeforeItIsDecompressed(string kind)
    {
        // Arrange: a batch within the bound is read, then one of a few KB on the wire that
        // is ExpandedItems MiB once decoded.
        var serializer = Bounded(kind);
        var small = Items(count: 1);
        var inside = await ItemChunkSink<string>.ReadBatchBodyFromMemoryAsync(serializer, await WriteAsync(HostBuilt(kind), small));
        var bytes = await WriteAsync(HostBuilt(kind), Items(ExpandedItems));
        var allocatedBefore = GC.GetTotalAllocatedBytes(precise: true);

        // Act
        var failure = await Assert.ThrowsAsync<UndecodableBatchException>(
            () => ItemChunkSink<string>.ReadBatchBodyFromMemoryAsync(serializer, bytes));
        var allocated = GC.GetTotalAllocatedBytes(precise: true) - allocatedBefore;

        // Assert
        Assert.Equal(small, inside);
        Assert.Contains("WithMaxBatchBytes", failure.Message);
        Assert.True(allocated < Margin,
            $"{allocated} bytes were allocated while a batch of {ExpandedItems} MiB was refused at {Bound}.");
    }

    [Theory]
    [MemberData(nameof(CheckedBeforeCompression))]
    public async Task ABatchTheSenderAccepts_IsOneTheReceiverAtTheSameBoundAccepts(string kind)
    {
        // Arrange: a batch whose size before compression is known to the byte.
        var items = Items(count: 1);
        var expanded = kind == "json"
            ? (await WriteAsync(new JsonBatchSerializer<string>(compress: false), items)).Length
            : ProtobufLength(items);

        // Act: the bound at that size, then one byte under it.
        var atSize = Bounded(kind, expanded);
        var received = await ItemChunkSink<string>.ReadBatchBodyFromMemoryAsync(atSize, await WriteAsync(atSize, items));
        var failure = await Assert.ThrowsAsync<InvalidOperationException>(
            () => WriteAsync(Bounded(kind, expanded - 1), items));

        // Assert
        Assert.Equal(items, received);
        Assert.Contains("WithMaxBatchBytes", failure.Message);
    }

    private static int ProtobufLength(List<string> items)
    {
        var buffer = new MemoryStream();
        ProtoBuf.Serializer.Serialize(buffer, items);
        return (int)buffer.Length;
    }

    [Fact]
    public async Task AnArrowBatchExpandingPastTheBound_IsRefused_BeforeItsBuffersAreAllocated()
    {
        // Arrange: eight million zeroed longs, which LZ4 reduces to a few KB, and a thousand
        // that fit.
        var allocator = new TrackingAllocator();
        var serializer = new TransportRClientBuilder<RecordBatch>()
            .WithMaxBatchBytes(Bound)
            .WithArrowSerialization(allocator)
            .ResolveSerializer();
        var inside = await ItemChunkSink<RecordBatch>.ReadBatchBodyFromMemoryAsync(
            serializer, await WriteAsync(new ArrowBatchSerializer(), [Zeroed(1000)]));
        var insideAllocated = allocator.Allocated;
        var bytes = await WriteAsync(new ArrowBatchSerializer(), [Zeroed(8_000_000)]);

        // Act
        var failure = await Assert.ThrowsAsync<UndecodableBatchException>(
            () => ItemChunkSink<RecordBatch>.ReadBatchBodyFromMemoryAsync(serializer, bytes));

        // Assert: the allocator of the host served the batch that fit, and was not asked for the other.
        Assert.NotNull(inside);
        Assert.Equal(1000, inside.Single().Length);
        Assert.True(insideAllocated > 0, "The buffers of a received batch bypassed the allocator of the host.");
        Assert.Contains("WithMaxBatchBytes", failure.Message);
        Assert.True(allocator.Allocated - insideAllocated < Margin,
            $"{allocator.Allocated - insideAllocated} bytes were allocated while a batch of 64 MiB was refused at {Bound}.");
        foreach (var received in inside) received.Dispose();
    }

    [Fact]
    public async Task AnArrowChunkRefusedAtItsSecondBatch_ReleasesTheBatchDecodedBeforeIt()
    {
        // Arrange: one chunk, a batch that expands to most of the bound, then one that LZ4 leaves
        // as it is and whose body alone takes the chunk past the bound, so it is refused before
        // anything is allocated for it.
        var allocator = new TrackingAllocator();
        var serializer = new TransportRClientBuilder<RecordBatch>()
            .WithMaxBatchBytes(Bound)
            .WithArrowSerialization(allocator)
            .ResolveSerializer();
        var bytes = await WriteAsync(new ArrowBatchSerializer(), [Zeroed(200_000), Random(100_000)]);
        Assert.True(bytes.Length < Bound, $"The chunk is {bytes.Length} bytes on the wire, over the bound of {Bound}.");

        // Act
        await Assert.ThrowsAsync<UndecodableBatchException>(
            () => ItemChunkSink<RecordBatch>.ReadBatchBodyFromMemoryAsync(serializer, bytes));

        // Assert: nothing the refused chunk was given is still held.
        Assert.True(allocator.Allocated > 0, "The first batch was not decoded before the second was refused.");
        Assert.Equal(0, allocator.Outstanding);
    }

    private static RecordBatch Zeroed(int length)
        => new RecordBatch.Builder().Append("x", false, new Int64Array.Builder().AppendRange(new long[length]).Build()).Build();

    private static RecordBatch Random(int length)
    {
        var random = new Random(1);
        var values = new long[length];
        for (var i = 0; i < length; i++) values[i] = random.NextInt64();
        return new RecordBatch.Builder().Append("x", false, new Int64Array.Builder().AppendRange(values).Build()).Build();
    }

    private sealed class TrackingAllocator : MemoryAllocator
    {
        private readonly MemoryAllocator _inner = new NativeMemoryAllocator();
        private long _allocated;
        private long _outstanding;

        public long Allocated => Interlocked.Read(ref _allocated);

        /// <summary>What was allocated and has not been released.</summary>
        public long Outstanding => Interlocked.Read(ref _outstanding);

        protected override IMemoryOwner<byte> AllocateInternal(int length, out int bytesAllocated)
        {
            var owner = _inner.Allocate(length);
            bytesAllocated = owner.Memory.Length;
            Interlocked.Add(ref _allocated, length);
            Interlocked.Add(ref _outstanding, length);
            return new Released(owner, () => Interlocked.Add(ref _outstanding, -length));
        }

        private sealed class Released(IMemoryOwner<byte> inner, Action onRelease) : IMemoryOwner<byte>
        {
            private int _disposed;

            public Memory<byte> Memory => inner.Memory;

            public void Dispose()
            {
                if (Interlocked.Exchange(ref _disposed, 1) != 0) return;
                inner.Dispose();
                onRelease();
            }
        }
    }
}
