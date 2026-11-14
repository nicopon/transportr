using System.Buffers;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using TransportR.Client.Batching;
using TransportR.Client.Internal;
using TransportR.Client.Protocol;
using TransportR.Client.Sessions;
using TransportR.Tests.TestHelpers;

namespace TransportR.Tests;

/// <summary>
/// A <see cref="DataSession"/> of strings, with the typed calls the tests make on it, through the
/// handles a client would give.
/// </summary>
internal sealed class TestSession : DataSession
{
    private readonly HttpSend<string> _send;
    private readonly HttpReceive<string> _receive;

    public TestSession(StubSessionContext context, string transferId, int batchSize, string dataUrl, string? affinity)
        : this(context, transferId, batchSize, dataUrl, affinity, context.CreatePorts(batchSize))
    {
    }

    private TestSession(
        StubSessionContext context, string transferId, int batchSize, string dataUrl, string? affinity,
        (ISendPort<string> Send, IReceivePort<string> Receive) ports)
        : base(context, transferId, batchSize, dataUrl, affinity, ports.Send.Source, ports.Receive.OpenSink)
    {
        _send = new HttpSend<string>(this, ports.Send);
        _receive = new HttpReceive<string>(this, ports.Receive);
    }

    public Task SendAsync(string item, CancellationToken cancellationToken) => _send.SendAsync(item, cancellationToken);

    public Task SendRangeAsync(IEnumerable<string> items, CancellationToken cancellationToken) =>
        _send.SendAsync(items, cancellationToken);

    public IAsyncEnumerable<string> ReceiveAsync(CancellationToken cancellationToken) =>
        _receive.ReceiveAsync(cancellationToken);
}

/// <summary>
/// A <see cref="BatchProcessor"/> the tests hand chunks they built, payload included.
/// </summary>
internal abstract class TestBatchProcessor(ISessionContext context, string transferId, IBatchSource batches, Func<IChunkSink> openSink)
    : BatchProcessor(context, transferId, batches, openSink)
{
    /// <summary>
    /// Hands the processor a chunk a test built, payload included: the chunk is then the processor's
    /// to dispose, as the memory a chunk is read into is.
    /// </summary>
    public Task ProcessChunkAsync(ChunkData chunk, CancellationToken ct = default) =>
        ProcessChunkAsync(chunk.Header, chunk.Header.PayloadLength > 0 ? new ChunkPayload(chunk) : null, ct);

    private sealed class ChunkPayload(ChunkData chunk) : IMemoryOwner<byte>
    {
        public Memory<byte> Memory => MemoryMarshal.AsMemory(chunk.Payload);

        public void Dispose() => chunk.Dispose();
    }
}

/// <summary>
/// A <see cref="BatchProcessor"/> with the typed calls the tests make on it, through the ends a
/// client would build for it.
/// </summary>
internal sealed class TestBatchProcessor<T>(
    ISessionContext context, string transferId, ISendPort<T> send, IReceivePort<T> receive)
    : TestBatchProcessor(context, transferId, send.Source, receive.OpenSink)
{
    public Task SendAsync(T item, CancellationToken token) => send.SendAsync(item, token);

    public Task SendRangeAsync(IEnumerable<T> items, CancellationToken token) => send.SendRangeAsync(items, token);

    public IAsyncEnumerable<T> ReceiveAsync(CancellationToken token) => receive.ReceiveAsync(token);
}

/// <summary>
/// A <see cref="BatchProcessor"/> with the calls the tests make on it for a stream of bytes, through the
/// ends a <see cref="TransportR.Client.ByteStreamClient"/> would build for it.
/// </summary>
internal sealed class TestByteBatchProcessor(
    ISessionContext context, string transferId, ByteBatchSource send, ByteReceivePort receive)
    : TestBatchProcessor(context, transferId, send, receive.OpenSink)
{
    public Task SendAsync(byte[] item, CancellationToken token) => send.SendBytesAsync(item, token);

    public async Task SendRangeAsync(IEnumerable<byte[]> items, CancellationToken token)
    {
        foreach (var item in items) await send.SendBytesAsync(item, token);
    }

    public Task SendBytesAsync(ReadOnlyMemory<byte> bytes, CancellationToken token) => send.SendBytesAsync(bytes, token);

    /// <summary>
    /// A copy of each chunk, which is disposed once the reader comes back for the next.
    /// </summary>
    public IAsyncEnumerable<byte[]> ReceiveAsync(CancellationToken token) => CopyChunksAsync(receive.ReadAllAsync(token), token);

    public IAsyncEnumerable<ByteChunk> ReceiveChunksAsync(CancellationToken token) => receive.ReadAllAsync(token);

    private static async IAsyncEnumerable<byte[]> CopyChunksAsync(
        IAsyncEnumerable<ByteChunk> chunks, [EnumeratorCancellation] CancellationToken token)
    {
        await foreach (var chunk in chunks.WithCancellation(token).ConfigureAwait(false))
        {
            try
            {
                yield return chunk.Memory.ToArray();
            }
            finally
            {
                chunk.Dispose();
            }
        }
    }
}
