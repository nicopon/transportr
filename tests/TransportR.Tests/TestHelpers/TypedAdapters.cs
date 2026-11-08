using System.Buffers;
using System.Runtime.InteropServices;
using TransportR.Client.Batching;
using TransportR.Client.Internal;
using TransportR.Client.Sessions;
using TransportR.Protocol;
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
/// A <see cref="BatchProcessor"/> with the typed calls the tests make on it, through the ends a
/// client would build for it.
/// </summary>
internal sealed class TestBatchProcessor<T>(
    ISessionContext context, string transferId, ISendPort<T> send, IReceivePort<T> receive)
    : BatchProcessor(context, transferId, send.Source, receive.OpenSink)
{
    public Task SendAsync(T item, CancellationToken token) => send.SendAsync(item, token);

    public Task SendRangeAsync(IEnumerable<T> items, CancellationToken token) => send.SendRangeAsync(items, token);

    public Task SendBytesAsync(ReadOnlyMemory<byte> bytes, CancellationToken token) => send.SendBytesAsync(bytes, token);

    public IAsyncEnumerable<T> ReceiveAsync(CancellationToken token) => receive.ReceiveAsync(token);

    public IAsyncEnumerable<ByteChunk> ReceiveChunksAsync(CancellationToken token) => receive.ReceiveChunksAsync(token);

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
