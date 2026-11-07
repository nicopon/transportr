namespace TransportR.Client.Batching;

/// <summary>
/// Makes the receiving sink of a session when it first asks, and reads the transfer from it.
/// </summary>
/// <param name="isByteStream">Whether the sink it makes hands on bytes as they are.</param>
/// <param name="create">Makes the sink.</param>
internal sealed class ReceivePort<T>(bool isByteStream, Func<IReceiveSink<T>> create) : IReceivePort<T>
{
    private IReceiveSink<T>? _sink;

    public bool IsByteStream => isByteStream;

    public IChunkSink OpenSink() => _sink ??= create();

    public IAsyncEnumerable<T> ReceiveAsync(CancellationToken token) => Opened().ReadItemsAsync(token);

    public IAsyncEnumerable<ByteChunk> ReceiveChunksAsync(CancellationToken token) => Opened().ReadChunksAsync(token);

    private IReceiveSink<T> Opened() =>
        _sink ?? throw new InvalidOperationException("Receiver not started");
}
