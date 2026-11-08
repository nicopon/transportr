namespace TransportR.Client.Batching;

/// <summary>
/// Makes the receiving sink of a session when it first asks, and reads the transfer from it.
/// </summary>
/// <param name="create">Makes the sink.</param>
internal sealed class ReceivePort<T>(Func<IReceiveSink<T>> create) : IReceivePort<T>
{
    private IReceiveSink<T>? _sink;

    public IChunkSink OpenSink() => _sink ??= create();

    public IAsyncEnumerable<T> ReceiveAsync(CancellationToken token) => Opened().ReadItemsAsync(token);

    private IReceiveSink<T> Opened() =>
        _sink ?? throw new InvalidOperationException("Receiver not started");
}
