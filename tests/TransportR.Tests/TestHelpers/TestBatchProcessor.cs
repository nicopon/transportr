using TransportR.Client.Batching;
using TransportR.Client.Sessions;

namespace TransportR.Tests;

/// <summary>
/// A <see cref="BatchProcessor{T}"/> with the typed calls the tests make on it, through the send port
/// a client would build for it.
/// </summary>
internal sealed class TestBatchProcessor<T>(ISessionContext<T> context, string transferId, ISendPort<T> send)
    : BatchProcessor<T>(context, transferId, send.Source)
{
    public Task SendAsync(T item, CancellationToken token) => send.SendAsync(item, token);

    public Task SendRangeAsync(IEnumerable<T> items, CancellationToken token) => send.SendRangeAsync(items, token);

    public Task SendBytesAsync(ReadOnlyMemory<byte> bytes, CancellationToken token) => send.SendBytesAsync(bytes, token);
}
