using TransportR.Abstractions;
using TransportR.Client.Sessions;

namespace TransportR.Client.Internal;

/// <summary>
/// Handle for sending data to a receiver. Wraps an IDataSession.
/// </summary>
internal class HttpSend<T>(IDataSession<T> session) : Send<T>(session.TransferId)
{
    public override Task SendAsync(T item, CancellationToken cancellationToken = default)
    {
            return session.SendAsync(item, cancellationToken);
    }

    public override Task SendAsync(IEnumerable<T> items, CancellationToken cancellationToken = default)
    {
            return session.SendRangeAsync(items, cancellationToken);
    }

    public override Task CompleteAsync(CancellationToken cancellationToken = default)
    {
        return session.CompleteAsync(cancellationToken);
    }

    protected override async ValueTask DisposeAsyncCore()
    {
        await session.DisposeAsync();
    }
}

/// <summary>
/// Handle for receiving data from a sender. Wraps an IDataSession.
/// </summary>
internal class HttpReceive<T>(IDataSession<T> session) : Receive<T>(session.TransferId, session.BatchSize)
{
    public override IAsyncEnumerable<T> ReceiveAsync(CancellationToken cancellationToken = default)
    {
        return session.ReceiveAsync(cancellationToken);
    }

    public override Task WaitForCompletionAsync(CancellationToken cancellationToken = default)
    {
        return session.WaitForCompletionAsync(cancellationToken);
    }

    protected override async ValueTask DisposeAsyncCore()
    {
        await session.DisposeAsync();
    }
}
