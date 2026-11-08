using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using TransportR.Client.Batching;
using TransportR.Client.Sessions;
using TransportR.Models;

namespace TransportR.Client.Internal;

/// <summary>
/// A data session with the ends of it that a client builds for the kind of transfer it carries, which
/// a second handle for the same transfer has to find again.
/// </summary>
internal abstract record TransferSession(IDataSession Session);

/// <summary>
/// A session of items: the ends that carry the item type.
/// </summary>
internal sealed record TransferSession<T>(IDataSession Session, ISendPort<T> Send, IReceivePort<T> Receive)
    : TransferSession(Session);

/// <summary>
/// A session of bytes: the ends that cut a stream into batches and hand them back as they arrived.
/// </summary>
internal sealed record ByteTransferSession(IDataSession Session, ByteBatchSource Send, ByteReceivePort Receive)
    : TransferSession(Session);

/// <summary>
/// Manages active Data sessions for a client.
/// </summary>
internal class SessionRegistry<TSession> : IAsyncDisposable where TSession : TransferSession
{
    private readonly ConcurrentDictionary<string, TSession> _sessions = new();
    private readonly ILogger _logger;

    public SessionRegistry(ILogger logger)
    {
        _logger = logger;
    }

    public int Count => _sessions.Count;
    public IEnumerable<IDataSession> Sessions => _sessions.Values.Select(s => s.Session);

    public TSession GetOrAdd(string transferId, Func<string, TSession> valueFactory)
    {
        return _sessions.GetOrAdd(transferId, valueFactory);
    }

    public bool TryGetSession(string transferId, out TSession? session)
    {
        return _sessions.TryGetValue(transferId, out session);
    }

    public bool TryRemove(string transferId, out TSession? session)
    {
        return _sessions.TryRemove(transferId, out session);
    }

    public bool ContainsKey(string transferId)
    {
        return _sessions.ContainsKey(transferId);
    }

    public void Clear()
    {
        _sessions.Clear();
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var session in _sessions.Values.Select(s => s.Session))
        {
            try
            {
                await session.DisposeAsync();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error disposing session {TransferId}", session.TransferId);
            }
        }
        _sessions.Clear();
    }
}
