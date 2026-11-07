using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using TransportR.Client.Batching;
using TransportR.Client.Sessions;
using TransportR.Models;

namespace TransportR.Client.Internal;

/// <summary>
/// A data session with the ends of it that carry the item type, which a second handle for the same
/// transfer has to find again.
/// </summary>
internal sealed record TransferSession<T>(IDataSession Session, ISendPort<T> Send, IReceivePort<T> Receive);

/// <summary>
/// Manages active Data sessions for a client.
/// </summary>
internal class SessionRegistry<T> : IAsyncDisposable
{
    private readonly ConcurrentDictionary<string, TransferSession<T>> _sessions = new();
    private readonly ILogger _logger;

    public SessionRegistry(ILogger logger)
    {
        _logger = logger;
    }

    public int Count => _sessions.Count;
    public IEnumerable<IDataSession> Sessions => _sessions.Values.Select(s => s.Session);

    public TransferSession<T> GetOrAdd(string transferId, Func<string, TransferSession<T>> valueFactory)
    {
        return _sessions.GetOrAdd(transferId, valueFactory);
    }

    public bool TryGetSession(string transferId, out TransferSession<T>? session)
    {
        return _sessions.TryGetValue(transferId, out session);
    }

    public bool TryRemove(string transferId, out TransferSession<T>? session)
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
