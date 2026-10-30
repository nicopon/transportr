using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using TransportR.Client.Sessions;
using TransportR.Models;

namespace TransportR.Client.Internal;

/// <summary>
/// Manages active Data sessions for a client.
/// </summary>
internal class SessionRegistry<T> : IAsyncDisposable
{
    private readonly ConcurrentDictionary<string, IDataSession<T>> _sessions = new();
    private readonly ILogger _logger;

    public SessionRegistry(ILogger logger)
    {
        _logger = logger;
    }

    public int Count => _sessions.Count;
    public IEnumerable<IDataSession<T>> Sessions => _sessions.Values;

    public IDataSession<T> GetOrAdd(string transferId, Func<string, IDataSession<T>> valueFactory)
    {
        return _sessions.GetOrAdd(transferId, valueFactory);
    }

    public bool TryGetSession(string transferId, out IDataSession<T>? session)
    {
        return _sessions.TryGetValue(transferId, out session);
    }
    
    public bool TryRemove(string transferId, out IDataSession<T>? session)
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
        foreach (var session in _sessions.Values)
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
