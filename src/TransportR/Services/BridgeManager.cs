using Microsoft.Extensions.Logging;
using TransportR.Models;
using TransportR.Interfaces;
using System.Collections.Concurrent;

namespace TransportR.Services;


/// <summary>
/// Manages connection bridges for transfer streaming.
/// Thread-safe implementation using ConcurrentDictionary.
/// </summary>
public class BridgeManager : IBridgeManager
{
    private readonly ConcurrentDictionary<string, ConnectionBridge> _bridges = new();
    private readonly ILoggerFactory _loggerFactory;

    public BridgeManager(ILoggerFactory loggerFactory)
    {
        _loggerFactory = loggerFactory;
    }

    public IReadOnlyDictionary<string, ConnectionBridge> Bridges => _bridges;

    public ConnectionBridge GetOrCreate(string bridgeKey)
    {
        return _bridges.GetOrAdd(bridgeKey, key => new ConnectionBridge(key, _loggerFactory.CreateLogger<ConnectionBridge>()));
    }

    public bool TryRemove(string bridgeKey)
    {
        return _bridges.TryRemove(bridgeKey, out _);
    }
}
