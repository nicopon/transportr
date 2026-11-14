using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.Logging;
using TransportR.Hub.Interfaces;
using TransportR.Hub.Models;

namespace TransportR.Hub.Services;


/// <summary>
/// Manages connection bridges for transfer streaming.
/// Thread-safe implementation using ConcurrentDictionary.
/// </summary>
internal class BridgeManager : IBridgeManager
{
    private readonly ConcurrentDictionary<string, ConnectionBridge> _bridges = new();
    private readonly ILoggerFactory _loggerFactory;
    private readonly TimeProvider _time;

    public BridgeManager(ILoggerFactory loggerFactory, TimeProvider? timeProvider = null)
    {
        _loggerFactory = loggerFactory;
        _time = timeProvider ?? TimeProvider.System;
    }

    public IReadOnlyDictionary<string, ConnectionBridge> Bridges => _bridges;

    public ConnectionBridge GetOrCreate(string transferId, ConnectionType type)
    {
        return _bridges.GetOrAdd(
            IBridgeManager.KeyFor(transferId, type),
            _ => new ConnectionBridge(transferId, type, _loggerFactory.CreateLogger<ConnectionBridge>(), _time));
    }

    public bool TryRemove(string bridgeKey, [NotNullWhen(true)] out ConnectionBridge? bridge)
    {
        return _bridges.TryRemove(bridgeKey, out bridge);
    }
}
