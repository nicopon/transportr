using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.Logging;
using TransportR.Hub.Models;

namespace TransportR.Hub.Services;


/// <summary>
/// Manages connection bridges for transfer streaming.
/// Thread-safe implementation using ConcurrentDictionary.
/// </summary>
internal class BridgeManager
{
    private readonly ConcurrentDictionary<string, ConnectionBridge> _bridges = new();
    private readonly ILoggerFactory _loggerFactory;
    private readonly TimeProvider _time;

    public BridgeManager(ILoggerFactory loggerFactory, TimeProvider? timeProvider = null)
    {
        _loggerFactory = loggerFactory;
        _time = timeProvider ?? TimeProvider.System;
    }

    /// <summary>
    /// Builds the key a transfer's bridge for <paramref name="type"/> is held under.
    /// </summary>
    /// <remarks>
    /// The format is the only thing tying together the code that creates a bridge, the code that
    /// looks one up and the code that reaps it. Composed here so that changing it cannot leave one of
    /// them behind.
    /// </remarks>
    public static string KeyFor(string transferId, ConnectionType type) => $"{transferId}:{type}";

    /// <summary>
    /// Gets all active bridges (for monitoring and cleanup).
    /// </summary>
    public IReadOnlyDictionary<string, ConnectionBridge> Bridges => _bridges;

    /// <summary>
    /// Gets or creates <paramref name="transferId"/>'s bridge for <paramref name="type"/>.
    /// </summary>
    public ConnectionBridge GetOrCreate(string transferId, ConnectionType type)
    {
        return _bridges.GetOrAdd(
            KeyFor(transferId, type),
            _ => new ConnectionBridge(transferId, type, _loggerFactory.CreateLogger<ConnectionBridge>(), _time));
    }

    /// <summary>
    /// Attempts to remove a bridge.
    /// </summary>
    /// <param name="bridgeKey">The bridge key to remove.</param>
    /// <param name="bridge">The bridge removed. Its streams run on until it is cancelled.</param>
    /// <returns>True if removed, false if not found.</returns>
    public bool TryRemove(string bridgeKey, [NotNullWhen(true)] out ConnectionBridge? bridge)
    {
        return _bridges.TryRemove(bridgeKey, out bridge);
    }
}
