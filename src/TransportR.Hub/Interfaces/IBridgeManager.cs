using System.Diagnostics.CodeAnalysis;
using TransportR.Models;

namespace TransportR.Interfaces;

/// <summary>
/// Manages connection bridges for transfer streaming.
/// Bridges coordinate the sender and receiver HTTP streams for a transfer.
/// </summary>
internal interface IBridgeManager
{
    /// <summary>
    /// Builds the key a transfer's bridge for <paramref name="type"/> is held under.
    /// </summary>
    /// <remarks>
    /// The format is the only thing tying together the code that creates a bridge, the code that
    /// looks one up and the code that reaps it. Composed here so that changing it cannot leave one of
    /// them behind.
    /// </remarks>
    static string KeyFor(string transferId, ConnectionType type) => $"{transferId}:{type}";

    /// <summary>
    /// Gets all active bridges (for monitoring and cleanup).
    /// </summary>
    IReadOnlyDictionary<string, ConnectionBridge> Bridges { get; }

    /// <summary>
    /// Gets or creates <paramref name="transferId"/>'s bridge for <paramref name="type"/>.
    /// </summary>
    ConnectionBridge GetOrCreate(string transferId, ConnectionType type);

    /// <summary>
    /// Attempts to remove a bridge.
    /// </summary>
    /// <param name="bridgeKey">The bridge key to remove.</param>
    /// <param name="bridge">The bridge removed. Its streams run on until it is cancelled.</param>
    /// <returns>True if removed, false if not found.</returns>
    bool TryRemove(string bridgeKey, [NotNullWhen(true)] out ConnectionBridge? bridge);
}
