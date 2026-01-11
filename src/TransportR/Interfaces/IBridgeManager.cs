using TransportR.Models;

namespace TransportR.Interfaces;

/// <summary>
/// Manages connection bridges for transfer streaming.
/// Bridges coordinate the sender and receiver HTTP streams for a transfer.
/// </summary>
public interface IBridgeManager
{
    /// <summary>
    /// Gets all active bridges (for monitoring and cleanup).
    /// </summary>
    IReadOnlyDictionary<string, ConnectionBridge> Bridges { get; }

    /// <summary>
    /// Gets or creates a bridge for the specified key.
    /// </summary>
    /// <param name="bridgeKey">The bridge key (format: "transferId:connectionType").</param>
    /// <returns>The existing or newly created bridge.</returns>
    ConnectionBridge GetOrCreate(string bridgeKey);

    /// <summary>
    /// Attempts to remove a bridge.
    /// </summary>
    /// <param name="bridgeKey">The bridge key to remove.</param>
    /// <returns>True if removed, false if not found.</returns>
    bool TryRemove(string bridgeKey);
}
