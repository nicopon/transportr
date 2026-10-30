using System.Threading.Tasks;

namespace TransportR.Interfaces;

/// <summary>
/// Abstraction for notifying clients about transfer lifecycle events (e.g. cancellation, shutdown).
/// </summary>
internal interface ITransferNotifier
{
    /// <summary>
    /// Notifies a client to cancel a specific transfer immediate.
    /// </summary>
    Task NotifyCancelTransferAsync(string connectionId, string transferId, CancellationToken cancellationToken = default);
}
