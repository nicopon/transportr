using TransportR.Hub.Interfaces;
using TransportR.Protocol;

namespace TransportR.Hub.Services;

/// <summary>
/// Tells a client through its event stream. A client whose stream another instance holds is not told;
/// its streams learn of the end from the data plane.
/// </summary>
internal class ClientStreamNotifier : ITransferNotifier
{
    private readonly ClientStreams _streams;

    public ClientStreamNotifier(ClientStreams streams)
    {
        _streams = streams;
    }

    public Task NotifyCancelTransferAsync(string connectionId, string transferId, CancellationToken cancellationToken = default)
    {
        if (!string.IsNullOrEmpty(connectionId))
        {
            _streams.TrySend(connectionId, ControlPlane.CancelTransfer, new CancelTransferEvent(transferId));
        }

        return Task.CompletedTask;
    }
}
