using Microsoft.AspNetCore.SignalR;
using TransportR.Hub.SignalR.Hubs;
using TransportR.Interfaces;

namespace TransportR.Hub.SignalR.Services;

/// <summary>
/// Hub-based implementation of ITransferNotifier.
/// </summary>
internal class SignalRTransferNotifier : ITransferNotifier
{
    private readonly IHubContext<CommandHub> _hubContext;

    public SignalRTransferNotifier(IHubContext<CommandHub> hubContext)
    {
        _hubContext = hubContext;
    }

    public async Task NotifyCancelTransferAsync(string connectionId, string transferId, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrEmpty(connectionId)) return;
        
        await _hubContext.Clients.Client(connectionId)
            .SendAsync(nameof(ICommandClient.CancelTransfer), transferId, cancellationToken);
    }
}
