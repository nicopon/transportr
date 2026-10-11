using Microsoft.Extensions.Logging;
using TransportR.Interfaces;

namespace TransportR.Services;

public class TransferTerminator : ITransferTerminator
{
    private readonly ITransferManager _transferManager;
    private readonly IBridgeManager _bridgeManager;
    private readonly IStateStore _stateStore;
    private readonly ITransferNotifier _notifier;
    private readonly ILogger<TransferTerminator> _logger;

    public TransferTerminator(
        ITransferManager transferManager,
        IBridgeManager bridgeManager,
        IStateStore stateStore,
        ITransferNotifier notifier,
        ILogger<TransferTerminator> logger)
    {
        _transferManager = transferManager;
        _bridgeManager = bridgeManager;
        _stateStore = stateStore;
        _notifier = notifier;
        _logger = logger;
    }

    public async Task TerminateAsync(string transferId, CancellationToken cancellationToken = default)
    {
        try
        {
            // 1. Get participants
            var participants = await _transferManager.GetParticipantsAsync(transferId);

            // 2. Clear state in TransferManager (in-memory state and its store copy)
            await _transferManager.RemoveTransferAsync(transferId);

            // 3. Remove bridges
            _bridgeManager.TryRemove(IBridgeManager.KeyFor(transferId, ConnectionType.Data));
            _bridgeManager.TryRemove(IBridgeManager.KeyFor(transferId, ConnectionType.Control));

            // 4. Notify Participants via Notifier
            if (participants != null)
            {
                await NotifyClientAsync(participants.SenderId, transferId, cancellationToken);
                await NotifyClientAsync(participants.ReceiverId, transferId, cancellationToken);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to terminate transfer {TransferId}", transferId);
        }
    }

    private async Task NotifyClientAsync(Guid clientId, string transferId, CancellationToken ct)
    {
        try
        {
            var client = await _stateStore.GetClientAsync(clientId);
            if (client != null)
            {
                // Use CancelTransfer for timeout - dataplane handles graceful shutdown
                await _notifier.NotifyCancelTransferAsync(client.ConnectionId, transferId, ct);
            }
        }
        catch (Exception ex)
        {
            _logger.LogDebug("Could not notify client {ClientId} of termination: {Message}", clientId, ex.Message);
        }
    }
}
