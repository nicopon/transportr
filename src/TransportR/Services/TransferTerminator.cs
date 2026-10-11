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
            // Participants are read first: removing the transfer takes them with it.
            var participants = await _transferManager.GetParticipantsAsync(transferId);

            await RemoveAsync(transferId);

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

    public async Task RetireAsync(string transferId)
    {
        try
        {
            await RemoveAsync(transferId);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to retire transfer {TransferId}", transferId);
        }
    }

    // The state goes before the bridges: a stream that reopens in between then meets a transfer that
    // is gone (404, which a client in its closing phase reads as the end) instead of a fresh bridge
    // that waits for a peer that will not come.
    private async Task RemoveAsync(string transferId)
    {
        await _transferManager.RemoveTransferAsync(transferId);

        _bridgeManager.TryRemove(IBridgeManager.KeyFor(transferId, ConnectionType.Data));
        _bridgeManager.TryRemove(IBridgeManager.KeyFor(transferId, ConnectionType.Control));
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
