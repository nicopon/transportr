using Microsoft.Extensions.Logging;
using TransportR.Hub.Streams;
using TransportR.Interfaces;
using TransportR.Models;

namespace TransportR.Services;

internal class TransferTerminator : ITransferTerminator, ITransferRetirer
{
    private readonly ITransferManager _transferManager;
    private readonly IBridgeManager _bridgeManager;
    private readonly IStateStore _stateStore;
    private readonly ITransferNotifier _notifier;
    private readonly UploadEpochs _epochs;
    private readonly ILogger<TransferTerminator> _logger;

    public TransferTerminator(
        ITransferManager transferManager,
        IBridgeManager bridgeManager,
        IStateStore stateStore,
        ITransferNotifier notifier,
        UploadEpochs epochs,
        ILogger<TransferTerminator> logger)
    {
        _transferManager = transferManager;
        _bridgeManager = bridgeManager;
        _stateStore = stateStore;
        _notifier = notifier;
        _epochs = epochs;
        _logger = logger;
    }

    public async Task TerminateAsync(string transferId, CancellationToken cancellationToken = default)
    {
        try
        {
            // Participants are read first: removing the transfer takes them with it.
            var participants = await _transferManager.GetParticipantsAsync(transferId);

            var bridges = await RemoveAsync(transferId);

            if (participants != null)
            {
                await NotifyClientAsync(participants.SenderId, transferId, cancellationToken);
                await NotifyClientAsync(participants.ReceiverId, transferId, cancellationToken);
            }

            // A participant is free to ignore the event: the streams it holds open are cut here, or
            // they go on carrying data through bridges nothing tracks any more. The event goes out
            // first, so that a client reading it before its streams break says the hub ended the
            // transfer rather than that a stream failed.
            foreach (var bridge in bridges)
            {
                bridge.Cancel();
                _epochs.Abandon(bridge.BridgeKey);
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
    // that waits for a peer that will not come. The bridges removed are left running: after a clean
    // end, the receiver's data GET is still on its way out, and cutting it would cut the end.
    private async Task<List<ConnectionBridge>> RemoveAsync(string transferId)
    {
        await _transferManager.RemoveTransferAsync(transferId);

        var removed = new List<ConnectionBridge>(2);
        if (_bridgeManager.TryRemove(IBridgeManager.KeyFor(transferId, ConnectionType.Data), out var data)) removed.Add(data);
        if (_bridgeManager.TryRemove(IBridgeManager.KeyFor(transferId, ConnectionType.Control), out var control)) removed.Add(control);
        return removed;
    }

    private async Task NotifyClientAsync(Guid clientId, string transferId, CancellationToken ct)
    {
        try
        {
            var client = await _stateStore.GetClientAsync(clientId);
            if (client != null)
            {
                await _notifier.NotifyCancelTransferAsync(client.ConnectionId, transferId, ct);
            }
        }
        catch (Exception ex)
        {
            _logger.LogDebug("Could not notify client {ClientId} of termination: {Message}", clientId, ex.Message);
        }
    }
}
