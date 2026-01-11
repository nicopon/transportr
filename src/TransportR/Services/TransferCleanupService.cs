using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using TransportR.Interfaces;
using TransportR.Models;

namespace TransportR.Services;

public class TransferCleanupService : BackgroundService
{
    private readonly ITransferManager _transferManager;
    private readonly ITransferNotifier _notifier;
    private readonly IBridgeManager _bridgeManager;
    private readonly IStateStore _stateStore;
    private readonly ILogger<TransferCleanupService> _logger;
    
    // Configurable timeout (default 60s to be aggressive enough for chaos recovery logic)
    private readonly TimeSpan _transferTimeout = TimeSpan.FromSeconds(60);
    private readonly TimeSpan _checkInterval = TimeSpan.FromSeconds(10);

    public TransferCleanupService(
        ITransferManager transferManager,
        ITransferNotifier notifier,
        IBridgeManager bridgeManager,
        IStateStore stateStore,
        ILogger<TransferCleanupService> logger)
    {
        _transferManager = transferManager;
        _notifier = notifier;
        _bridgeManager = bridgeManager;
        _stateStore = stateStore;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("TransferCleanupService started. Timeout={Timeout}", _transferTimeout);

        using var timer = new PeriodicTimer(_checkInterval);
        
        while (await timer.WaitForNextTickAsync(stoppingToken))
        {
            try
            {
                await CleanupStaleTransfersAsync(stoppingToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error during transfer cleanup");
            }
        }
    }

    private async Task CleanupStaleTransfersAsync(CancellationToken ct)
    {
        var now = DateTime.UtcNow;
        var transfers = _transferManager.GetAllTransfers();

        foreach (var transfer in transfers)
        {
            // Use LastActivityUtc to detect STALLED transfers.
            // This includes Heartbeats and Pings (KeepAlive), preventing premature cleanup during network glitches.
            if (now - transfer.LastActivityUtc > _transferTimeout)
            {
                _logger.LogWarning("Transfer {TransferId} STALLED (LastActivity: {LastActivity}). Terminating...", 
                    transfer.TransferId, transfer.LastActivityUtc);

                await TerminateTransferAsync(transfer.TransferId, ct);
            }
        }
    }

    private async Task TerminateTransferAsync(string transferId, CancellationToken ct)
    {
        try
        {
            // 1. Get participants
            var participants = await _transferManager.GetParticipantsAsync(transferId);

            // 2. Clear state in TransferManager (Checkpoints, InMemory State)
            await _transferManager.TerminateTransferAsync(transferId, "Timeout");

            // 3. Remove bridges
            _bridgeManager.TryRemove($"{transferId}:Data");
            _bridgeManager.TryRemove($"{transferId}:Control");

            // 4. Notify Participants via Notifier
            if (participants != null)
            {
                await NotifyClientAsync(participants.SenderId, transferId, ct);
                await NotifyClientAsync(participants.ReceiverId, transferId, ct);
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
