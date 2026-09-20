using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using TransportR.Configuration;
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
    
    // No activity at all (not even a heartbeat): the peer is gone.
    private readonly TimeSpan _inactivityTimeout;

    // Alive but not advancing. Opt-in: null disables the check entirely.
    private readonly TimeSpan? _stalledProgressTimeout;

    private readonly TimeSpan _checkInterval = TimeSpan.FromSeconds(10);

    public TransferCleanupService(
        ITransferManager transferManager,
        ITransferNotifier notifier,
        IBridgeManager bridgeManager,
        IStateStore stateStore,
        IOptions<DataPlaneConfiguration> config,
        ILogger<TransferCleanupService> logger)
    {
        _inactivityTimeout = TimeSpan.FromSeconds(config.Value.TransferInactivityTimeoutSeconds);
        _stalledProgressTimeout = config.Value.StalledProgressTimeoutSeconds is { } stalled
            ? TimeSpan.FromSeconds(stalled)
            : null;
        _transferManager = transferManager;
        _notifier = notifier;
        _bridgeManager = bridgeManager;
        _stateStore = stateStore;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation(
            "TransferCleanupService started. InactivityTimeout={Inactivity}, StalledProgressTimeout={Stalled}",
            _inactivityTimeout,
            _stalledProgressTimeout?.ToString() ?? "disabled");

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
            // LastActivityUtc covers Heartbeats and Pings (KeepAlive), so it only fires once the
            // peer has gone silent entirely - that is what keeps a network glitch from killing a
            // healthy transfer.
            if (now - transfer.LastActivityUtc > _inactivityTimeout)
            {
                _logger.LogWarning("Transfer {TransferId} INACTIVE (LastActivity: {LastActivity}). Terminating...",
                    transfer.TransferId, transfer.LastActivityUtc);

                await TerminateTransferAsync(transfer.TransferId, ct);
                continue;
            }

            // A peer that heartbeats but never advances is invisible to the check above. Opt-in,
            // because a reconnection looks exactly like this from here.
            if (_stalledProgressTimeout is { } stalledTimeout &&
                now - transfer.LastProgressUtc > stalledTimeout)
            {
                _logger.LogWarning(
                    "Transfer {TransferId} STALLED: alive but no progress since {LastProgress} " +
                    "(ItemsSent={ItemsSent}, AcksReceived={AcksReceived}, ItemsReceived={ItemsReceived}, AcksSent={AcksSent}). Terminating...",
                    transfer.TransferId, transfer.LastProgressUtc,
                    transfer.ItemsSent, transfer.AcksReceived, transfer.ItemsReceived, transfer.AcksSent);

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
            await _transferManager.TerminateTransferAsync(transferId);

            // 3. Remove bridges
            _bridgeManager.TryRemove(IBridgeManager.KeyFor(transferId, ConnectionType.Data));
            _bridgeManager.TryRemove(IBridgeManager.KeyFor(transferId, ConnectionType.Control));

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
