using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using TransportR.Configuration;
using TransportR.Interfaces;
using TransportR.Models;

namespace TransportR.Services;

/// <summary>
/// Background service that cleans up stale bridges after timeout.
/// Sends CancelTransfer to both parties when a bridge times out.
/// </summary>
public class BridgeCleanupService : BackgroundService
{
    private readonly ILogger<BridgeCleanupService> _logger;
    private readonly ITransferNotifier _notifier;
    private readonly IStateStore _stateStore;
    private readonly ITransferManager _transferManager;
    private readonly IBridgeManager _bridgeManager;
    private readonly DataPlaneConfiguration _config;
    private readonly TimeProvider _time;
    private readonly TimeSpan _checkInterval = TimeSpan.FromSeconds(5);

    public BridgeCleanupService(
        ILogger<BridgeCleanupService> logger,
        ITransferNotifier notifier,
        IStateStore stateStore,
        ITransferManager transferManager,
        IBridgeManager bridgeManager,
        IOptions<DataPlaneConfiguration> config,
        TimeProvider? timeProvider = null)
    {
        _time = timeProvider ?? TimeProvider.System;
        _logger = logger;
        _notifier = notifier;
        _stateStore = stateStore;
        _transferManager = transferManager;
        _bridgeManager = bridgeManager;
        _config = config.Value;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("BridgeCleanupService started. Timeout: {Timeout}s", _config.BridgeTimeoutSeconds);
        
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await CleanupStaleBridgesAsync(stoppingToken);
                await Task.Delay(_checkInterval, _time, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in bridge cleanup loop");
                await Task.Delay(TimeSpan.FromSeconds(10), _time, stoppingToken);
            }
        }
        
        _logger.LogInformation("BridgeCleanupService stopped");
    }

    private async Task CleanupStaleBridgesAsync(CancellationToken ct)
    {
        var timeout = TimeSpan.FromSeconds(_config.BridgeTimeoutSeconds);
        var now = _time.GetUtcNow().UtcDateTime;
        
        foreach (var kvp in _bridgeManager.Bridges)
        {
            var bridge = kvp.Value;
            
            // If the bridge is Active, we assume it's healthy (or managed by TCP keepalives)
            // We only cleanup bridges that are stuck waiting for a peer or idle.
            if (bridge.State == BridgeState.Active)
                continue;
            
            var idleTime = now - bridge.LastActivity;
            if (idleTime < timeout)
                continue;
            
            _logger.LogWarning("Bridge {BridgeKey} timed out after {IdleSeconds}s of inactivity. Cancelling.", 
                bridge.BridgeKey, idleTime.TotalSeconds);
            
            bridge.Cancel();
            _bridgeManager.TryRemove(bridge.BridgeKey);
            
            var transferId = bridge.BridgeKey.Split(':')[0];
            await NotifyCancelTransferAsync(transferId, ct).ConfigureAwait(false);
        }
    }

    private async Task NotifyCancelTransferAsync(string transferId, CancellationToken ct)
    {
        try
        {
            var participants = await _transferManager.GetParticipantsAsync(transferId);
            if (participants == null)
            {
                _logger.LogWarning("Cannot notify CancelTransfer: no participants found for transfer {TransferId}", transferId);
                return;
            }

            var senderClient = await _stateStore.GetClientAsync(participants.SenderId);
            if (senderClient != null)
            {
                await _notifier.NotifyCancelTransferAsync(senderClient.ConnectionId, transferId, ct);
                _logger.LogInformation("Sent CancelTransfer to sender {SenderId} for transfer {TransferId}", 
                    participants.SenderId, transferId);
            }

            var receiverClient = await _stateStore.GetClientAsync(participants.ReceiverId);
            if (receiverClient != null)
            {
                await _notifier.NotifyCancelTransferAsync(receiverClient.ConnectionId, transferId, ct);
                _logger.LogInformation("Sent CancelTransfer to receiver {ReceiverId} for transfer {TransferId}", 
                    participants.ReceiverId, transferId);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to send CancelTransfer for transfer {TransferId}", transferId);
        }
    }
}

