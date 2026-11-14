using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using TransportR.Hub.Interfaces;
using TransportR.Hub.Models;

namespace TransportR.Hub.Services;

/// <summary>
/// Ends the transfer of a bridge left short of <see cref="BridgeState.Active"/> for
/// <c>BridgeTimeoutSeconds</c> through <see cref="ITransferTerminator"/>, which cancels the bridge and
/// so releases the requests parked on it.
/// </summary>
internal class BridgeCleanupService : BackgroundService
{
    private readonly ILogger<BridgeCleanupService> _logger;
    private readonly ITransferTerminator _terminator;
    private readonly BridgeManager _bridgeManager;
    private readonly DataPlaneConfiguration _config;
    private readonly TimeProvider _time;
    private readonly TimeSpan _checkInterval = TimeSpan.FromSeconds(5);

    public BridgeCleanupService(
        ILogger<BridgeCleanupService> logger,
        ITransferTerminator terminator,
        BridgeManager bridgeManager,
        IOptions<DataPlaneConfiguration> config,
        TimeProvider? timeProvider = null)
    {
        _time = timeProvider ?? TimeProvider.System;
        _logger = logger;
        _terminator = terminator;
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

            // An Active bridge is judged by TransferCleanupService, on the bytes its streams carry and
            // the transfer's heartbeats; this sweep only ends a bridge stuck waiting for a peer.
            if (bridge.State == BridgeState.Active)
                continue;

            var idleTime = now - bridge.LastActivity;
            if (idleTime < timeout)
                continue;

            _logger.LogWarning("Bridge {BridgeKey} timed out after {IdleSeconds}s of inactivity. Ending its transfer.",
                bridge.BridgeKey, idleTime.TotalSeconds);

            await _terminator.TerminateAsync(bridge.TransferId, ct).ConfigureAwait(false);
        }
    }
}
