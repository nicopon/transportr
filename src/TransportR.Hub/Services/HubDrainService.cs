using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using TransportR.Hub.Interfaces;

namespace TransportR.Hub.Services;

internal sealed class HubDrainService : IHostedService
{
    private static readonly TimeSpan DrainPollInterval = TimeSpan.FromMilliseconds(100);

    private readonly HubDrainState _drain;
    private readonly BridgeManager _bridgeManager;
    private readonly ITransferManager _transferManager;
    private readonly TimeSpan _drainTimeout;
    private readonly IHostApplicationLifetime _lifetime;
    private readonly TimeProvider _time;
    private readonly ILogger<HubDrainService> _logger;
    private CancellationTokenRegistration _stoppingRegistration;

    public HubDrainService(
        HubDrainState drain,
        BridgeManager bridgeManager,
        ITransferManager transferManager,
        IOptions<DataPlaneConfiguration> dataPlane,
        IHostApplicationLifetime lifetime,
        TimeProvider time,
        ILogger<HubDrainService> logger)
    {
        _drain = drain;
        _bridgeManager = bridgeManager;
        _transferManager = transferManager;
        _drainTimeout = TimeSpan.FromSeconds(dataPlane.Value.DrainTimeoutSeconds);
        _lifetime = lifetime;
        _time = time;
        _logger = logger;
    }

    private bool HasActiveWork => _bridgeManager.Bridges.Count > 0 || _transferManager.ActiveCount > 0;

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _stoppingRegistration = _lifetime.ApplicationStopping.Register(() =>
        {
            _drain.BeginDraining();
            _logger.LogInformation("TransportR hub is draining: refusing new transfers.");
        });
        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        _drain.BeginDraining();
        await _stoppingRegistration.DisposeAsync().ConfigureAwait(false);

        if (_drainTimeout <= TimeSpan.Zero || !HasActiveWork)
        {
            return;
        }

        _logger.LogInformation(
            "Waiting up to {DrainTimeoutSeconds}s for {BridgeCount} active bridge(s) and {TransferCount} active transfer(s) to drain...",
            _drainTimeout.TotalSeconds, _bridgeManager.Bridges.Count, _transferManager.ActiveCount);

        using var bound = new CancellationTokenSource(_drainTimeout, _time);
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(bound.Token, cancellationToken);

        while (HasActiveWork && !linked.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(DrainPollInterval, _time, linked.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }

        if (!HasActiveWork)
        {
            _logger.LogInformation("All active transfers and bridges drained before shutdown.");
        }
        else
        {
            _logger.LogWarning(
                "Hub drain finished with {BridgeCount} active bridge(s) and {TransferCount} active transfer(s) still open.",
                _bridgeManager.Bridges.Count, _transferManager.ActiveCount);
        }
    }
}
