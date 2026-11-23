using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using TransportR.Hub.Diagnostics;

namespace TransportR.Hub.Services;

internal sealed class HubDrainService : IHostedService
{
    private static readonly TimeSpan DrainPollInterval = TimeSpan.FromMilliseconds(100);

    private readonly HubDrainState _drain;
    private readonly BridgeManager _bridgeManager;
    private readonly HubMetrics _metrics;
    private readonly TimeSpan _drainTimeout;
    private readonly IHostApplicationLifetime _lifetime;
    private readonly TimeProvider _time;
    private readonly ILogger<HubDrainService> _logger;
    private CancellationTokenRegistration _stoppingRegistration;

    public HubDrainService(
        HubDrainState drain,
        BridgeManager bridgeManager,
        HubMetrics metrics,
        IOptions<DataPlaneConfiguration> dataPlane,
        IHostApplicationLifetime lifetime,
        TimeProvider time,
        ILogger<HubDrainService> logger)
    {
        _drain = drain;
        _bridgeManager = bridgeManager;
        _metrics = metrics;
        _drainTimeout = TimeSpan.FromSeconds(dataPlane.Value.DrainTimeoutSeconds);
        _lifetime = lifetime;
        _time = time;
        _logger = logger;
    }

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

        if (_drainTimeout <= TimeSpan.Zero || _bridgeManager.Bridges.Count == 0)
        {
            return;
        }

        _logger.LogInformation(
            "Waiting up to {DrainTimeoutSeconds}s for {BridgeCount} active bridge(s) to drain...",
            _drainTimeout.TotalSeconds, _bridgeManager.Bridges.Count);

        using var bound = new CancellationTokenSource(_drainTimeout, _time);
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(bound.Token, cancellationToken);

        while (_bridgeManager.Bridges.Count > 0 && !linked.IsCancellationRequested)
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

        if (_bridgeManager.Bridges.Count == 0)
        {
            _logger.LogInformation("All active bridges drained before shutdown.");
        }
        else
        {
            _logger.LogWarning(
                "Hub drain finished with {BridgeCount} active bridge(s) still open.",
                _bridgeManager.Bridges.Count);
        }
    }
}
