using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using TransportR.Hub.SignalR.Configuration;

namespace TransportR.Hub.SignalR.Access;

/// <summary>
/// Runs <see cref="IAccessReconciler.ReconcileAllAsync"/> on a schedule: the one code path that
/// closes a connection whose token ran out, and the backstop for a right the host withdrew without
/// calling the reconciler.
/// </summary>
public sealed class AccessSweepService : BackgroundService
{
    private readonly IAccessReconciler _reconciler;
    private readonly TimeSpan _interval;
    private readonly TimeProvider _time;
    private readonly ILogger<AccessSweepService> _logger;

    public AccessSweepService(
        IAccessReconciler reconciler,
        IOptions<AccessOptions> options,
        TimeProvider timeProvider,
        ILogger<AccessSweepService> logger)
    {
        _reconciler = reconciler;
        _interval = TimeSpan.FromSeconds(options.Value.AccessSweepIntervalSeconds);
        _time = timeProvider;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("AccessSweepService started. Interval={Interval}", _interval);

        using var timer = new PeriodicTimer(_interval, _time);

        while (await timer.WaitForNextTickAsync(stoppingToken))
        {
            try
            {
                await _reconciler.ReconcileAllAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error during the access sweep");
            }
        }
    }
}
