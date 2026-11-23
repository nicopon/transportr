using Microsoft.Extensions.Diagnostics.HealthChecks;
using TransportR.Hub.Services;

namespace TransportR.Hub.Health;

internal sealed class DataHubHealthCheck : IHealthCheck
{
    private readonly IStateStore _stateStore;
    private readonly HubDrainState _drain;

    public DataHubHealthCheck(IStateStore stateStore, HubDrainState drain)
    {
        _stateStore = stateStore;
        _drain = drain;
    }

    public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        var failureStatus = context.Registration?.FailureStatus ?? HealthStatus.Unhealthy;
        if (_drain.IsDraining)
        {
            return new HealthCheckResult(failureStatus, "TransportR hub is draining.");
        }

        var probe = _stateStore.GetClientAsync(Guid.Empty);
        try
        {
            await probe.WaitAsync(cancellationToken).ConfigureAwait(false);
            return HealthCheckResult.Healthy();
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            _ = probe.ContinueWith(
                static t => _ = t.Exception,
                CancellationToken.None,
                TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
                TaskScheduler.Default);
            throw;
        }
        catch (Exception ex)
        {
            return new HealthCheckResult(failureStatus, "TransportR state store probe failed.", ex);
        }
    }
}
