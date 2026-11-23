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
        if (_drain.IsDraining)
        {
            return HealthCheckResult.Unhealthy("TransportR hub is draining.");
        }

        try
        {
            await _stateStore.GetClientAsync(Guid.Empty).WaitAsync(cancellationToken).ConfigureAwait(false);
            return HealthCheckResult.Healthy();
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            return HealthCheckResult.Unhealthy("TransportR state store probe failed.", ex);
        }
    }
}
