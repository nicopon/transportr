using DataHub.Services;
using TransportR.Interfaces;

namespace DataHub.Adapters;

/// <summary>
/// Adapter that bridges <see cref="IHubProgressMonitor"/> interface to <see cref="ProgressMonitorService"/>.
/// </summary>
public class ProgressMonitorAdapter : IHubProgressMonitor
{
    private readonly ProgressMonitorService _service;

    public ProgressMonitorAdapter(ProgressMonitorService service)
    {
        _service = service;
    }

    public void OnBytesTransferred(long bytes)
    {
        _service.OnBytesTransferred(bytes);
    }
}
