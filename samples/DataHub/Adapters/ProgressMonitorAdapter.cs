using TransportR.Interfaces;
using DataHub.Services;

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

    public void InitializeTransfer(string transferId)
    {
        _service.InitializeTransfer(transferId);
    }

    public void OnBytesTransferred(long bytes)
    {
        _service.OnBytesTransferred(bytes);
    }

    public void CompleteTransfer(string transferId)
    {
        // No-op: ProgressMonitorService tracks globally, not per-transfer
    }
}
