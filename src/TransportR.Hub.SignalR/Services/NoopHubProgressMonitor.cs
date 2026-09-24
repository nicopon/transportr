using TransportR.Interfaces;

namespace TransportR.Hub.SignalR.Services;

/// <summary>
/// Default <see cref="IHubProgressMonitor"/> registered by <c>AddDataHub()</c> when the host
/// registers none of its own; any host registration made before <c>Build()</c> replaces it.
/// </summary>
internal sealed class NoopHubProgressMonitor : IHubProgressMonitor
{
    public void InitializeTransfer(string transferId) { }

    public void OnBytesTransferred(long bytes) { }

    public void CompleteTransfer(string transferId) { }
}
