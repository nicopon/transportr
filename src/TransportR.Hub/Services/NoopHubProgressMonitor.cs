using TransportR.Interfaces;

namespace TransportR.Hub.Services;

/// <summary>
/// Default <see cref="IHubProgressMonitor"/> registered by <c>AddDataHub()</c> when the host
/// registers none of its own; any host registration made before <c>Build()</c> replaces it.
/// </summary>
internal sealed class NoopHubProgressMonitor : IHubProgressMonitor
{
    public void OnBytesTransferred(long bytes) { }
}
