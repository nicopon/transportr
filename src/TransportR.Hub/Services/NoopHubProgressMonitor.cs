namespace TransportR.Hub.Services;

/// <summary>
/// Default <see cref="IHubProgressMonitor"/> registered by <c>AddDataHub()</c> when the host
/// registers none of its own; a monitor the host registers, before or after that call, replaces it.
/// </summary>
internal sealed class NoopHubProgressMonitor : IHubProgressMonitor
{
    public void OnBytesTransferred(long bytes) { }
}
