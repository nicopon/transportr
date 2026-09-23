using TransportR.Interfaces;

namespace TransportR.Tests.Infrastructure;

/// <summary>
/// The progress monitor a test hub registers because <c>AddDataHub()</c> does not: without one,
/// every data-plane request fails to activate <c>SignalRDataServer</c> and answers 500, and a
/// client reconnects in a loop instead of failing.
/// </summary>
internal sealed class SilentProgressMonitor : IHubProgressMonitor
{
    public void InitializeTransfer(string transferId) { }

    public void OnBytesTransferred(long bytes) { }

    public void CompleteTransfer(string transferId) { }
}
