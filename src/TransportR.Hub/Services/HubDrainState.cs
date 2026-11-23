namespace TransportR.Hub.Services;

internal sealed class HubDrainState
{
    private volatile bool _draining;

    public bool IsDraining => _draining;

    public void BeginDraining() => _draining = true;
}
