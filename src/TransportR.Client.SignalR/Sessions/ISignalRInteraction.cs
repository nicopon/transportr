using Microsoft.AspNetCore.SignalR.Client;

namespace TransportR.Client.SignalR.Sessions;

public interface ISignalRInteraction
{
    HubConnection HubConnection { get; }
    void RemoveSession(string transferId);
}
