using Microsoft.AspNetCore.SignalR.Client;

namespace TransportR.Client.Sessions;

internal interface ISignalRInteraction
{
    HubConnection HubConnection { get; }
    void RemoveSession(string transferId);
}
