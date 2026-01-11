using Microsoft.AspNetCore.SignalR.Client;
using Microsoft.Extensions.Logging;
using Microsoft.IO;
using TransportR.Client.SignalR.Configuration;
using TransportR.Interfaces;

namespace TransportR.Client.SignalR.Sessions;

/// <summary>
/// Interface exposing the session context dependencies from the parent SignalRDataClient.
/// Enables dependency injection and unit testing of DataSession.
/// </summary>
public interface ISessionContext : ISessionConfig, IStreamTransport, ISignalRInteraction
{
    // No additional members needed, functions as an aggregator for the Client implementation
    // The specific logic (DataSession, HttpStreamManager) uses the narrow interfaces.
}
