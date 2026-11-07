namespace TransportR.Client.Sessions;

/// <summary>
/// Interface exposing the session context dependencies from the parent TransportRClient.
/// Enables dependency injection and unit testing of DataSession.
/// </summary>
internal interface ISessionContext : ISessionConfig, IStreamTransport, IControlPlane
{
    // No additional members needed, functions as an aggregator for the Client implementation
    // The specific logic (DataSession, HttpStreamManager) uses the narrow interfaces.
}
