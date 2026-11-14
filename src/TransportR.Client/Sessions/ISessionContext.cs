namespace TransportR.Client.Sessions;

/// <summary>
/// What a session asks of the client that holds it, gathered so that a test can stand in for the
/// client. <see cref="Services.ClientConnection"/> is the implementation.
/// </summary>
internal interface ISessionContext : ISessionConfig, IStreamTransport, IControlPlane
{
}
