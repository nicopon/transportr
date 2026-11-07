using Microsoft.Extensions.Logging;
using TransportR.Client.Configuration;

namespace TransportR.Client.Sessions;

/// <summary>
/// The session dependencies that are configuration: a logger and the options of the client.
/// </summary>
/// <remarks>
/// Split from <see cref="ISessionContext"/> so that the parts of a session which never touch the
/// control plane or the transport — <see cref="HttpStreamManager"/>, <see cref="ReconnectionHandler"/>
/// — are not handed more than they use just to reach a logger.
/// </remarks>
internal interface ISessionConfig
{
    ILogger Logger { get; }
    ClientOptions Options { get; }
}
