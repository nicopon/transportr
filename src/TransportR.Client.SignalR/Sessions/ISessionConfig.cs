using Microsoft.Extensions.Logging;
using Microsoft.IO;
using TransportR.Client.SignalR.Configuration;
using TransportR.Interfaces;

namespace TransportR.Client.SignalR.Sessions;

/// <summary>
/// The session dependencies that do not depend on the item type.
/// </summary>
/// <remarks>
/// Split from <see cref="ISessionConfig{T}"/> so that the parts of a session which never touch a
/// payload — <see cref="HttpStreamManager"/>, <see cref="ReconnectionHandler"/> — are not made
/// generic just to reach a logger.
/// </remarks>
public interface ISessionConfig
{
    ILogger Logger { get; }
    int BatchFlushIntervalMs { get; }
    ClientOptions Options { get; }
    RecyclableMemoryStreamManager StreamManager { get; }
}

/// <summary>
/// Adds the serializer, which is where the item type enters a session.
/// </summary>
public interface ISessionConfig<T> : ISessionConfig
{
    IBatchSerializer<T> Serializer { get; }
}
