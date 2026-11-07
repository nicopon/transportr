using Microsoft.Extensions.Logging;
using Microsoft.IO;
using TransportR.Client.Batching;
using TransportR.Client.Configuration;
using TransportR.Interfaces;

namespace TransportR.Client.Sessions;

/// <summary>
/// The session dependencies that do not depend on the item type.
/// </summary>
/// <remarks>
/// Split from <see cref="ISessionConfig{T}"/> so that the parts of a session which never touch a
/// payload — <see cref="HttpStreamManager"/>, <see cref="ReconnectionHandler"/> — are not made
/// generic just to reach a logger.
/// </remarks>
internal interface ISessionConfig
{
    ILogger Logger { get; }
    int BatchFlushIntervalMs { get; }
    ClientOptions Options { get; }
    RecyclableMemoryStreamManager StreamManager { get; }
}

/// <summary>
/// Adds the serializer and the receiving sink, which is where the item type enters a session.
/// </summary>
internal interface ISessionConfig<T> : ISessionConfig
{
    /// <summary>
    /// <c>null</c> in the byte-stream mode (<see cref="ClientOptions.ByteStreamBatchBytes"/>), where
    /// the payload crosses the wire as it is.
    /// </summary>
    IBatchSerializer<T>? Serializer { get; }

    /// <summary>
    /// A new sink for a session that starts receiving, of the mode the client was built for. The
    /// session keeps it across reconnections.
    /// </summary>
    IReceiveSink<T> CreateReceiveSink();
}
