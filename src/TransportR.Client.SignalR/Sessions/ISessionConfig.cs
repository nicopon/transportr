using Microsoft.Extensions.Logging;
using Microsoft.IO;
using TransportR.Client.SignalR.Configuration;
using TransportR.Interfaces;

namespace TransportR.Client.SignalR.Sessions;

public interface ISessionConfig
{
    Guid ClientId { get; }
    ILogger Logger { get; }
    IBatchSerializer Serializer { get; }
    int BatchFlushIntervalMs { get; }
    ClientOptions Options { get; }
    RecyclableMemoryStreamManager StreamManager { get; }
}
