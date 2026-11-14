using Microsoft.IO;
using TransportR.Client.Configuration;

namespace TransportR.Client.Batching;

/// <summary>
/// Makes the ends of a session that carry the item type.
/// </summary>
internal static class SessionPorts
{
    public static (ISendPort<T> Send, IReceivePort<T> Receive) Create<T>(
        ClientOptions options,
        IBatchSerializer<T> serializer,
        RecyclableMemoryStreamManager streams,
        int batchSize) =>
        (
            new ItemBatchSource<T>(serializer, streams, batchSize, options.BatchFlushIntervalMs),
            new ReceivePort<T>(() => new ItemChunkSink<T>(new ReceiveBuffer<T>(options.ReceiveBufferCapacity), serializer))
        );
}
