using Microsoft.IO;
using TransportR.Client.Configuration;
using TransportR.Interfaces;

namespace TransportR.Client.Batching;

/// <summary>
/// Makes the ends of a session that carry the item type, of the mode the client was built for: the
/// one place the mode is chosen.
/// </summary>
internal static class SessionPorts
{
    /// <summary>
    /// The builder gives a byte-stream client <c>byte[]</c> as its item type, and an item client its
    /// serializer; a client built any other way is refused here, as its first session is made.
    /// </summary>
    /// <exception cref="InvalidOperationException">
    /// A byte-stream client of another item type, or an item client without a serializer.
    /// </exception>
    public static (ISendPort<T> Send, IReceivePort<T> Receive) Create<T>(
        ClientOptions options,
        IBatchSerializer<T>? serializer,
        RecyclableMemoryStreamManager streams,
        int batchSize)
    {
        if (options.ByteStreamBatchBytes is { } batchBytes)
        {
            if (typeof(T) != typeof(byte[]))
                throw new InvalidOperationException("The byte-stream mode carries byte[] only.");

            return (
                (ISendPort<T>)(object)new ByteBatchSource(batchBytes, options.BatchFlushIntervalMs, streams),
                (IReceivePort<T>)(object)new ReceivePort<byte[]>(
                    isByteStream: true,
                    () => new ByteChunkSink(new ReceiveBuffer<ByteChunk>(options.ReceiveBufferCapacity))));
        }

        if (serializer is null)
            throw new InvalidOperationException("A serializer is required outside the byte-stream mode.");

        return (
            new ItemBatchSource<T>(serializer, streams, batchSize, options.BatchFlushIntervalMs),
            new ReceivePort<T>(
                isByteStream: false,
                () => new ItemChunkSink<T>(new ReceiveBuffer<T>(options.ReceiveBufferCapacity), serializer)));
    }
}
