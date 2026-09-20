using TransportR.Interfaces;

using Microsoft.IO;

namespace TransportR.Protocol;

/// <summary>
/// Relays <typeparamref name="T"/> to both ends of the configured serializer. It carries the
/// constraint; it does not decide it.
/// </summary>
public class BatchProtocol<T>
{
    private readonly IBatchSerializer<T> _serializer;

    public BatchProtocol(IBatchSerializer<T> serializer)
    {
        _serializer = serializer;
    }

    /// <summary>
    /// Serializes the items into the output stream using the configured serializer (which handles compression).
    /// </summary>
    public async Task SerializeBatchAsync(Stream output, List<T> items)
    {
        await _serializer.SerializeBatchAsync(output, items);
    }

    /// <summary>
    /// Reads the batch body from memory.
    /// </summary>
    /// <remarks>
    /// Takes no cancellation token, because there is nowhere to honour one:
    /// <see cref="IBatchSerializer{T}.DeserializeBatchAsync"/> does not accept one, and the
    /// source is memory already read off the wire. A token accepted here and passed nowhere
    /// would tell a caller that decoding is interruptible when it is not.
    /// </remarks>
    public async Task<List<T>?> ReadBatchBodyFromMemoryAsync(ReadOnlyMemory<byte> bodyBytes)
    {
         Stream sourceStream;

         // Optimization: If Memory is backed by array, use MemoryStream(byte[], index, count) to avoid copy
         if (System.Runtime.InteropServices.MemoryMarshal.TryGetArray(bodyBytes, out var segment))
         {
             sourceStream = new MemoryStream(segment.Array!, segment.Offset, segment.Count, writable: false);
         }
         else
         {
             // Fallback: This copies, but happens only if memory is not array-backed (rare for us)
             sourceStream = new MemoryStream(bodyBytes.ToArray());
         }

         using (sourceStream)
         {
             return await _serializer.DeserializeBatchAsync(sourceStream);
         }
    }
}
