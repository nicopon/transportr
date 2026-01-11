using TransportR.Interfaces;

using Microsoft.IO;

namespace TransportR.Protocol;

public class BatchProtocol
{
    private readonly IBatchSerializer _serializer;

    public BatchProtocol(IBatchSerializer serializer)
    {
        _serializer = serializer;
    }

    /// <summary>
    /// Serializes the items into the output stream using the configured serializer (which handles compression).
    /// </summary>
    public async Task SerializeBatchAsync<T>(Stream output, List<T> items)
    {
        await _serializer.SerializeBatchAsync(output, items);
    }

    /// <summary>
    /// Reads the batch body from memory. 
    /// </summary>
    public async Task<List<T>?> ReadBatchBodyFromMemoryAsync<T>(ReadOnlyMemory<byte> bodyBytes, CancellationToken ct)
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
             return await _serializer.DeserializeBatchAsync<T>(sourceStream);
         }
    }
}
