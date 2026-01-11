namespace TransportR.Interfaces;

/// <summary>
/// Defines the contract for serializing and deserializing batches of items.
/// </summary>
/// <remarks>
/// <para>
/// Implementations should handle both serialization and optional compression.
/// The default implementation uses MessagePack with Lz4 compression.
/// </para>
/// <para>
/// Implementations must be thread-safe as they may be called concurrently
/// from multiple transfers.
/// </para>
/// </remarks>
public interface IBatchSerializer
{
    /// <summary>
    /// Serializes a batch of items to the destination stream.
    /// </summary>
    /// <typeparam name="T">The type of items in the batch.</typeparam>
    /// <param name="destination">The stream to write the serialized data to.</param>
    /// <param name="batch">The list of items to serialize.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task SerializeBatchAsync<T>(Stream destination, List<T> batch);

    /// <summary>
    /// Deserializes a batch of items from the source stream.
    /// </summary>
    /// <typeparam name="T">The type of items in the batch.</typeparam>
    /// <param name="source">The stream to read the serialized data from.</param>
    /// <returns>
    /// A task containing the deserialized list of items, or null if the stream is empty.
    /// </returns>
    Task<List<T>?> DeserializeBatchAsync<T>(Stream source);
}
