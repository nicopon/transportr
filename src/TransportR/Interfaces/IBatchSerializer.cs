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
/// <para>
/// <b>Ownership.</b> An implementation may take ownership of the items handed to
/// <see cref="SerializeBatchAsync"/> and release them once written — see the guarantee on that
/// method. This matters only for item types holding unmanaged or off-heap memory; for plain
/// objects the question does not arise.
/// </para>
/// </remarks>
public interface IBatchSerializer
{
    /// <summary>
    /// Serializes a batch of items to the destination stream.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Guaranteed: this is called at most once per batch, and the implementation owns
    /// <paramref name="batch"/> and its items from that call on.</b> An implementation may dispose
    /// or otherwise release them once they are written.
    /// </para>
    /// <para>
    /// This is a contract, not an observation of the current sender. A resend replays the bytes
    /// produced here, never the items, so nothing comes back to this method for a batch it has
    /// already serialized — and a future change to the resend path must keep that true rather than
    /// re-serialize. An implementation carrying off-heap memory (Apache Arrow record batches,
    /// native buffers) relies on this to release it; breaking the guarantee turns that release
    /// into a use-after-free.
    /// </para>
    /// </remarks>
    /// <typeparam name="T">The type of items in the batch.</typeparam>
    /// <param name="destination">The stream to write the serialized data to.</param>
    /// <param name="batch">
    /// The list of items to serialize. Owned by the implementation once passed.
    /// </param>
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
