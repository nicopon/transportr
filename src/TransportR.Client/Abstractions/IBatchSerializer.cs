namespace TransportR.Client;

/// <summary>
/// Defines the contract for serializing and deserializing batches of <typeparamref name="T"/>.
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
/// <b>The item type belongs to the serializer, not to the call.</b> Making these methods generic
/// instead would say that every implementation carries every type, which is a promise no
/// implementation can keep and one actively cannot: a serializer built on Apache Arrow carries
/// record batches and nothing else. It would have to refuse the wrong type by throwing, at the
/// first batch, on a worker thread — where the compiler had everything it needed to refuse. Put
/// the type here and building a mismatched client stops compiling, whether or not it goes through
/// the builder.
/// </para>
/// <para>
/// <b>Ownership.</b> An implementation may take ownership of the items handed to
/// <see cref="SerializeBatchAsync"/> and release them once written — see the guarantee on that
/// method. This matters only for item types holding unmanaged or off-heap memory; for plain
/// objects the question does not arise.
/// </para>
/// </remarks>
/// <typeparam name="T">The type of items this serializer carries.</typeparam>
public interface IBatchSerializer<T>
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
    /// <param name="destination">The stream to write the serialized data to.</param>
    /// <param name="batch">
    /// The list of items to serialize. Owned by the implementation once passed.
    /// </param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task SerializeBatchAsync(Stream destination, List<T> batch);

    /// <summary>
    /// Deserializes a batch of items from the source stream.
    /// </summary>
    /// <param name="source">The stream to read the serialized data from.</param>
    /// <returns>
    /// A task containing the deserialized list of items, or null if the stream is empty.
    /// </returns>
    Task<List<T>?> DeserializeBatchAsync(Stream source);
}
