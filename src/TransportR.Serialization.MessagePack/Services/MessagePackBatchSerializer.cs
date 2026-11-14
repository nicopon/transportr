using MessagePack;
using MessagePack.Resolvers;
using TransportR.Client;
using TransportR.Client.Configuration;
using TransportR.Client.Internal;

namespace TransportR.Serialization.MessagePack;

/// <summary>
/// Serializes batches with MessagePack, compressed with LZ4, and reads them as untrusted data.
/// </summary>
/// <remarks>
/// A batch that expands to more than the bound given at construction is refused without being
/// decompressed. A serializer built through the public constructor, which is the one a host passes
/// to <c>WithSerializer</c>, keeps the 256 MiB default and not the figure of <c>WithMaxBatchBytes</c>:
/// <c>WithMessagePackSerialization</c> is the one that carries that figure.
/// </remarks>
public class MessagePackBatchSerializer<T> : IBatchSerializer<T>
{
    private readonly MessagePackSerializerOptions _options;
    private readonly int _maxDecompressedBytes;

    public MessagePackBatchSerializer(bool useAnnotations = false)
        : this(useAnnotations, ClientOptions.DefaultMaxBatchBytes)
    {
    }

    internal MessagePackBatchSerializer(bool useAnnotations, int maxDecompressedBytes)
    {
        _maxDecompressedBytes = maxDecompressedBytes;

        // Use Lz4BlockArray compression.
        // If useAnnotations is true, use StandardResolver (which respects [Key] attributes).
        // If false, use ContractlessStandardResolver (for POCOs without attributes or legacy behavior).
        IFormatterResolver resolver = useAnnotations
            ? StandardResolver.Instance
            : ContractlessStandardResolver.Instance;

        // The size comes off the peer's batch, so it is read as untrusted: UntrustedData brings a
        // ceiling of its own on what a batch expands to, replaced here by the one bound the client has.
        _options = MessagePackSerializerOptions.Standard
            .WithResolver(resolver)
            .WithSecurity(MessagePackSecurity.UntrustedData.WithMaximumDecompressedSize(maxDecompressedBytes))
            .WithCompression(MessagePackCompression.Lz4BlockArray);
    }

    public async Task SerializeBatchAsync(Stream destination, List<T> batch)
    {
        // Async serialization directly to the stream with compression
        await MessagePackSerializer.SerializeAsync(destination, batch, _options);
    }

    public async Task<List<T>?> DeserializeBatchAsync(Stream source)
    {
        try
        {
            return await MessagePackSerializer.DeserializeAsync<List<T>>(source, _options);
        }
        catch (MessagePackSerializationException ex) when (ExceedsDecompressedBound(ex))
        {
            throw DecompressionBound.Exceeded(_maxDecompressedBytes);
        }
    }

    // The library reports its ceiling as a serialization failure like any other, with nothing but
    // the wording to tell it from a body that does not decode.
    private static bool ExceedsDecompressedBound(Exception ex)
    {
        for (var inner = ex; inner is not null; inner = inner.InnerException)
        {
            if (inner.Message.Contains("decompressed length that exceeds", StringComparison.Ordinal)) return true;
        }

        return false;
    }
}
