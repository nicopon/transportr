using MessagePack;
using MessagePack.Resolvers;
using TransportR.Interfaces;

namespace TransportR.Serialization.MessagePack.Services;

public class MessagePackBatchSerializer<T> : IBatchSerializer<T>
{
    private readonly MessagePackSerializerOptions _options;

    public MessagePackBatchSerializer(bool useAnnotations = false)
    {
        // Use Lz4BlockArray compression.
        // If useAnnotations is true, use StandardResolver (which respects [Key] attributes).
        // If false, use ContractlessStandardResolver (for POCOs without attributes or legacy behavior).
        IFormatterResolver resolver = useAnnotations
            ? StandardResolver.Instance
            : ContractlessStandardResolver.Instance;

        _options = MessagePackSerializerOptions.Standard
            .WithResolver(resolver)
            .WithCompression(MessagePackCompression.Lz4BlockArray);
    }

    public async Task SerializeBatchAsync(Stream destination, List<T> batch)
    {
        // Async serialization directly to the stream with compression
        await MessagePackSerializer.SerializeAsync(destination, batch, _options);
    }

    public async Task<List<T>?> DeserializeBatchAsync(Stream source)
    {
        return await MessagePackSerializer.DeserializeAsync<List<T>>(source, _options);
    }
}
