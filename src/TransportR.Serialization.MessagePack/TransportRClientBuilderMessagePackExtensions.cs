using TransportR.Client;
using TransportR.Serialization.MessagePack.Services;

namespace TransportR.Serialization.MessagePack;

public static class TransportRClientBuilderMessagePackExtensions
{
    public static TransportRClientBuilder<T> WithMessagePackSerialization<T>(this TransportRClientBuilder<T> builder, bool useAnnotations = false)
    {
        return builder.WithSerializerFactory(maxBatchBytes => new MessagePackBatchSerializer<T>(useAnnotations, maxBatchBytes));
    }
}
