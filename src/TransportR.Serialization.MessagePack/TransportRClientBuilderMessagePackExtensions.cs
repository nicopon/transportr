using TransportR.Client.SignalR;
using TransportR.Serialization.MessagePack.Services;

namespace TransportR.Serialization.MessagePack;

public static class TransportRClientBuilderMessagePackExtensions
{
    public static TransportRClientBuilder<T> WithMessagePackSerialization<T>(this TransportRClientBuilder<T> builder, bool useAnnotations = false)
    {
        return builder.WithSerializer(new MessagePackBatchSerializer<T>(useAnnotations));
    }
}
