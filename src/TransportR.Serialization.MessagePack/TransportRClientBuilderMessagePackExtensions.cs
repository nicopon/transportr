using TransportR.Client.SignalR;
using TransportR.Serialization.MessagePack.Services;

namespace TransportR.Serialization.MessagePack;

public static class TransportRClientBuilderMessagePackExtensions
{
    public static TransportRClientBuilder WithMessagePackSerialization(this TransportRClientBuilder builder, bool useAnnotations = false)
    {
        return builder.WithSerializer(new MessagePackBatchSerializer(useAnnotations));
    }
}
