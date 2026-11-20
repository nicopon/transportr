using TransportR.Client;

namespace TransportR.DocSnippets;

/// <summary>
/// The two snippets of "End-to-end encryption" in <c>src/TransportR.Client/README.md</c>, compiled.
/// Edit them and the document together, as <see cref="ReadmeQuickStart"/>.
/// </summary>
internal static class ClientEndToEndEncryption
{
    public static TransportRClient<Order> Usage(string hubUrl, byte[] nodeKey)
    {
        var client = new TransportRClientBuilder<Order>()
            .WithUrl(hubUrl)
            .WithEndToEndEncryption(peerId => nodeKey) // 32 bytes from this node's own configuration
            .Build();
        return client;
    }

    public static TransportRClientBuilder<Order> KeyPerPair(TransportRClientBuilder<Order> builder, Dictionary<Guid, byte[]> keys) =>
        builder
            // keys: a Dictionary<Guid, byte[]> read from this node's own configuration, one key per peer
            .WithEndToEndEncryption(peerId => keys.GetValueOrDefault(peerId));
}
