using System.Net;
using System.Security.Cryptography.X509Certificates;
using TransportR.Client;

namespace TransportR.DocSnippets;

/// <summary>
/// The snippet of "Network" in <c>src/TransportR.Client/README.md</c>, compiled.
/// Edit it and the document together, as <see cref="ReadmeQuickStart"/>.
/// </summary>
internal static class ClientNetwork
{
    public static TransportRClientBuilder<Order> Configure(
        TransportRClientBuilder<Order> builder, X509Certificate2 clientCertificate, X509Certificate2 authority) =>
        builder
            .ConfigureHttpHandler(handler =>
            {
                handler.Proxy = new WebProxy("http://proxy.example:3128");
                handler.SslOptions.ClientCertificates = [clientCertificate];
                handler.SslOptions.CertificateChainPolicy = new X509ChainPolicy
                {
                    TrustMode = X509ChainTrustMode.CustomRootTrust,
                    CustomTrustStore = { authority }
                };
            });
}
