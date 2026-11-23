using Microsoft.AspNetCore.TestHost;
using TransportR.Client;
using TransportR.Serialization.MessagePack;
using TransportR.Tests.Infrastructure;

namespace TransportR.Tests;

public class DiscoverySubjectTests
{
    [Fact]
    public async Task GetReceivers_HidesSubjectByDefault()
    {
        using var host = AuthenticatedHubServerFactory.CreateHost();
        var handler = host.GetTestServer().CreateHandler();

        await using var sender = CreateClient(handler, "sender-subject");
        await using var receiver = CreateClient(handler, "receiver-subject");

        await sender.ConnectAsync();
        await receiver.ConnectAsync();

        var receivers = await sender.GetReceiversAsync();
        var target = Assert.Single(receivers, r => r.ClientId == receiver.ClientId);
        Assert.Null(target.Subject);
    }

    [Fact]
    public async Task GetReceivers_IncludesAttestedSubject_WhenExposeSubjectInDiscoveryIsEnabled()
    {
        using var host = AuthenticatedHubServerFactory.CreateHost(hub =>
            hub.ConfigureClientRegistry(o => o.ExposeSubjectInDiscovery = true));
        var handler = host.GetTestServer().CreateHandler();

        await using var sender = CreateClient(handler, "sender-subject");
        await using var receiver = CreateClient(handler, "receiver-subject");

        await sender.ConnectAsync();
        await receiver.ConnectAsync();

        var receivers = await sender.GetReceiversAsync();
        var target = Assert.Single(receivers, r => r.ClientId == receiver.ClientId);
        Assert.Equal("receiver-subject", target.Subject);
    }

    private static TransportRClient<string> CreateClient(HttpMessageHandler handler, string subject) =>
        new TransportRClientBuilder<string>()
            .WithUrl("http://localhost")
            .WithHttpMessageHandler(handler)
            .WithWriteTransport(WriteTransportPolicy.SegmentsOnly)
            .WithAccessToken(StubJwt.Create(subject, DateTime.UtcNow.AddMinutes(30), AuthenticatedHubServerFactory.DefaultGroup))
            .WithMessagePackSerialization()
            .Build();
}
