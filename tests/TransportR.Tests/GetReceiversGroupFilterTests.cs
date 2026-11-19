using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using TransportR.Client;
using TransportR.Client.Configuration;
using TransportR.Client.Internal;
using TransportR.Client.Protocol;
using TransportR.Tests.Infrastructure;

namespace TransportR.Tests;

/// <summary>
/// <c>GetReceiversAsync(group)</c>: the hub's answer, kept to the receivers of one group by the client.
/// Over <c>TestServer</c>, in dev mode, where a client names its groups.
/// </summary>
public class GetReceiversGroupFilterTests
{
    private const string Url = "http://localhost";

    [Fact]
    public async Task TheFilteredCall_KeepsOnlyTheReceiversOfTheGroup_WhereTheUnfilteredOneReturnsBoth()
    {
        using var host = TestHubServerFactory.CreateHost();
        var handler = host.GetTestServer().CreateHandler();
        await using var sender = TestClients.Create(handler, Url);
        await using var alpha = TestClients.Create(handler, Url);
        await using var beta = TestClients.Create(handler, Url);
        await sender.ConnectAsync(["senders"]);
        await alpha.ConnectAsync(["alpha"]);
        await beta.ConnectAsync(["beta"]);

        var all = await sender.GetReceiversAsync();
        var onlyAlpha = await sender.GetReceiversAsync("alpha");
        var onlyBeta = await sender.GetReceiversAsync("beta", CancellationToken.None);
        var nobody = await sender.GetReceiversAsync("gamma");

        Assert.Contains(all, c => c.ClientId == alpha.ClientId);
        Assert.Contains(all, c => c.ClientId == beta.ClientId);
        Assert.Equal(alpha.ClientId, Assert.Single(onlyAlpha).ClientId);
        Assert.Equal(beta.ClientId, Assert.Single(onlyBeta).ClientId);
        Assert.Empty(nobody);
    }

    [Fact]
    public async Task TheGroupIsComparedAsGiven_NotIgnoringCase()
    {
        using var host = TestHubServerFactory.CreateHost();
        var handler = host.GetTestServer().CreateHandler();
        await using var sender = TestClients.Create(handler, Url);
        await using var receiver = TestClients.Create(handler, Url);
        await sender.ConnectAsync(["senders"]);
        await receiver.ConnectAsync(["receivers"]);

        Assert.Single(await sender.GetReceiversAsync("receivers"));
        Assert.Empty(await sender.GetReceiversAsync("Receivers"));
    }

    [Fact]
    public async Task AReceiverWhoseStreamIsDown_IsStillReturned_ForTheCallerToTellApartByDisconnectedAtUtc()
    {
        using var host = TestHubServerFactory.CreateHost();
        var handler = host.GetTestServer().CreateHandler();
        await using var sender = TestClients.Create(handler, Url);
        var receiver = TestClients.Create(handler, Url);
        await sender.ConnectAsync(["senders"]);
        await receiver.ConnectAsync(["receivers"]);
        var receiverId = receiver.ClientId;
        await receiver.DisposeAsync();
        await Task.Delay(500);

        var receivers = await sender.GetReceiversAsync("receivers");

        var stale = Assert.Single(receivers, c => c.ClientId == receiverId);
        Assert.NotNull(stale.DisconnectedAtUtc);
    }

    [Fact]
    public async Task AByteStreamClient_FiltersTheSameWay()
    {
        using var host = TestHubServerFactory.CreateHost();
        var handler = host.GetTestServer().CreateHandler();
        await using var sender = CreateByteClient(handler);
        await using var alpha = CreateByteClient(handler);
        await using var beta = CreateByteClient(handler);
        await sender.ConnectAsync(["senders"]);
        await alpha.ConnectAsync(["alpha"]);
        await beta.ConnectAsync(["beta"]);

        var all = await sender.GetReceiversAsync();
        var onlyAlpha = await sender.GetReceiversAsync("alpha");

        Assert.Contains(all, c => c.ClientId == alpha.ClientId);
        Assert.Contains(all, c => c.ClientId == beta.ClientId);
        Assert.Equal(alpha.ClientId, Assert.Single(onlyAlpha).ClientId);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public async Task ABlankGroup_IsRefusedAtTheCall_BeforeAnyRequest(string? group)
    {
        var handler = new RefusingHandler();
        await using var items = TestClients.Create(handler, Url);
        await using var bytes = CreateByteClient(handler);

        Assert.ThrowsAny<ArgumentException>(() => { _ = items.GetReceiversAsync(group!); });
        Assert.ThrowsAny<ArgumentException>(() => { _ = bytes.GetReceiversAsync(group!); });

        Assert.Equal(0, handler.Requests);
    }

    private static ByteStreamClient CreateByteClient(HttpMessageHandler handler)
    {
        var options = new ClientOptions
        {
            // A TestServer handler cannot carry a WebSocket.
            WriteTransportPolicy = WriteTransportPolicy.SegmentsOnly,
            MaxInFlightBatches = 4
        };
        ILogger logger = NullLogger.Instance;
        var httpClient = new HttpClient(handler) { BaseAddress = new Uri(Url), Timeout = Timeout.InfiniteTimeSpan };
        var channel = new ControlChannel(Url, Guid.NewGuid(), httpClient, options, logger);

        return new ByteStreamClient(
            channel,
            new SessionRegistry<ByteClientSession>(logger),
            logger,
            batchBytes: 4096,
            options,
            Url + WireRoutes.StreamBase,
            httpClient);
    }

    /// <summary>A hub that must not be reached: every request it receives is counted, and fails.</summary>
    private sealed class RefusingHandler : HttpMessageHandler
    {
        private int _requests;

        public int Requests => Volatile.Read(ref _requests);

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref _requests);
            throw new HttpRequestException("No hub is reachable in this test.");
        }
    }
}
