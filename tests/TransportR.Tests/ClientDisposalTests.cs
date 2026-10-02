using Microsoft.AspNetCore.TestHost;
using TransportR.Tests.Infrastructure;
using Xunit;

namespace TransportR.Tests;

/// <summary>
/// A host that stops its client does it the ordinary way: <c>DisconnectAsync</c> because it is public
/// and says what it means, then the end of an <c>await using</c>. Neither order may throw.
/// </summary>
public class ClientDisposalTests
{
    [Fact]
    public async Task DisconnectAsync_ThenDisposeAsync_IsNotAnError()
    {
        using var host = TestHubServerFactory.CreateHost();
        var client = TestClients.Create(host.GetTestServer().CreateHandler(), "http://localhost");
        await client.ConnectAsync();

        await client.DisconnectAsync();
        await client.DisposeAsync();
    }

    [Fact]
    public async Task DisposeAsync_CalledTwice_IsNotAnError()
    {
        using var host = TestHubServerFactory.CreateHost();
        var client = TestClients.Create(host.GetTestServer().CreateHandler(), "http://localhost");
        await client.ConnectAsync();

        await client.DisposeAsync();
        await client.DisposeAsync();
    }

    [Fact]
    public async Task DisconnectAsync_CalledTwice_IsNotAnError()
    {
        using var host = TestHubServerFactory.CreateHost();
        var client = TestClients.Create(host.GetTestServer().CreateHandler(), "http://localhost");
        await client.ConnectAsync();

        await client.DisconnectAsync();
        await client.DisconnectAsync();
        await client.DisposeAsync();
    }
}
