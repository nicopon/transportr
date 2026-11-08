using Microsoft.AspNetCore.TestHost;
using TransportR.Abstractions;
using TransportR.Models;
using TransportR.Tests.Infrastructure;

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

    [Fact]
    public async Task DisposeAsync_WithATransferUnderWay_ReleasesItsSession_WhileItCanStillTellThePeer()
    {
        // On Kestrel and on segments: each segment, and the hub's abort route, is a request of the
        // client's HttpClient, so a session released once that is gone cannot tell the peer anything.
        await using var host = await TestHubServerFactory.StartLoopbackAsync();
        var sender = TestClients.Create(null, host.BaseUrl, configure: o => o.WriteTransportPolicy = WriteTransportPolicy.SegmentsOnly);
        await using var receiver = TestClients.Create(null, host.BaseUrl, configure: o => o.WriteTransportPolicy = WriteTransportPolicy.SegmentsOnly);

        var receiveHandle = new TaskCompletionSource<Receive<string>>(TaskCreationOptions.RunContinuationsAsynchronously);
        receiver.OnTransferStarted += receive => { receiveHandle.TrySetResult(receive); return Task.CompletedTask; };

        await sender.ConnectAsync();
        await receiver.ConnectAsync();

        var send = await sender.InitTransferAsync(receiver.ClientId, batchSize: 1);
        var receive = await receiveHandle.Task.WaitAsync(TimeSpan.FromSeconds(10));
        var receiveTask = Task.Run(async () =>
        {
            await foreach (var _ in receive.ReceiveAsync())
            {
            }
        });

        await send.SendAsync("item-1");
        await send.SendAsync("item-2");

        // The handle is left alone: the client is what releases the session.
        await sender.DisposeAsync();

        Assert.Empty(sender.GetTransfers());
        await Assert.ThrowsAsync<TransferFailedException>(() => receiveTask.WaitAsync(TimeSpan.FromSeconds(10)));

        await receive.DisposeAsync();
    }
}
