using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using TransportR.Client;
using TransportR.Hub;
using TransportR.Hub.Interfaces;
using TransportR.Hub.Services;
using TransportR.Tests.Infrastructure;

namespace TransportR.Tests;

/// <summary>
/// The handshake timeout a host opens a transfer with is held to the range a client's own request is:
/// from zero to the time the hub keeps a transfer's session for, and refused before anything is written.
/// </summary>
public class HostHandshakeTimeoutTests
{
    public static TheoryData<TimeSpan> OutsideTheRange => new()
    {
        TimeSpan.FromMilliseconds(-5),
        Timeout.InfiniteTimeSpan,
        TransferManager.SessionTtl + TimeSpan.FromMilliseconds(1),
    };

    [Theory]
    [MemberData(nameof(OutsideTheRange))]
    public async Task AHandshakeTimeoutOutsideTheRange_IsRefused_BeforeAnythingIsWritten(TimeSpan handshakeTimeout)
    {
        var store = new SessionCountingStore();
        using var host = TestHubServerFactory.CreateHost(services => services.Replace(ServiceDescriptor.Singleton<IStateStore>(store)));
        await using var clients = await ConnectAsync(host);
        var initiator = host.Services.GetRequiredService<ITransferInitiator>();

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            () => initiator.InitTransferAsync(clients.Sender.ClientId, clients.Receiver.ClientId, handshakeTimeout));

        Assert.Equal(0, store.SessionsSaved);
        Assert.Empty(host.Services.GetRequiredService<ITransferManager>().GetAllParticipants());
    }

    [Fact]
    public async Task TheLongestHandshakeTimeout_IsInsideTheRange()
    {
        using var host = TestHubServerFactory.CreateHost();
        await using var clients = await ConnectAsync(host);
        var initiator = host.Services.GetRequiredService<ITransferInitiator>();

        var transferId = await initiator.InitTransferAsync(
            clients.Sender.ClientId, clients.Receiver.ClientId, TransferManager.SessionTtl);

        Assert.False(string.IsNullOrEmpty(transferId));
    }

    [Fact]
    public async Task AHandshakeTimeoutOfZero_IsInsideTheRange_AndTheHandshakeTimesOutAtOnce()
    {
        using var host = TestHubServerFactory.CreateHost();
        await using var clients = await ConnectAsync(host);
        var initiator = host.Services.GetRequiredService<ITransferInitiator>();

        var timedOut = await Assert.ThrowsAsync<HubRequestFailedException>(
            () => initiator.InitTransferAsync(clients.Sender.ClientId, clients.Receiver.ClientId, TimeSpan.Zero));

        Assert.Contains("timed out", timedOut.Message);
    }

    private static async Task<ConnectedPair> ConnectAsync(IHost host)
    {
        var handler = host.GetTestServer().CreateHandler();
        var sender = TestClients.Create(handler, "http://localhost");
        var receiver = TestClients.Create(handler, "http://localhost");
        receiver.OnTransferStarted += _ => Task.CompletedTask;
        sender.OnSendRequested += _ => Task.CompletedTask;
        await sender.ConnectAsync();
        await receiver.ConnectAsync();
        return new ConnectedPair(sender, receiver);
    }

    private sealed class ConnectedPair(TransportRClient<string> sender, TransportRClient<string> receiver) : IAsyncDisposable
    {
        public TransportRClient<string> Sender { get; } = sender;
        public TransportRClient<string> Receiver { get; } = receiver;

        public async ValueTask DisposeAsync()
        {
            await Sender.DisposeAsync();
            await Receiver.DisposeAsync();
        }
    }
}
