using Microsoft.Extensions.Logging.Abstractions;
using TransportR.Client.SignalR.Configuration;
using TransportR.Client.SignalR.Internal;
using Xunit;

namespace TransportR.Tests;

public class TokenRenewerDisposalTests
{
    private static TokenRenewer Create(out HubConnectionManager manager)
    {
        manager = new HubConnectionManager("http://unused", new ClientOptions(), NullLogger.Instance);
        return new TokenRenewer(manager, new HttpClient(), "http://unused/api/client", Guid.NewGuid(), TimeSpan.FromSeconds(30), NullLogger.Instance);
    }

    [Fact]
    public async Task DisposeAsync_CalledTwice_IsNotAnError()
    {
        var renewer = Create(out var manager);
        await using var _ = manager;

        await renewer.DisposeAsync();
        await renewer.DisposeAsync();
    }

    [Fact]
    public async Task DisposeAsync_AfterStart_CalledTwice_IsNotAnError()
    {
        var renewer = Create(out var manager);
        await using var _ = manager;

        renewer.Start();
        await renewer.DisposeAsync();
        await renewer.DisposeAsync();
    }

    [Fact]
    public async Task StartAndPushNow_AfterDisposal_StartNothing()
    {
        var renewer = Create(out var manager);
        await using var _ = manager;

        await renewer.DisposeAsync();

        renewer.Start();
        renewer.PushNow();
    }
}
