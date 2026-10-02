using Microsoft.AspNetCore.Http.Connections;
using Microsoft.AspNetCore.SignalR;
using Microsoft.AspNetCore.SignalR.Client;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using TransportR.Abstractions;
using TransportR.Client.SignalR;
using TransportR.Client.SignalR.Internal;
using TransportR.Hub.SignalR.Access;
using TransportR.Hub.SignalR.Configuration;
using TransportR.Hub.SignalR.Hubs;
using TransportR.Hub.SignalR.Services;
using TransportR.Interfaces;
using TransportR.Serialization.MessagePack.Services;
using TransportR.Tests.Infrastructure;
using Xunit;

namespace TransportR.Tests;

/// <summary>
/// A client that is connected and idle for longer than its token lives is still a client: its token
/// provider can give it a new one, and nothing about the connection has changed.
/// </summary>
public class TokenRenewalTests
{
    private static readonly TimeSpan Lifetime = TimeSpan.FromSeconds(3);

    [Fact]
    public async Task ClientWithinItsTokenLifetime_CanOpenATransfer()
    {
        // The control: the same flow with nothing to outlive, so that the test below can only be
        // red for the token.
        await OpenTransferAfterIdleAsync(senderLifetime: TimeSpan.FromHours(1), idle: TimeSpan.FromSeconds(1));
    }

    [Fact]
    public async Task IdleClient_PastItsTokenLifetime_CanStillOpenATransfer()
    {
        await OpenTransferAfterIdleAsync(senderLifetime: Lifetime, idle: Lifetime + TimeSpan.FromSeconds(1));
    }

    private static async Task OpenTransferAfterIdleAsync(TimeSpan senderLifetime, TimeSpan idle)
    {
        using var host = AuthenticatedHubServerFactory.CreateHost();
        var handler = host.GetTestServer().CreateHandler();

        // The receiver's token outlives the test, so a refusal can only be the sender's.
        var senderTokens = new TokenSource("sender-principal", senderLifetime);
        var receiverTokens = new TokenSource("receiver-principal", TimeSpan.FromHours(1));

        // The beat is shorter than the token's life: whatever carries a new token to the hub has
        // several chances to do so before the old one runs out.
        await using var sender = TestClients.Create(handler, "http://localhost", senderTokens.GetAsync,
            options => options.PresenceHeartbeatInterval = TimeSpan.FromSeconds(1));
        await using var receiver = TestClients.Create(handler, "http://localhost", receiverTokens.GetAsync);

        var receiveHandle = new TaskCompletionSource<Receive<string>>(TaskCreationOptions.RunContinuationsAsynchronously);
        receiver.OnTransferStarted += receive => { receiveHandle.TrySetResult(receive); return Task.CompletedTask; };

        await sender.ConnectAsync();
        await receiver.ConnectAsync();

        // Connected, and doing nothing.
        await Task.Delay(idle);

        await using var send = await sender.InitTransferAsync(receiver.ClientId, batchSize: 10);
        await using var receive = await receiveHandle.Task.WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Equal(send.TransferId, receive.TransferId);
    }

    private static async Task<T> EventuallyAsync<T>(Func<Task<T>> read, Func<T, bool> done, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        var value = await read();
        while (!done(value) && DateTime.UtcNow < deadline)
        {
            await Task.Delay(50);
            value = await read();
        }

        return value;
    }

    private static Func<Task<DateTime?>> ExpiryOf(IHost host, Guid clientId) =>
        async () => (await host.Services.GetRequiredService<IStateStore>().GetClientAsync(clientId))?.TokenExpiry;

    [Theory]
    [InlineData(300, 3600, 300)]
    [InlineData(300, 480, 240)]
    [InlineData(300, 20, 10)]
    [InlineData(300, 0, 0)]
    [InlineData(300, -5, 0)]
    public void Advance_IsTheThreshold_CappedAtHalfWhatIsLeft(int thresholdSeconds, int remainingSeconds, int expectedSeconds)
    {
        var advance = TokenRenewer.Advance(TimeSpan.FromSeconds(thresholdSeconds), TimeSpan.FromSeconds(remainingSeconds));

        Assert.Equal(TimeSpan.FromSeconds(expectedSeconds), advance);
    }

    [Fact]
    public async Task Renewal_PresentsANewTokenAheadOfTheExpiry()
    {
        using var host = AuthenticatedHubServerFactory.CreateHost();
        var tokens = new TokenSource("renewing-principal", TimeSpan.FromSeconds(4));
        await using var client = TestClients.Create(host.GetTestServer().CreateHandler(), "http://localhost", tokens.GetAsync);

        await client.ConnectAsync();
        var first = (await ExpiryOf(host, client.ClientId)())!.Value;

        // A token of four seconds is renewed at two: the hub holds a later expiry well before the first is due.
        var renewed = await EventuallyAsync(ExpiryOf(host, client.ClientId), e => e > first.AddSeconds(1), TimeSpan.FromSeconds(3.5));

        Assert.True(renewed > first.AddSeconds(1), $"The hub still holds {renewed:O}; the first token expires at {first:O}.");
    }

    [Fact]
    public async Task Renewal_ArmsItselfOnTheHubsExpiry_WhenTheClientCannotReadItsOwnToken()
    {
        using var host = AuthenticatedHubServerFactory.CreateHost();
        var tokens = new TokenSource("opaque-principal", TimeSpan.FromSeconds(4)) { UnreadableByClients = true };
        await using var client = TestClients.Create(host.GetTestServer().CreateHandler(), "http://localhost", tokens.GetAsync);

        await client.ConnectAsync();
        var first = (await ExpiryOf(host, client.ClientId)())!.Value;

        var renewed = await EventuallyAsync(ExpiryOf(host, client.ClientId), e => e > first.AddSeconds(1), TimeSpan.FromSeconds(3.5));

        Assert.True(renewed > first.AddSeconds(1), $"The hub still holds {renewed:O}; the first token expires at {first:O}.");
    }

    [Fact]
    public async Task Renewal_KeepsTryingWhileTheProviderFails_AndKeepsTheOldTokenMeanwhile()
    {
        using var host = AuthenticatedHubServerFactory.CreateHost();
        var tokens = new TokenSource("flaky-principal", TimeSpan.FromSeconds(12));
        var calls = 0;

        // The two asks that follow the connection's own (the first push, then the renewal) fail.
        Task<string> Provider() => Interlocked.Increment(ref calls) is 3 or 4
            ? throw new InvalidOperationException("The identity provider is down.")
            : tokens.GetAsync();

        await using var client = TestClients.Create(host.GetTestServer().CreateHandler(), "http://localhost", Provider);
        await client.ConnectAsync();
        var first = (await ExpiryOf(host, client.ClientId)())!.Value;

        var renewed = await EventuallyAsync(ExpiryOf(host, client.ClientId), e => e > first.AddSeconds(1), TimeSpan.FromSeconds(10));

        Assert.True(renewed > first.AddSeconds(1), $"Never renewed ({calls} calls); the hub still holds {renewed:O}.");
        Assert.True(calls >= 5, $"The provider was asked {calls} times.");
    }

    [Fact]
    public async Task Renewal_KeepsAskingWhileTheProviderReturnsTheTokenItGaveBefore()
    {
        // WithClientCredentials caches a token until its own threshold. Until then a renewal gets the
        // token the hub already holds, which renews nothing.
        using var host = AuthenticatedHubServerFactory.CreateHost();
        var tokens = new TokenSource("caching-principal", TimeSpan.FromSeconds(12));
        string? cached = null;
        var calls = 0;

        async Task<string> Provider()
        {
            var n = Interlocked.Increment(ref calls);
            if (cached == null || n >= 6) cached = await tokens.GetAsync();
            return cached;
        }

        await using var client = TestClients.Create(host.GetTestServer().CreateHandler(), "http://localhost", Provider);
        await client.ConnectAsync();
        var first = (await ExpiryOf(host, client.ClientId)())!.Value;

        var renewed = await EventuallyAsync(ExpiryOf(host, client.ClientId), e => e > first.AddSeconds(1), TimeSpan.FromSeconds(10));

        Assert.True(renewed > first.AddSeconds(1), $"Never renewed ({calls} calls); the hub still holds {renewed:O}.");
        Assert.True(calls >= 6);
    }

    [Fact]
    public async Task Renewal_StopsWithTheClient()
    {
        using var host = AuthenticatedHubServerFactory.CreateHost();
        var tokens = new TokenSource("leaving-principal", TimeSpan.FromSeconds(2));
        var client = TestClients.Create(host.GetTestServer().CreateHandler(), "http://localhost", tokens.GetAsync);
        await client.ConnectAsync();

        await client.DisposeAsync();
        var asked = tokens.Calls;
        await Task.Delay(TimeSpan.FromSeconds(2.5));

        Assert.Equal(asked, tokens.Calls);
    }

    [Fact]
    public async Task ClientBuiltThroughThePublicBuilder_RenewsWithItsDefaults()
    {
        // The public constructor is what an application uses, and nothing in it is tuned for a short
        // token. The first token lives four seconds and the client is idle for five.
        await using var host = await AuthenticatedHubServerFactory.StartLoopbackAsync();
        var senderTokens = new TokenSource("builder-sender", TimeSpan.FromSeconds(4));
        var receiverTokens = new TokenSource("builder-receiver", TimeSpan.FromHours(1));

        await using var sender = new TransportRClientBuilder<string>()
            .WithUrl(host.BaseUrl)
            .WithSerializer(new MessagePackBatchSerializer<string>())
            .WithTokenProvider(senderTokens.GetAsync)
            .WithMaxInFlightBatches(4)
            .Build();
        await using var receiver = new TransportRClientBuilder<string>()
            .WithUrl(host.BaseUrl)
            .WithSerializer(new MessagePackBatchSerializer<string>())
            .WithTokenProvider(receiverTokens.GetAsync)
            .WithReceiveCapacity(maxItems: 10)
            .Build();

        var receiveHandle = new TaskCompletionSource<Receive<string>>(TaskCreationOptions.RunContinuationsAsynchronously);
        receiver.OnTransferStarted += receive => { receiveHandle.TrySetResult(receive); return Task.CompletedTask; };

        await sender.ConnectAsync();
        await receiver.ConnectAsync();
        await Task.Delay(TimeSpan.FromSeconds(5));

        await using var send = await sender.InitTransferAsync(receiver.ClientId, batchSize: 10);
        await using var receive = await receiveHandle.Task.WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Equal(send.TransferId, receive.TransferId);
    }

    [Fact]
    public async Task StaticToken_IsSaidToExpire_AtConnect()
    {
        // Nothing can renew it, and the hub closes the connection when it runs out: better said now
        // than found out then.
        using var host = AuthenticatedHubServerFactory.CreateHost();
        var logs = new LogRecorder();
        var token = StubJwt.Create("static-principal", DateTime.UtcNow.AddHours(1), AuthenticatedHubServerFactory.DefaultGroup);

        await using var client = TestClients.Create(
            host.GetTestServer().CreateHandler(), "http://localhost", accessToken: token, logger: logs.Factory.CreateLogger("client"));
        await client.ConnectAsync();

        Assert.True(logs.Has(LogLevel.Warning, "The access token is static and expires at"));
    }
}
