using System.Collections.Concurrent;
using System.IO.Pipelines;
using System.Net;
using System.Text;
using Microsoft.AspNetCore.TestHost;
using TransportR.Client.Internal;
using TransportR.Client.Configuration;
using TransportR.Protocol;
using TransportR.Tests.Infrastructure;
using Microsoft.Extensions.Logging.Abstractions;

namespace TransportR.Tests;

public class ControlChannelTests
{
    [Fact]
    public async Task AStreamWhoseResponseSetsTheCookies_IsOpenedOnceMore_CarryingThem()
    {
        // A proxy with affinity sets its cookie on the answer to a request that carried none, and the
        // hub, which read that request, registered the client without it.
        var hub = new StreamingHub(setCookie: "affinity=instance-a; path=/");
        await using var channel = new ControlChannel("http://hub", Guid.NewGuid(), new HttpClient(hub), new ClientOptions(), NullLogger.Instance);

        await channel.OpenAsync(groups: null, CancellationToken.None);
        await Task.Delay(200);

        Assert.Equal(new string?[] { null, "affinity=instance-a" }, hub.StreamCookies);
        Assert.Equal("affinity=instance-a", channel.CookieHeader());
        Assert.True(channel.IsOpen);
    }

    [Fact]
    public async Task AStreamWhoseResponseSetsNoCookie_IsOpenedOnce()
    {
        var hub = new StreamingHub(setCookie: null);
        await using var channel = new ControlChannel("http://hub", Guid.NewGuid(), new HttpClient(hub), new ClientOptions(), NullLogger.Instance);

        await channel.OpenAsync(groups: null, CancellationToken.None);
        await Task.Delay(200);

        Assert.Equal(new string?[] { null }, hub.StreamCookies);
    }

    [Fact]
    public void WithCookie_SetsTheCookie_AndKeepsTheOthers()
    {
        Assert.Equal("session=1; affinity=b", ControlChannel.WithCookie("affinity=a; session=1", "affinity", "b"));
        Assert.Equal("affinity=b", ControlChannel.WithCookie(null, "affinity", "b"));
    }

    [Fact]
    public async Task GetAccessTokenAsync_ShouldCacheToken()
    {
        // Arrange
        var options = new ClientOptions();
        int callCount = 0;
        Func<Task<string>> provider = () => {
            callCount++;
            return Task.FromResult($"token_{callCount}");
        };

        await using var channel = Create(options, provider);

        // Act 1: Fetch (initial)
        var token1 = await channel.GetAccessTokenAsync(forceRefresh: false);

        // Assert 1
        Assert.Equal("token_1", token1);
        Assert.Equal(1, callCount);

        // Act 2: Fetch again (should be cached)
        var token2 = await channel.GetAccessTokenAsync(forceRefresh: false);

        // Assert 2
        Assert.Equal("token_1", token2);
        Assert.Equal(1, callCount); // Still 1
    }

    [Fact]
    public async Task GetAccessTokenAsync_ShouldForceRefresh()
    {
        // Arrange
        var options = new ClientOptions();
        int callCount = 0;
        Func<Task<string>> provider = () => {
            callCount++;
            return Task.FromResult($"token_{callCount}");
        };

        await using var channel = Create(options, provider);

        // Fill cache
        await channel.GetAccessTokenAsync(false);
        Assert.Equal(1, callCount);

        // Act: Force Refresh
        var token = await channel.GetAccessTokenAsync(forceRefresh: true);

        // Assert
        Assert.Equal("token_2", token);
        Assert.Equal(2, callCount);
    }

    [Fact]
    public async Task DisposeAsync_CalledTwice_IsNotAnError()
    {
        var channel = Create(new ClientOptions());

        await channel.DisposeAsync();

        // The convention is the sessions' (SessionDisposalTests): a second release is a defensive
        // call, not a caller error. The second Cancel() on the disposed lifetime source threw.
        await channel.DisposeAsync();
    }

    [Fact]
    public async Task DisposeAsync_AfterConnecting_CalledTwice_IsNotAnError()
    {
        using var host = TestHubServerFactory.CreateHost();
        var channel = new ControlChannel("http://localhost", Guid.NewGuid(), new HttpClient(host.GetTestServer().CreateHandler()),
            new ClientOptions(), NullLogger.Instance);
        await channel.OpenAsync(groups: null, CancellationToken.None);

        await channel.DisposeAsync();
        await channel.DisposeAsync();
    }

    private static ControlChannel Create(ClientOptions options, Func<Task<string>>? provider = null) =>
        new("http://hub", Guid.NewGuid(), new HttpClient(), options, NullLogger.Instance, initialAccessToken: null, tokenProvider: provider);

    /// <summary>
    /// Answers every opening of a stream with the registration and then nothing, and sets
    /// <c>setCookie</c> on the answer to a request that carried no cookie.
    /// </summary>
    private sealed class StreamingHub(string? setCookie) : HttpMessageHandler
    {
        private readonly ConcurrentQueue<string?> _streamCookies = new();
        private int _opened;

        public string?[] StreamCookies => [.. _streamCookies];

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var cookie = request.Headers.TryGetValues("Cookie", out var values) ? string.Join("; ", values) : null;
            _streamCookies.Enqueue(cookie);

            var pipe = new Pipe();
            var registered = $"event: {ControlPlane.Registered}\ndata: {{\"connectionId\":\"c-{Interlocked.Increment(ref _opened)}\"}}\n\n";
            await pipe.Writer.WriteAsync(Encoding.UTF8.GetBytes(registered), cancellationToken);

            var response = new HttpResponseMessage(HttpStatusCode.OK) { Content = new StreamContent(pipe.Reader.AsStream()) };
            if (cookie == null && setCookie != null) response.Headers.Add("Set-Cookie", setCookie);
            return response;
        }
    }
}
