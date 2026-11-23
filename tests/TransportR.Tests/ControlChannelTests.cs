using System.Collections.Concurrent;
using System.IO.Pipelines;
using System.Net;
using System.Text;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.Logging.Abstractions;
using TransportR.Client.Configuration;
using TransportR.Client.Internal;
using TransportR.Client.Sessions;
using TransportR.Protocol;
using TransportR.Tests.Infrastructure;
using TransportR.Tests.TestHelpers;

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
    public async Task AnOpeningAnsweredWithAnUnmarkedNotFound_IsTriedAgain_AsWhenTheHubIsOutOfReach()
    {
        // A proxy that does not forward the control plane yet answers 404: no refusal of the hub's.
        var hub = new NotFoundHub(marked: false, refusedOpenings: 1);
        await using var channel = new ControlChannel("http://hub", Guid.NewGuid(), new HttpClient(hub), new ClientOptions(), NullLogger.Instance);

        await channel.OpenAsync(groups: null, CancellationToken.None);

        Assert.Equal(2, hub.Openings);
        Assert.True(channel.IsOpen);
    }

    [Fact]
    public async Task AnOpeningAnsweredWithTheHubsNotFound_IsARefusal_RaisedAtOnce()
    {
        var hub = new NotFoundHub(marked: true, refusedOpenings: 1);
        await using var channel = new ControlChannel("http://hub", Guid.NewGuid(), new HttpClient(hub), new ClientOptions(), NullLogger.Instance);

        var refusal = await Assert.ThrowsAsync<HubRequestFailedException>(() => channel.OpenAsync(groups: null, CancellationToken.None));

        Assert.Equal(ControlChannel.RefusedMessage, refusal.Message);
        Assert.Equal(HubRequestFailureCategory.Refused, refusal.Category);
        Assert.Equal(1, hub.Openings);
    }

    [Fact]
    public async Task AReplacementAnsweredWithAnUnmarkedNotFound_RaisesAnHttpRequestException()
    {
        var hub = new NotFoundHub(marked: false, refusedOpenings: 2);
        await using var channel = new ControlChannel("http://hub", Guid.NewGuid(), new HttpClient(hub), new ClientOptions(), NullLogger.Instance);
        await channel.OpenAsync(groups: null, CancellationToken.None);

        var failure = await Assert.ThrowsAsync<HttpRequestException>(() => channel.ReplaceAsync(token: null, CancellationToken.None));

        Assert.Equal(ControlChannel.NoHubRouteMessage, failure.Message);
        Assert.True(channel.IsOpen);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task APresenceAnsweredWithANotFound_IsAnUnknownStream_OnlyWhenTheHubMarkedIt(bool marked)
    {
        var hub = new NotFoundHub(marked);
        await using var channel = new ControlChannel("http://hub", Guid.NewGuid(), new HttpClient(hub), new ClientOptions(), NullLogger.Instance);
        await channel.OpenAsync(groups: null, CancellationToken.None);

        if (marked)
        {
            Assert.False(await channel.IsKnownAsync(CancellationToken.None));
        }
        else
        {
            await Assert.ThrowsAsync<HttpRequestException>(() => channel.IsKnownAsync(CancellationToken.None));
        }
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task ARequestAnsweredWithANotFound_IsARefusal_OnlyWhenTheHubMarkedIt(bool marked)
    {
        using var response = NotFoundHub.NotFound(marked);

        if (marked)
        {
            var refusal = await Assert.ThrowsAsync<HubRequestFailedException>(() => ControlChannel.EnsureAcceptedAsync(response));
            Assert.Equal(HubRequestFailureCategory.Refused, refusal.Category);
        }
        else
        {
            await Assert.ThrowsAsync<HttpRequestException>(() => ControlChannel.EnsureAcceptedAsync(response));
        }
    }

    [Fact]
    public async Task ATooManyRequestsThatCarriesTheHubsError_IsARefusal_AndOneWithout_IsNot()
    {
        using var fromHub = new HttpResponseMessage(HttpStatusCode.TooManyRequests)
        {
            Content = System.Net.Http.Json.JsonContent.Create(new HubError("Too many."), options: ControlPlane.Json)
        };
        using var fromAProxy = new HttpResponseMessage(HttpStatusCode.TooManyRequests)
        {
            Content = new StringContent("<html>Slow down</html>", Encoding.UTF8, "text/html")
        };

        var refusal = await Assert.ThrowsAsync<HubRequestFailedException>(() => ControlChannel.EnsureAcceptedAsync(fromHub));
        Assert.Equal("Too many.", refusal.Message);
        Assert.Equal(HubRequestFailureCategory.RateLimited, refusal.Category);

        var failure = await Assert.ThrowsAsync<HttpRequestException>(() => ControlChannel.EnsureAcceptedAsync(fromAProxy));
        Assert.Equal(HttpStatusCode.TooManyRequests, failure.StatusCode);
    }

    [Fact]
    public async Task AServiceUnavailableThatCarriesTheHubsError_IsRefused_AndOneWithout_IsNot()
    {
        using var fromHub = new HttpResponseMessage(HttpStatusCode.ServiceUnavailable)
        {
            Content = System.Net.Http.Json.JsonContent.Create(new HubError("The hub is shutting down."), options: ControlPlane.Json)
        };
        fromHub.Headers.Add(WireRoutes.RefusedHeader, "1");

        using var fromAProxy = new HttpResponseMessage(HttpStatusCode.ServiceUnavailable)
        {
            Content = new StringContent("<html>Unavailable</html>", Encoding.UTF8, "text/html")
        };
        using var fromAProxyWithJson = new HttpResponseMessage(HttpStatusCode.ServiceUnavailable)
        {
            Content = new StringContent("""{"message":"gateway unavailable"}""", Encoding.UTF8, "application/json")
        };
        using var fromHubWithBadJson = new HttpResponseMessage(HttpStatusCode.ServiceUnavailable)
        {
            Content = new StringContent("{not-json", Encoding.UTF8, "application/json")
        };
        fromHubWithBadJson.Headers.Add(WireRoutes.RefusedHeader, "1");

        var refusal = await Assert.ThrowsAsync<HubRequestFailedException>(() => ControlChannel.EnsureAcceptedAsync(fromHub));
        Assert.Equal("The hub is shutting down.", refusal.Message);
        Assert.Equal(HubRequestFailureCategory.Refused, refusal.Category);

        var proxyHtmlFailure = await Assert.ThrowsAsync<HttpRequestException>(() => ControlChannel.EnsureAcceptedAsync(fromAProxy));
        Assert.Equal(HttpStatusCode.ServiceUnavailable, proxyHtmlFailure.StatusCode);

        var proxyJsonFailure = await Assert.ThrowsAsync<HttpRequestException>(() => ControlChannel.EnsureAcceptedAsync(fromAProxyWithJson));
        Assert.Equal(HttpStatusCode.ServiceUnavailable, proxyJsonFailure.StatusCode);

        var malformedFailure = await Assert.ThrowsAsync<HttpRequestException>(() => ControlChannel.EnsureAcceptedAsync(fromHubWithBadJson));
        Assert.Equal(HttpStatusCode.ServiceUnavailable, malformedFailure.StatusCode);
    }

    [Theory]
    [InlineData("""{"message":"Refused."}""")]
    [InlineData("""{"message":"Refused.","code":99}""")]
    public async Task AConflictWhoseCategoryIsAbsentOrUnknown_IsAnAnswerTheClientCannotRead_NotARefusal(string body)
    {
        using var response = new HttpResponseMessage(HttpStatusCode.Conflict)
        {
            Content = new StringContent(body, Encoding.UTF8, "application/json")
        };

        var failure = await Assert.ThrowsAsync<HttpRequestException>(() => ControlChannel.EnsureAcceptedAsync(response));

        Assert.Equal(HttpRequestError.InvalidResponse, failure.HttpRequestError);
        Assert.Equal(HttpStatusCode.Conflict, failure.StatusCode);
        Assert.Equal(ControlChannel.UnreadableRefusalMessage, failure.Message);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task AProbeAnsweredWithANotFound_HearsTheHub_OnlyWhenTheHubMarkedIt(bool marked)
    {
        await using var client = TestClients.Create(new NotFoundHub(marked), "http://hub");
        var probe = client.Connection.ProbeAsync("transfer-1", affinity: null, CancellationToken.None);

        if (marked)
        {
            Assert.True(await probe);
        }
        else
        {
            await Assert.ThrowsAsync<HttpRequestException>(() => probe);
        }
    }

    [Theory]
    [InlineData(true, "The hub refused the token (404).")]
    [InlineData(false, ControlChannel.NoHubRouteMessage)]
    public async Task ARenewalAnsweredWithANotFound_IsARefusal_OnlyWhenTheHubMarkedIt(bool marked, string message)
    {
        await using var channel = new ControlChannel("http://hub", Guid.NewGuid(), new HttpClient(new NotFoundHub(marked)), new ClientOptions(), NullLogger.Instance);
        await using var renewer = new TokenRenewer(channel, TimeSpan.FromSeconds(30), NullLogger.Instance);

        var failure = await Assert.ThrowsAsync<HttpRequestException>(() => renewer.PushNowAsync("token", CancellationToken.None));

        Assert.Equal(message, failure.Message);
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
        Func<Task<string>> provider = () =>
        {
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
        Func<Task<string>> provider = () =>
        {
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

            var response = await RegisteredAsync($"c-{Interlocked.Increment(ref _opened)}", cancellationToken);
            if (cookie == null && setCookie != null) response.Headers.Add("Set-Cookie", setCookie);
            return response;
        }
    }

    /// <summary>
    /// Registers every opening of a stream but those of <c>refusedOpenings</c> (the first is 1), and
    /// answers those and every other request with a 404, which carries the hub's mark when
    /// <c>marked</c>.
    /// </summary>
    private sealed class NotFoundHub(bool marked, params int[] refusedOpenings) : HttpMessageHandler
    {
        private int _openings;

        public int Openings => Volatile.Read(ref _openings);

        public static HttpResponseMessage NotFound(bool marked) =>
            marked ? HubResponse.With(HttpStatusCode.NotFound) : new HttpResponseMessage(HttpStatusCode.NotFound);

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            if (request.RequestUri!.AbsolutePath.EndsWith("/" + WireRoutes.EventsSegment, StringComparison.Ordinal))
            {
                var opening = Interlocked.Increment(ref _openings);
                if (!refusedOpenings.Contains(opening)) return await RegisteredAsync($"c-{opening}", cancellationToken);
            }

            return NotFound(marked);
        }
    }

    /// <summary>A stream that opens with the registration under <paramref name="connectionId"/>, then carries nothing.</summary>
    private static async Task<HttpResponseMessage> RegisteredAsync(string connectionId, CancellationToken cancellationToken)
    {
        var pipe = new Pipe();
        var registered = $"event: {ControlPlane.Registered}\ndata: {{\"connectionId\":\"{connectionId}\"}}\n\n";
        await pipe.Writer.WriteAsync(Encoding.UTF8.GetBytes(registered), cancellationToken);

        return new HttpResponseMessage(HttpStatusCode.OK) { Content = new StreamContent(pipe.Reader.AsStream()) };
    }
}
