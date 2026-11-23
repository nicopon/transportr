using System.Net;
using System.Net.Sockets;
using System.Text;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Time.Testing;
using TransportR.Client;
using TransportR.Client.Auth;
using TransportR.Client.Internal;
using TransportR.Hub;
using TransportR.Serialization.MessagePack;
using TransportR.Tests.Infrastructure;

namespace TransportR.Tests;

/// <summary>
/// What the handler a builder's caller configures reaches, and what <c>Build</c> refuses of it.
/// </summary>
public class ConfigureHttpHandlerTests
{
    private static readonly TimeSpan TestBound = TimeSpan.FromSeconds(30);
    private static readonly Uri TokenEndpoint = new("http://idp.invalid/token");

    [Theory]
    [InlineData(WriteTransportPolicy.SegmentsOnly)]
    [InlineData(WriteTransportPolicy.RequireWebSocket)]
    public async Task AConnectCallback_CarriesTheEventsStream_AndTheWriteStream(WriteTransportPolicy policy)
    {
        await using var hub = await TestHubServerFactory.StartLoopbackAsync();
        var wire = new Wire();

        await using var sender = new TransportRClientBuilder<string>()
            .WithUrl(hub.BaseUrl)
            .WithMessagePackSerialization()
            .WithMaxInFlightBatches(4)
            .WithWriteTransport(policy)
            .ConfigureHttpHandler(handler => handler.ConnectCallback = wire.ConnectAsync)
            .Build();

        await using var receiver = new TransportRClientBuilder<string>()
            .WithUrl(hub.BaseUrl)
            .WithMessagePackSerialization()
            .WithReceiveCapacity(maxItems: 100)
            .Build();

        var receiveHandle = new TaskCompletionSource<Receive<string>>(TaskCreationOptions.RunContinuationsAsynchronously);
        receiver.OnTransferStarted(started => { receiveHandle.TrySetResult(started); return Task.CompletedTask; });

        await sender.ConnectAsync();
        await receiver.ConnectAsync();

        await using var send = await sender.InitTransferAsync(receiver.ClientId, batchSize: 10);
        await using var receive = await receiveHandle.Task.WaitAsync(TestBound);
        var consumed = ConsumeAsync(receive);

        var items = Enumerable.Range(0, 25).Select(i => $"item-{i}").ToList();
        foreach (var item in items) await send.SendAsync(item).WaitAsync(TestBound);
        await send.CompleteAsync().WaitAsync(TestBound);
        Assert.Equal(items, await consumed.WaitAsync(TestBound));

        Assert.True(wire.Carried($"GET /api/client/{sender.ClientId}/events?"), "The events stream did not go through the callback.");
        if (policy == WriteTransportPolicy.SegmentsOnly)
        {
            Assert.True(wire.Carried($"POST /api/stream/{send.TransferId}/send?"), "No segment went through the callback.");
        }
        else
        {
            Assert.True(wire.Carried($"GET /api/stream/{send.TransferId}/send?", "upgrade: websocket"), "The WebSocket did not open through the callback.");
        }
    }

    public static TheoryData<string, string> RefusedSettings => new()
    {
        { "items", nameof(SocketsHttpHandler.UseCookies) },
        { "items", nameof(SocketsHttpHandler.AutomaticDecompression) },
        { "bytes", nameof(SocketsHttpHandler.UseCookies) },
        { "bytes", nameof(SocketsHttpHandler.AutomaticDecompression) },
    };

    [Theory]
    [MemberData(nameof(RefusedSettings))]
    public void Build_RefusesAHandlerSettingThatBreaksTheClient_AndNamesIt(string client, string setting)
    {
        Action<SocketsHttpHandler> configure = setting switch
        {
            nameof(SocketsHttpHandler.UseCookies) => handler => handler.UseCookies = true,
            nameof(SocketsHttpHandler.AutomaticDecompression) => handler => handler.AutomaticDecompression = DecompressionMethods.GZip,
            _ => throw new ArgumentOutOfRangeException(nameof(setting)),
        };

        Action build = client == "items"
            ? () => ItemsBuilder().ConfigureHttpHandler(configure).Build()
            : () => BytesBuilder().ConfigureHttpHandler(configure).Build();

        var refused = Assert.Throws<InvalidOperationException>(build);
        Assert.Contains(setting, refused.Message);
    }

    [Theory]
    [InlineData("items")]
    [InlineData("bytes")]
    public async Task WithClientCredentials_WithoutAnHttpClient_AsksForItsTokensThroughTheHubsHandler(string client)
    {
        Uri? asked = null;
        Action<SocketsHttpHandler> configure = handler => handler.ConnectCallback = (context, _) =>
        {
            asked = context.InitialRequestMessage.RequestUri;
            throw new IOException("No identity provider in this test.");
        };

        await using var built = client == "items"
            ? Built.Of(ItemsBuilder().WithClientCredentials(TokenEndpoint.ToString(), "id", "secret", scope: null).ConfigureHttpHandler(configure).Build())
            : Built.Of(BytesBuilder().WithClientCredentials(TokenEndpoint.ToString(), "id", "secret", scope: null).ConfigureHttpHandler(configure).Build());

        await Assert.ThrowsAsync<HttpRequestException>(() => built.Channel.AcquireTokenAsync());
        Assert.Equal(TokenEndpoint, asked);

        // Its own HttpClient on the hub's handler: nothing the hub's client carries reaches the provider.
        var provider = Assert.IsType<ClientCredentialsTokenProvider>(built.Channel.TokenProvider!.Target);
        Assert.NotSame(built.HubClient, provider.HttpClient);
        Assert.NotEqual(Timeout.InfiniteTimeSpan, provider.HttpClient.Timeout);
        Assert.Null(provider.HttpClient.BaseAddress);
        Assert.Empty(provider.HttpClient.DefaultRequestHeaders);
    }

    [Theory]
    [InlineData("items")]
    [InlineData("bytes")]
    public async Task WithClientCredentials_GivenAnHttpClient_AsksForItsTokensThroughThatOne_AndNotTheHubsHandler(string client)
    {
        Uri? theirs = null;
        var hubs = 0;
        using var idpClient = new HttpClient(new SocketsHttpHandler
        {
            ConnectCallback = (context, _) =>
            {
                theirs = context.InitialRequestMessage.RequestUri;
                throw new IOException("No identity provider in this test.");
            }
        });

        Action<SocketsHttpHandler> configure = handler => handler.ConnectCallback = (_, _) =>
        {
            Interlocked.Increment(ref hubs);
            throw new IOException("No hub in this test.");
        };

        await using var built = client == "items"
            ? Built.Of(ItemsBuilder().WithClientCredentials(TokenEndpoint.ToString(), "id", "secret", scope: null, httpClient: idpClient).ConfigureHttpHandler(configure).Build())
            : Built.Of(BytesBuilder().WithClientCredentials(TokenEndpoint.ToString(), "id", "secret", scope: null, httpClient: idpClient).ConfigureHttpHandler(configure).Build());

        await Assert.ThrowsAsync<HttpRequestException>(() => built.Channel.AcquireTokenAsync());
        Assert.Equal(TokenEndpoint, theirs);
        Assert.Equal(0, Volatile.Read(ref hubs));
    }

    [Fact]
    public async Task WithHttpMessageHandler_CarriesItemTransferOverTestServer()
    {
        using var host = TestHubServerFactory.CreateHost();
        var server = host.GetTestServer();

        await using var sender = new TransportRClientBuilder<string>()
            .WithUrl("http://localhost")
            .WithHttpMessageHandler(server.CreateHandler())
            .WithWriteTransport(WriteTransportPolicy.SegmentsOnly)
            .WithMessagePackSerialization()
            .WithMaxInFlightBatches(4)
            .Build();

        await using var receiver = new TransportRClientBuilder<string>()
            .WithUrl("http://localhost")
            .WithHttpMessageHandler(server.CreateHandler())
            .WithWriteTransport(WriteTransportPolicy.SegmentsOnly)
            .WithMessagePackSerialization()
            .WithReceiveCapacity(maxItems: 100)
            .Build();

        var receiveHandle = new TaskCompletionSource<Receive<string>>(TaskCreationOptions.RunContinuationsAsynchronously);
        receiver.OnTransferStarted(started => { receiveHandle.TrySetResult(started); return Task.CompletedTask; });

        await sender.ConnectAsync();
        await receiver.ConnectAsync();

        await using var send = await sender.InitTransferAsync(receiver.ClientId, batchSize: 10);
        await using var receive = await receiveHandle.Task.WaitAsync(TestBound);
        var consumed = ConsumeAsync(receive);

        var items = Enumerable.Range(0, 25).Select(i => $"item-{i}").ToList();
        await send.SendAsync(items).WaitAsync(TestBound);
        await send.CompleteAsync().WaitAsync(TestBound);

        Assert.Equal(items, await consumed.WaitAsync(TestBound));
    }

    [Fact]
    public async Task WithHttpMessageHandler_CarriesByteTransferOverTestServer()
    {
        using var host = TestHubServerFactory.CreateHost();
        var server = host.GetTestServer();

        await using var sender = new ByteStreamClientBuilder(1024)
            .WithUrl("http://localhost")
            .WithHttpMessageHandler(server.CreateHandler())
            .WithWriteTransport(WriteTransportPolicy.SegmentsOnly)
            .WithMaxInFlightBatches(4)
            .Build();

        await using var receiver = new ByteStreamClientBuilder(1024)
            .WithUrl("http://localhost")
            .WithHttpMessageHandler(server.CreateHandler())
            .WithWriteTransport(WriteTransportPolicy.SegmentsOnly)
            .WithReceiveCapacity(1 << 16)
            .Build();

        var receiveHandle = new TaskCompletionSource<ByteReceive>(TaskCreationOptions.RunContinuationsAsynchronously);
        receiver.OnTransferStarted(started => { receiveHandle.TrySetResult(started); return Task.CompletedTask; });

        await sender.ConnectAsync();
        await receiver.ConnectAsync();

        await using var send = await sender.InitTransferAsync(receiver.ClientId);
        await using var receive = await receiveHandle.Task.WaitAsync(TestBound);

        var received = new MemoryStream();
        var copy = receive.CopyToAsync(received);

        var payload = Enumerable.Range(0, 2500).Select(i => (byte)(i % 251)).ToArray();
        await send.WriteAsync(payload).WaitAsync(TestBound);
        await send.CompleteAsync().WaitAsync(TestBound);
        await copy.WaitAsync(TestBound);

        Assert.Equal(payload, received.ToArray());
    }

    [Fact]
    public async Task WithHttpMessageHandler_WorksWithFakeTimeProvider_OnTestServer()
    {
        var clock = new FakeTimeProvider(DateTimeOffset.UtcNow);
        using var host = new HostBuilder()
            .ConfigureWebHost(web => web
                .UseTestServer()
                .ConfigureServices(services =>
                {
                    services.AddSingleton<TimeProvider>(clock);
                    services.AddDataHub()
                        .UseDevMode()
                        .ConfigureClientRegistry(o => o.DisconnectedGraceSeconds = 10);
                })
                .Configure(app =>
                {
                    app.UseRouting();
                    app.UseEndpoints(endpoints => endpoints.MapDataHub(requireAuth: false));
                }))
            .Start();

        await using var sender = new TransportRClientBuilder<string>()
            .WithUrl("http://localhost")
            .WithHttpMessageHandler(host.GetTestServer().CreateHandler())
            .WithWriteTransport(WriteTransportPolicy.SegmentsOnly)
            .WithMessagePackSerialization()
            .WithMaxInFlightBatches(1)
            .Build();

        var receiver = new TransportRClientBuilder<string>()
            .WithUrl("http://localhost")
            .WithHttpMessageHandler(host.GetTestServer().CreateHandler())
            .WithWriteTransport(WriteTransportPolicy.SegmentsOnly)
            .WithMessagePackSerialization()
            .WithReceiveCapacity(maxItems: 1)
            .Build();

        await sender.ConnectAsync(["senders"]);
        await receiver.ConnectAsync(["receivers"]);
        await receiver.DisposeAsync();

        var duringGrace = await sender.GetReceiversAsync("receivers");
        Assert.Equal(receiver.ClientId, Assert.Single(duringGrace).ClientId);

        clock.Advance(TimeSpan.FromSeconds(11));

        Assert.Empty(await sender.GetReceiversAsync("receivers"));
    }

    [Theory]
    [InlineData("items")]
    [InlineData("bytes")]
    public void WithHttpMessageHandler_NullHandler_ThrowsArgumentNullException(string client)
    {
        Action act = client == "items"
            ? () => ItemsBuilder().WithHttpMessageHandler(null!)
            : () => BytesBuilder().WithHttpMessageHandler(null!);

        Assert.Throws<ArgumentNullException>(act);
    }

    [Theory]
    [InlineData("items", true)]
    [InlineData("items", false)]
    [InlineData("bytes", true)]
    [InlineData("bytes", false)]
    public void Build_RefusesCombiningConfigureHttpHandlerAndWithHttpMessageHandler(string client, bool configureFirst)
    {
        using var custom = new SocketsHttpHandler { UseCookies = false, AutomaticDecompression = DecompressionMethods.None };

        Action build = (client, configureFirst) switch
        {
            ("items", true) => () => ItemsBuilder().ConfigureHttpHandler(_ => { }).WithHttpMessageHandler(custom).Build(),
            ("items", false) => () => ItemsBuilder().WithHttpMessageHandler(custom).ConfigureHttpHandler(_ => { }).Build(),
            ("bytes", true) => () => BytesBuilder().ConfigureHttpHandler(_ => { }).WithHttpMessageHandler(custom).Build(),
            _ => () => BytesBuilder().WithHttpMessageHandler(custom).ConfigureHttpHandler(_ => { }).Build(),
        };

        Assert.Throws<InvalidOperationException>(build);
    }

    public static TheoryData<string, string, bool, bool> RefusedCustomHandlerSettings => new()
    {
        { "items", nameof(SocketsHttpHandler.UseCookies), false, false },
        { "items", nameof(SocketsHttpHandler.AutomaticDecompression), false, false },
        { "items", nameof(SocketsHttpHandler.UseCookies), true, true },
        { "items", nameof(SocketsHttpHandler.AutomaticDecompression), true, true },
        { "bytes", nameof(SocketsHttpHandler.UseCookies), false, false },
        { "bytes", nameof(SocketsHttpHandler.AutomaticDecompression), false, false },
        { "bytes", nameof(SocketsHttpHandler.UseCookies), true, true },
        { "bytes", nameof(SocketsHttpHandler.AutomaticDecompression), true, true },
    };

    [Theory]
    [MemberData(nameof(RefusedCustomHandlerSettings))]
    public void WithHttpMessageHandler_RefusesWrappedOrDirectHandlersWithForbiddenSettings(
        string client, string setting, bool useHttpClientHandler, bool wrapInDelegating)
    {
        HttpMessageHandler inner = useHttpClientHandler
            ? new HttpClientHandler
            {
                UseCookies = setting == nameof(SocketsHttpHandler.UseCookies),
                AutomaticDecompression = setting == nameof(SocketsHttpHandler.AutomaticDecompression) ? DecompressionMethods.GZip : DecompressionMethods.None,
            }
            : new SocketsHttpHandler
            {
                UseCookies = setting == nameof(SocketsHttpHandler.UseCookies),
                AutomaticDecompression = setting == nameof(SocketsHttpHandler.AutomaticDecompression) ? DecompressionMethods.GZip : DecompressionMethods.None,
            };

        using var handler = wrapInDelegating ? new PassThroughHandler(inner) : inner;

        Action build = client == "items"
            ? () => ItemsBuilder().WithHttpMessageHandler(handler).Build()
            : () => BytesBuilder().WithHttpMessageHandler(handler).Build();

        var refused = Assert.Throws<InvalidOperationException>(build);
        Assert.Contains(setting, refused.Message);
    }

    [Theory]
    [InlineData("items", false)]
    [InlineData("items", true)]
    [InlineData("bytes", false)]
    [InlineData("bytes", true)]
    public async Task WithHttpMessageHandler_RespectsDisposeHandlerFlag(string client, bool disposeHandler)
    {
        using var probe = new DisposalTrackingHandler();

        await using (var built = client == "items"
            ? (IAsyncDisposable)ItemsBuilder().WithHttpMessageHandler(probe, disposeHandler).Build()
            : BytesBuilder().WithHttpMessageHandler(probe, disposeHandler).Build())
        {
        }

        Assert.Equal(disposeHandler, probe.Disposed);
    }

    [Theory]
    [InlineData("items")]
    [InlineData("bytes")]
    public async Task WithClientCredentials_WithoutAnHttpClient_UsesCustomHttpMessageHandler(string client)
    {
        Uri? asked = null;
        using var custom = new InterceptingHandler(req =>
        {
            asked = req.RequestUri;
            throw new IOException("No identity provider in this test.");
        });

        await using var built = client == "items"
            ? Built.Of(ItemsBuilder().WithClientCredentials(TokenEndpoint.ToString(), "id", "secret").WithHttpMessageHandler(custom).Build())
            : Built.Of(BytesBuilder().WithClientCredentials(TokenEndpoint.ToString(), "id", "secret").WithHttpMessageHandler(custom).Build());

        await Assert.ThrowsAsync<IOException>(() => built.Channel.AcquireTokenAsync());
        Assert.Equal(TokenEndpoint, asked);
    }

    [Theory]
    [InlineData("items", true)]
    [InlineData("items", false)]
    [InlineData("bytes", true)]
    [InlineData("bytes", false)]
    public async Task WithClientCredentials_LogsThroughBuilderLoggerFactory_RegardlessOfCallOrder(string client, bool loggerFirst)
    {
        using var recorder = new LogRecorder();
        using var loggerFactory = recorder.Factory;
        using var failingIdp = new InterceptingHandler(_ =>
            Task.FromResult(new HttpResponseMessage(HttpStatusCode.BadRequest) { Content = new StringContent("bad_client") }));
        using var idpClient = new HttpClient(failingIdp);

        await using var built = (client, loggerFirst) switch
        {
            ("items", true) => Built.Of(ItemsBuilder().WithLoggerFactory(loggerFactory).WithClientCredentials(TokenEndpoint.ToString(), "id", "secret", httpClient: idpClient).Build()),
            ("items", false) => Built.Of(ItemsBuilder().WithClientCredentials(TokenEndpoint.ToString(), "id", "secret", httpClient: idpClient).WithLoggerFactory(loggerFactory).Build()),
            ("bytes", true) => Built.Of(BytesBuilder().WithLoggerFactory(loggerFactory).WithClientCredentials(TokenEndpoint.ToString(), "id", "secret", httpClient: idpClient).Build()),
            _ => Built.Of(BytesBuilder().WithClientCredentials(TokenEndpoint.ToString(), "id", "secret", httpClient: idpClient).WithLoggerFactory(loggerFactory).Build()),
        };

        await Assert.ThrowsAsync<InvalidOperationException>(() => built.Channel.AcquireTokenAsync());
        Assert.True(recorder.Has(LogLevel.Information, "Acquiring token from"));
    }

    private sealed class PassThroughHandler(HttpMessageHandler inner) : DelegatingHandler(inner);

    private sealed class DisposalTrackingHandler : HttpMessageHandler
    {
        public bool Disposed { get; private set; }

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        protected override void Dispose(bool disposing)
        {
            Disposed = true;
            base.Dispose(disposing);
        }
    }

    private sealed class InterceptingHandler(Func<HttpRequestMessage, Task<HttpResponseMessage>> onSend) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken) =>
            onSend(request);
    }

    private static TransportRClientBuilder<string> ItemsBuilder() =>
        new TransportRClientBuilder<string>().WithUrl("http://localhost:1").WithMessagePackSerialization().WithMaxInFlightBatches(4);

    private static ByteStreamClientBuilder BytesBuilder() =>
        new ByteStreamClientBuilder(4096).WithUrl("http://localhost:1").WithMaxInFlightBatches(4);

    private static async Task<List<string>> ConsumeAsync(Receive<string> receive)
    {
        var items = new List<string>();
        await foreach (var item in receive.ReceiveAsync()) items.Add(item);
        await receive.WaitForCompletionAsync();
        return items;
    }

    /// <summary>Either kind of client, by what these tests read of it.</summary>
    private sealed class Built(ControlChannel channel, HttpClient hubClient, IAsyncDisposable client) : IAsyncDisposable
    {
        public ControlChannel Channel { get; } = channel;

        public HttpClient HubClient { get; } = hubClient;

        public static Built Of(TransportRClient<string> client) => new(client.Channel, client.Connection.HttpClient, client);

        public static Built Of(ByteStreamClient client) => new(client.Channel, client.Connection.HttpClient, client);

        public ValueTask DisposeAsync() => client.DisposeAsync();
    }

    /// <summary>
    /// Connections to the real address, with what the client writes on each kept apart, so that two
    /// connections' writes cannot interleave inside one request's head.
    /// </summary>
    private sealed class Wire
    {
        private readonly List<StringBuilder> _connections = [];

        public async ValueTask<Stream> ConnectAsync(SocketsHttpConnectionContext context, CancellationToken token)
        {
            var socket = new Socket(SocketType.Stream, ProtocolType.Tcp) { NoDelay = true };
            try
            {
                await socket.ConnectAsync(context.DnsEndPoint, token);
            }
            catch
            {
                socket.Dispose();
                throw;
            }

            var written = new StringBuilder();
            lock (_connections) _connections.Add(written);
            return new Tapped(new NetworkStream(socket, ownsSocket: true), written);
        }

        /// <summary>Whether one request's head starts with <paramref name="requestLine"/> and holds each of <paramref name="headers"/>.</summary>
        public bool Carried(string requestLine, params string[] headers)
        {
            StringBuilder[] connections;
            lock (_connections) connections = [.. _connections];

            foreach (var connection in connections)
            {
                string text;
                lock (connection) text = connection.ToString();

                for (var at = text.IndexOf(requestLine, StringComparison.Ordinal); at >= 0; at = text.IndexOf(requestLine, at + 1, StringComparison.Ordinal))
                {
                    var end = text.IndexOf("\r\n\r\n", at, StringComparison.Ordinal);
                    var head = end < 0 ? text[at..] : text[at..end];
                    if (headers.All(header => head.Contains(header, StringComparison.OrdinalIgnoreCase))) return true;
                }
            }

            return false;
        }
    }

    private sealed class Tapped(Stream inner, StringBuilder written) : Stream
    {
        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => true;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }

        public override int Read(byte[] buffer, int offset, int count) => inner.Read(buffer, offset, count);

        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default) =>
            inner.ReadAsync(buffer, cancellationToken);

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
            inner.ReadAsync(buffer, offset, count, cancellationToken);

        public override void Write(byte[] buffer, int offset, int count)
        {
            Record(buffer.AsSpan(offset, count));
            inner.Write(buffer, offset, count);
        }

        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            Record(buffer.Span);
            return inner.WriteAsync(buffer, cancellationToken);
        }

        public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            Record(buffer.AsSpan(offset, count));
            return inner.WriteAsync(buffer, offset, count, cancellationToken);
        }

        public override void Flush() => inner.Flush();

        public override Task FlushAsync(CancellationToken cancellationToken) => inner.FlushAsync(cancellationToken);

        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

        public override void SetLength(long value) => throw new NotSupportedException();

        protected override void Dispose(bool disposing)
        {
            if (disposing) inner.Dispose();
            base.Dispose(disposing);
        }

        public override ValueTask DisposeAsync() => inner.DisposeAsync();

        private void Record(ReadOnlySpan<byte> bytes)
        {
            var text = Encoding.Latin1.GetString(bytes);
            lock (written) written.Append(text);
        }
    }
}
