using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using TransportR.Client.Sessions;
using TransportR.Protocol;
using TransportR.Tests.TestHelpers;

namespace TransportR.Tests;

/// <summary>
/// A segment of a write stream that the hub cuts, or refuses, fails the attempt at once: a client
/// waiting on the answer to its segment learns of the cut from that answer, and reopens.
/// </summary>
/// <remarks>
/// These run against a real socket: a cut is a connection that closes under a request in flight,
/// which an in-memory handler does not reproduce.
/// </remarks>
public class WriteStreamCutTests
{
    // The recovery target is about 3 s after the link is back; a stream failing on its own
    // account has no business taking longer than that to be noticed.
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(3);

    [Fact]
    public async Task SenderDataStream_CutUnderItsSegment_FailsTheAttemptAtOnce()
    {
        await using var hub = await LoopbackHub.StartAsync(app =>
        {
            app.MapPost("/data/{id}/send", async (HttpContext ctx) =>
            {
                await ReadAtLeastAsync(ctx, ChunkHeader.Size);
                await Task.Delay(200);
                ctx.Abort();
            });
            app.MapGet("/data/{id}/receive", ParkedResponseAsync);
        });

        var context = new StubSessionContext(hub.CreateClient());
        var transferId = Guid.NewGuid();
        var handshake = new ReconnectionHandler(context, transferId.ToString(), affinity: null);
        await using var manager = new HttpStreamManager(context, context, transferId.ToString(), hub.DataUrl, affinity: null);

        // The sender's real write action up to the point where it parks: the handshake Ping goes
        // out, then the window is full and nothing else is written.
        manager.StartStreams(
            isSender: true,
            writeAction: async (stream, token) =>
            {
                handshake.PrepareForHandshake();
                await handshake.ExecuteHandshakeAsync(stream, transferId, token);
                await Task.Delay(Timeout.Infinite, token);
            },
            readAction: ReadUntilCancelledAsync,
            sessionToken: CancellationToken.None);

        var result = await manager.SenderTask!.WaitAsync(Bound);

        Assert.Equal(StreamOutcome.Retryable, result.Outcome);
    }

    [Fact]
    public async Task WriteStream_RefusedBeforeItsBodyIsRead_IsFatalAtOnce()
    {
        // What ValidateTransfer does for a transfer that is gone: a 404 before anything is read.
        await using var hub = await LoopbackHub.StartAsync(app =>
        {
            app.MapPost("/data/{id}/send", (HttpContext ctx) =>
            {
                ctx.Response.Headers[WireRoutes.RefusedHeader] = "1";
                ctx.Response.StatusCode = StatusCodes.Status404NotFound;
                return Task.CompletedTask;
            });
            app.MapGet("/data/{id}/receive", ParkedResponseAsync);
        });

        var context = new StubSessionContext(hub.CreateClient());
        var transferId = Guid.NewGuid().ToString();
        await using var manager = new HttpStreamManager(context, context, transferId, hub.DataUrl, affinity: null);

        manager.StartStreams(
            isSender: true,
            writeAction: async (stream, token) =>
            {
                await stream.WriteAsync(new byte[] { 1 }, token);
                await stream.FlushAsync(token);
                await Task.Delay(Timeout.Infinite, token);
            },
            readAction: ReadUntilCancelledAsync,
            sessionToken: CancellationToken.None);

        var result = await manager.SenderTask!.WaitAsync(Bound);

        Assert.Equal(StreamOutcome.Gone, result.Outcome);
    }

    [Fact]
    public async Task HandshakeWait_EndsWhenItsAttemptIsCancelled()
    {
        // The Pong never comes; what ends the wait is the attempt, not the 10 s PongTimeout.
        var context = new StubSessionContext();
        var handshake = new ReconnectionHandler(context, Guid.NewGuid().ToString(), affinity: null);
        handshake.PrepareForHandshake();

        using var attempt = new CancellationTokenSource();
        var waiting = handshake.ExecuteHandshakeAsync(Stream.Null, Guid.NewGuid(), attempt.Token);

        await Task.Delay(100);
        Assert.False(waiting.IsCompleted);

        attempt.Cancel();

        Assert.Equal(-1, await waiting.WaitAsync(TimeSpan.FromSeconds(2)));
    }

    private static async Task ReadAtLeastAsync(HttpContext ctx, int count)
    {
        var buffer = new byte[count];
        var read = 0;
        while (read < count)
        {
            var n = await ctx.Request.Body.ReadAsync(buffer.AsMemory(read));
            if (n == 0) return;
            read += n;
        }
    }

    private static async Task ParkedResponseAsync(HttpContext ctx)
    {
        await ctx.Response.StartAsync();
        try { await Task.Delay(Timeout.Infinite, ctx.RequestAborted); }
        catch (OperationCanceledException) { }
    }

    private static async Task ReadUntilCancelledAsync(Stream stream, CancellationToken token)
    {
        var buffer = new byte[256];
        try
        {
            while (await stream.ReadAsync(buffer, token) > 0) { }
        }
        catch (OperationCanceledException) { }
    }

    private sealed class LoopbackHub : IAsyncDisposable
    {
        private readonly WebApplication _app;

        private LoopbackHub(WebApplication app, string baseUrl)
        {
            _app = app;
            DataUrl = baseUrl + "/data";
        }

        public string DataUrl { get; }

        public static async Task<LoopbackHub> StartAsync(Action<WebApplication> map)
        {
            var builder = WebApplication.CreateSlimBuilder();
            builder.WebHost.UseUrls("http://127.0.0.1:0");
            builder.Logging.ClearProviders();

            var app = builder.Build();
            map(app);
            await app.StartAsync();
            return new LoopbackHub(app, app.Urls.First());
        }

        // The handler and its unbounded timeout are the ones the client builder creates.
        public HttpClient CreateClient() =>
            new(new SocketsHttpHandler()) { Timeout = Timeout.InfiniteTimeSpan };

        public async ValueTask DisposeAsync()
        {
            await _app.StopAsync();
            await _app.DisposeAsync();
        }
    }
}
