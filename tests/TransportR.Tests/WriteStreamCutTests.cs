using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using TransportR.Client.SignalR.Sessions;
using TransportR.Protocol;
using TransportR.Tests.TestHelpers;

namespace TransportR.Tests;

/// <summary>
/// A write stream that the hub cuts, or refuses, while its client has nothing to write has to fail
/// the attempt at once. A client with a full send window, or a receiver with nothing to
/// acknowledge, only learns of a cut on its next write otherwise, and that write can be minutes
/// away.
/// </summary>
/// <remarks>
/// These run against a real socket. What is under test is the order in which
/// <see cref="System.Net.Http.SocketsHttpHandler"/> sends a request body and reads the response,
/// which an in-memory handler does not reproduce: a handler that throws from SendAsync is already
/// green on the code before the fix.
/// </remarks>
public class WriteStreamCutTests
{
    // The recovery target is about 3 s after the link is back; a stream failing on its own
    // account has no business taking longer than that to be noticed.
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(3);

    [Fact]
    public async Task SenderDataStream_CutWhileIdle_FailsTheAttemptAtOnce()
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
        var handshake = new ReconnectionHandler(context, transferId.ToString());
        await using var manager = new HttpStreamManager(context, context, transferId.ToString(), hub.DataUrl);

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
    public async Task ReceiverControlStream_CutWhileNothingIsWritten_FailsTheAttemptAtOnce()
    {
        var arrived = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var hub = await LoopbackHub.StartAsync(app =>
        {
            app.MapPost("/data/{id}/send", async (HttpContext ctx) =>
            {
                arrived.TrySetResult();
                BeginReading(ctx);
                await Task.Delay(200);
                ctx.Abort();
            });
            app.MapGet("/data/{id}/receive", ParkedResponseAsync);
        });

        var context = new StubSessionContext(hub.CreateClient());
        var transferId = Guid.NewGuid().ToString();
        await using var manager = new HttpStreamManager(context, context, transferId, hub.DataUrl);

        manager.StartStreams(
            isSender: false,
            writeAction: (_, token) => Task.Delay(Timeout.Infinite, token),
            readAction: ReadUntilCancelledAsync,
            sessionToken: CancellationToken.None);

        // A POST whose body is still empty must reach the hub all the same: until it does, the hub
        // has no source to release when the peer goes, and nothing to cut.
        await arrived.Task.WaitAsync(Bound);

        var result = await manager.ReceiverTask!.WaitAsync(Bound);

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
                ctx.Response.StatusCode = StatusCodes.Status404NotFound;
                return Task.CompletedTask;
            });
            app.MapGet("/data/{id}/receive", ParkedResponseAsync);
        });

        var context = new StubSessionContext(hub.CreateClient());
        var transferId = Guid.NewGuid().ToString();
        await using var manager = new HttpStreamManager(context, context, transferId, hub.DataUrl);

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
        var handshake = new ReconnectionHandler(context, Guid.NewGuid().ToString());
        handshake.PrepareForHandshake();

        using var attempt = new CancellationTokenSource();
        var waiting = handshake.ExecuteHandshakeAsync(Stream.Null, Guid.NewGuid(), attempt.Token);

        await Task.Delay(100);
        Assert.False(waiting.IsCompleted);

        attempt.Cancel();

        Assert.Equal(-1, await waiting.WaitAsync(TimeSpan.FromSeconds(2)));
    }

    /// <summary>Reads without waiting for the result, which is what lets a client's 100-continue through.</summary>
    private static void BeginReading(HttpContext ctx) =>
        _ = Task.Run(async () =>
        {
            var buffer = new byte[256];
            try { while (await ctx.Request.Body.ReadAsync(buffer) > 0) { } }
            catch (Exception) { }
        });

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
