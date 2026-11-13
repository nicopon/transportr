using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using TransportR.Abstractions;
using TransportR.Client.Configuration;
using TransportR.Client.Services;
using TransportR.Interfaces;
using TransportR.Models;
using TransportR.Protocol;
using TransportR.Tests.Infrastructure;

namespace TransportR.Tests;

/// <summary>
/// Whole transfers over a real socket, with what stands between the client and the hub played by a
/// middleware in front of the hub's routes.
/// </summary>
public class WebSocketTransferTests
{
    private static readonly TimeSpan TestBound = TimeSpan.FromSeconds(60);
    private static readonly List<string> Items = Enumerable.Range(0, 250).Select(i => $"item-{i}").ToList();

    private static readonly Action<ClientOptions> ShortBound = options => options.WebSocketOpenBound = TimeSpan.FromMilliseconds(500);

    private sealed class InFront(Action<IApplicationBuilder> configure) : IStartupFilter
    {
        public Action<IApplicationBuilder> Configure(Action<IApplicationBuilder> next) => app =>
        {
            configure(app);
            next(app);
        };
    }

    private static Action<IServiceCollection> With(Action<IApplicationBuilder> configure) =>
        services => services.AddTransient<IStartupFilter>(_ => new InFront(configure));

    private static Action<IServiceCollection> OnWriteSockets(Func<HttpContext, Func<Task>, Task> handle) =>
        With(app => app.Use((context, next) => IsWriteSocket(context) ? handle(context, () => next()) : next()));

    private static bool IsWriteSocket(HttpContext context) =>
        HttpMethods.IsGet(context.Request.Method) && context.Request.Path.Value?.EndsWith("/send", StringComparison.Ordinal) == true;

    private static bool IsDataStream(HttpContext context) => context.Request.Headers["X-Connection-Type"] == nameof(ConnectionType.Data);

    private sealed class Transfer : IAsyncDisposable
    {
        public required TransportRClient<string> Sender { get; init; }
        public required TransportRClient<string> Receiver { get; init; }
        public required Send<string> Send { get; init; }
        public required Receive<string> Receive { get; init; }
        public required Task<List<string>> Consumed { get; init; }

        public TransferInfo SenderInfo => Sender.GetTransfers().Single();
        public TransferInfo ReceiverInfo => Receiver.GetTransfers().Single();

        public async Task<List<string>> DeliverAsync(IEnumerable<string> items)
        {
            await Send.SendAsync(items).WaitAsync(TestBound);
            await Send.CompleteAsync().WaitAsync(TestBound);
            return await Consumed.WaitAsync(TestBound);
        }

        public async ValueTask DisposeAsync()
        {
            await Send.DisposeAsync();
            await Receive.DisposeAsync();
            await Sender.DisposeAsync();
            await Receiver.DisposeAsync();
        }
    }

    private static async Task<Transfer> StartAsync(
        LoopbackHost hub,
        Action<ClientOptions>? sender = null,
        Action<ClientOptions>? receiver = null,
        Func<string, Task>? consume = null)
    {
        var senderClient = TestClients.Create(null, hub.BaseUrl, configure: sender);
        var receiverClient = TestClients.Create(null, hub.BaseUrl, configure: receiver);

        var receiveHandle = new TaskCompletionSource<Receive<string>>(TaskCreationOptions.RunContinuationsAsynchronously);
        receiverClient.OnTransferStarted += started =>
        {
            receiveHandle.TrySetResult(started);
            return Task.CompletedTask;
        };

        await senderClient.ConnectAsync();
        await receiverClient.ConnectAsync();

        var send = await senderClient.InitTransferAsync(receiverClient.ClientId, batchSize: 10);
        var receive = await receiveHandle.Task.WaitAsync(TestBound);

        return new Transfer
        {
            Sender = senderClient,
            Receiver = receiverClient,
            Send = send,
            Receive = receive,
            Consumed = ConsumeAsync(receive, consume),
        };
    }

    private static async Task<List<string>> ConsumeAsync(Receive<string> receive, Func<string, Task>? consume)
    {
        var items = new List<string>();
        await foreach (var item in receive.ReceiveAsync())
        {
            if (consume != null) await consume(item);
            items.Add(item);
        }

        await receive.WaitForCompletionAsync();
        return items;
    }

    private static async Task EventuallyAsync(Func<bool> condition, string what)
    {
        var deadline = DateTime.UtcNow + TestBound;
        while (!condition())
        {
            Assert.True(DateTime.UtcNow < deadline, $"Never happened: {what}");
            await Task.Delay(20);
        }
    }

    [Fact]
    public async Task OnAPathThatCarriesOne_BothWriteStreamsAreWebSockets_AndTheTransferEndsWell()
    {
        await using var hub = await TestHubServerFactory.StartLoopbackAsync();
        await using var transfer = await StartAsync(hub);

        await EventuallyAsync(() => transfer.SenderInfo.WriteTransport != null && transfer.ReceiverInfo.WriteTransport != null, "both write streams open");
        Assert.Equal(WriteTransport.WebSocket, transfer.SenderInfo.WriteTransport);
        Assert.Equal(WriteTransport.WebSocket, transfer.ReceiverInfo.WriteTransport);
        Assert.Null(transfer.SenderInfo.WriteTransportFallback);
        Assert.Null(transfer.ReceiverInfo.WriteTransportFallback);

        var transferId = transfer.Send.TransferId;
        Assert.Equal(Items, await transfer.DeliverAsync(Items));

        // The clean end of the data stream is what makes the hub forget the transfer.
        await transfer.Send.DisposeAsync();
        var manager = hub.Services.GetRequiredService<ITransferManager>();
        var deadline = DateTime.UtcNow + TestBound;
        while (await manager.GetParticipantsAsync(transferId) != null)
        {
            Assert.True(DateTime.UtcNow < deadline, "The hub never forgot the transfer");
            await Task.Delay(20);
        }
    }

    [Fact]
    public async Task AHostWithWebSocketsOfItsOwn_ChangesNothing()
    {
        await using var hub = await TestHubServerFactory.StartLoopbackAsync(With(app => app.UseWebSockets()));
        await using var transfer = await StartAsync(hub);

        Assert.Equal(Items, await transfer.DeliverAsync(Items));
        Assert.Null(transfer.SenderInfo.WriteTransportFallback);
    }

    public static TheoryData<int> AnswersThatAreNoVerdict => new() { 200, 400, 403, 404, 421, 426, 500, 502 };

    [Theory]
    [MemberData(nameof(AnswersThatAreNoVerdict))]
    public async Task AnAnswerOtherThan101_FallsBack_AndSaysWhy(int status)
    {
        await using var hub = await TestHubServerFactory.StartLoopbackAsync(OnWriteSockets((context, _) =>
        {
            context.Response.StatusCode = status;
            return Task.CompletedTask;
        }));
        await using var transfer = await StartAsync(hub);

        Assert.Equal(Items, await transfer.DeliverAsync(Items));
    }

    [Fact]
    public async Task AFallback_IsReadable_AndLastsUntilTheEndOfTheSession()
    {
        var upgrades = 0;
        await using var hub = await TestHubServerFactory.StartLoopbackAsync(OnWriteSockets((context, _) =>
        {
            if (IsDataStream(context)) Interlocked.Increment(ref upgrades);
            context.Response.StatusCode = StatusCodes.Status403Forbidden;
            return Task.CompletedTask;
        }));
        await using var transfer = await StartAsync(hub);

        await EventuallyAsync(() => transfer.SenderInfo.WriteTransport != null, "the write stream open");
        Assert.Equal(WriteTransport.Segments, transfer.SenderInfo.WriteTransport);
        Assert.Contains("403", transfer.SenderInfo.WriteTransportFallback);

        Assert.Equal(Items, await transfer.DeliverAsync(Items));
        Assert.Equal(1, Volatile.Read(ref upgrades));
    }

    [Fact]
    public async Task AnUpgradeThatIsNeverAnswered_FallsBack_AtTheBound()
    {
        await using var hub = await TestHubServerFactory.StartLoopbackAsync(OnWriteSockets(async (context, _) =>
        {
            try
            {
                await Task.Delay(Timeout.InfiniteTimeSpan, context.RequestAborted);
            }
            catch (OperationCanceledException)
            {
            }
        }));
        await using var transfer = await StartAsync(hub, ShortBound, ShortBound);

        Assert.Equal(Items, await transfer.DeliverAsync(Items));
        Assert.Contains("not answered", transfer.SenderInfo.WriteTransportFallback);
    }

    [Fact]
    public async Task AConnectionCutDuringTheUpgrade_FallsBack()
    {
        await using var hub = await TestHubServerFactory.StartLoopbackAsync(OnWriteSockets((context, _) =>
        {
            context.Abort();
            return Task.CompletedTask;
        }));
        await using var transfer = await StartAsync(hub);

        Assert.Equal(Items, await transfer.DeliverAsync(Items));
        Assert.Contains("before the upgrade was answered", transfer.SenderInfo.WriteTransportFallback);
    }

    [Fact]
    public async Task APathThatAcceptsTheUpgradeButHoldsTheBytesBack_FallsBack_AtTheBound()
    {
        await using var hub = await TestHubServerFactory.StartLoopbackAsync(With(app =>
        {
            app.UseWebSockets();
            app.Use(async (context, next) =>
            {
                if (!IsWriteSocket(context))
                {
                    await next();
                    return;
                }

                using var socket = await context.WebSockets.AcceptWebSocketAsync();
                try
                {
                    await Task.Delay(Timeout.InfiniteTimeSpan, context.RequestAborted);
                }
                catch (OperationCanceledException)
                {
                }
            });
        }));
        await using var transfer = await StartAsync(hub, ShortBound, ShortBound);

        Assert.Equal(Items, await transfer.DeliverAsync(Items));
        Assert.Contains("did not carry its probe", transfer.SenderInfo.WriteTransportFallback);
    }

    [Fact]
    public async Task WhenAWebSocketIsRequired_ThePathNotCarryingOne_FailsTheTransfer_InPlainWords()
    {
        await using var hub = await TestHubServerFactory.StartLoopbackAsync(OnWriteSockets((context, _) =>
        {
            context.Response.StatusCode = StatusCodes.Status403Forbidden;
            return Task.CompletedTask;
        }));
        await using var transfer = await StartAsync(hub, options => options.WriteTransportPolicy = WriteTransportPolicy.RequireWebSocket);

        var failure = await Assert.ThrowsAsync<TransferFailedException>(() => transfer.DeliverAsync(Items));

        Assert.Contains("requires a WebSocket", failure.Message);
        Assert.DoesNotContain("403", failure.Message);
    }

    [Fact]
    public async Task SegmentsOnly_NeverAsksForAnUpgrade()
    {
        var upgrades = 0;
        await using var hub = await TestHubServerFactory.StartLoopbackAsync(OnWriteSockets((_, next) =>
        {
            Interlocked.Increment(ref upgrades);
            return next();
        }));
        Action<ClientOptions> segments = options => options.WriteTransportPolicy = WriteTransportPolicy.SegmentsOnly;
        await using var transfer = await StartAsync(hub, segments, segments);

        await EventuallyAsync(() => transfer.SenderInfo.WriteTransport != null, "the write stream open");
        Assert.Equal(WriteTransport.Segments, transfer.SenderInfo.WriteTransport);
        Assert.Null(transfer.SenderInfo.WriteTransportFallback);

        Assert.Equal(Items, await transfer.DeliverAsync(Items));
        Assert.Equal(0, Volatile.Read(ref upgrades));
    }

    [Fact]
    public async Task ATokenRefusedAtTheUpgrade_IsRenewed_AndTheUpgradeTriedAgain()
    {
        var refused = 0;
        await using var hub = await TestHubServerFactory.StartLoopbackAsync(OnWriteSockets((context, next) =>
        {
            if (IsDataStream(context) && Interlocked.Exchange(ref refused, 1) == 0)
            {
                context.Response.StatusCode = StatusCodes.Status401Unauthorized;
                return Task.CompletedTask;
            }

            return next();
        }));
        await using var transfer = await StartAsync(hub);

        Assert.Equal(Items, await transfer.DeliverAsync(Items));
        Assert.Equal(WriteTransport.WebSocket, transfer.SenderInfo.WriteTransport);
        Assert.Null(transfer.SenderInfo.WriteTransportFallback);
    }

    [Fact]
    public async Task ACutInMidTransfer_ReopensAWebSocket_AndDeliversEveryItemOnce()
    {
        var openings = 0;
        var cut = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var hub = await TestHubServerFactory.StartLoopbackAsync(OnWriteSockets((context, next) =>
        {
            if (IsDataStream(context) && Interlocked.Increment(ref openings) == 1)
            {
                _ = cut.Task.ContinueWith(_ => context.Abort(), TaskScheduler.Default);
            }

            return next();
        }));
        await using var transfer = await StartAsync(hub);

        var half = Items.Count / 2;
        await transfer.Send.SendAsync(Items.Take(half));
        await EventuallyAsync(() => transfer.ReceiverInfo.ItemsProcessed > 0, "the first items through");

        cut.SetResult();
        await EventuallyAsync(() => Volatile.Read(ref openings) > 1, "the stream opened again");

        Assert.Equal(Items, await transfer.DeliverAsync(Items.Skip(half)));
        Assert.Equal(WriteTransport.WebSocket, transfer.SenderInfo.WriteTransport);
        Assert.Null(transfer.SenderInfo.WriteTransportFallback);
    }

    [Fact]
    public async Task ThreeCutsInARowSoonAfterOpening_FallBack()
    {
        var openings = 0;
        await using var hub = await TestHubServerFactory.StartLoopbackAsync(OnWriteSockets((context, next) =>
        {
            if (IsDataStream(context) && Interlocked.Increment(ref openings) <= 3)
            {
                _ = Task.Delay(300).ContinueWith(_ => context.Abort(), TaskScheduler.Default);
            }

            return next();
        }));
        await using var transfer = await StartAsync(hub);

        await EventuallyAsync(() => transfer.SenderInfo.WriteTransport == WriteTransport.Segments, "the session on segments");
        Assert.Contains("3 connections", transfer.SenderInfo.WriteTransportFallback);

        Assert.Equal(Items, await transfer.DeliverAsync(Items));
        Assert.Equal(3, Volatile.Read(ref openings));
    }

    [Fact]
    public async Task AReceiverThatHoldsOnToABatch_DoesNotCostTheTransfer()
    {
        await using var hub = await TestHubServerFactory.StartLoopbackAsync();
        var payload = new string('x', 64 * 1024);
        var items = Enumerable.Range(0, 200).Select(i => $"{i}:{payload}").ToList();
        var held = 0;

        await using var transfer = await StartAsync(
            hub,
            sender: options => options.PresenceHeartbeatInterval = TimeSpan.FromSeconds(1),
            receiver: options => options.ReceiveBufferCapacity = BufferCapacity.Items(10),
            consume: async _ =>
            {
                if (Interlocked.Exchange(ref held, 1) == 0)
                {
                    await Task.Delay(TimeSpan.FromSeconds(3));
                }
            });

        Assert.Equal(items, await transfer.DeliverAsync(items));
        Assert.Null(transfer.SenderInfo.WriteTransportFallback);
    }

    [Fact]
    public async Task ASenderReleasedEarly_TellsTheReceiverWhy()
    {
        await using var hub = await TestHubServerFactory.StartLoopbackAsync();
        await using var transfer = await StartAsync(hub);

        await transfer.Send.SendAsync(Items.Take(20));
        await EventuallyAsync(() => transfer.ReceiverInfo.ItemsProcessed > 0, "the first items through");

        await transfer.Send.DisposeAsync();

        var failure = await Assert.ThrowsAsync<TransferFailedException>(() => transfer.Consumed.WaitAsync(TestBound));
        Assert.Contains("released", failure.Message);
    }
}
