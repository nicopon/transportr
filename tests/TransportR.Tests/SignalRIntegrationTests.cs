using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.SignalR;
using TransportR.Protocol;
using Microsoft.AspNetCore.SignalR.Client;
using TransportR.Models;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using TransportR.Abstractions;
using TransportR.Client.SignalR.Configuration;
using TransportR.Client.SignalR.Internal;
using TransportR.Client.SignalR.Services;
using TransportR.Hub.SignalR;
using TransportR.Interfaces;
using TransportR.Tests.Infrastructure;
using Xunit.Abstractions;


using TransportR.Serialization.MessagePack; // Extension

namespace TransportR.Tests;

public class SignalRIntegrationTests
{
    private readonly ITestOutputHelper _output;

    public SignalRIntegrationTests(ITestOutputHelper output)
    {
        _output = output;
    }

    [Fact]
    public async Task InitTransfer_ShouldSucceed_WhenHandlersReturnValues()
    {
        // 1. Start Server
        using var host = TestHubServerFactory.CreateHost();
        var server = host.GetTestServer();
        var handler = server.CreateHandler();

        // 2. Create Clients
        await using var sender = CreateClient(handler, "http://localhost");
        await using var receiver = CreateClient(handler, "http://localhost");

        // 3. Connect clients
        // Note: In dev mode (TestHubServerFactory), groups are auto-assigned to ["test"]
        await sender.ConnectAsync();
        await receiver.ConnectAsync();
        
        // Wait briefly for registration
        await Task.Delay(500);

        // 4. Setup Receiver to "Accept" the transfer
        bool getReadyCalled = false;
        receiver.OnTransferStarted += (transfer) => 
        {
            getReadyCalled = true;
            return Task.CompletedTask;
        };

        // 5. Init Transfer (This is where it failed with HubException)
        var sendSession = await sender.InitTransferAsync(receiver.ClientId, batchSize: 100);

        // Assert
        Assert.NotNull(sendSession);
        Assert.True(getReadyCalled, "Receiver OnTransferStarted should have been triggered");
    }

    [Fact]
    public async Task HostInitiatedTransfer_DeliversEveryItem()
    {
        using var host = TestHubServerFactory.CreateHost();
        var handler = host.GetTestServer().CreateHandler();

        await using var sender = CreateClient(handler, "http://localhost");
        await using var receiver = CreateClient(handler, "http://localhost");

        var sendHandle = new TaskCompletionSource<Send<string>>(TaskCreationOptions.RunContinuationsAsynchronously);
        var receiveHandle = new TaskCompletionSource<Receive<string>>(TaskCreationOptions.RunContinuationsAsynchronously);
        sender.OnSendRequested += send => { sendHandle.TrySetResult(send); return Task.CompletedTask; };
        receiver.OnTransferStarted += receive => { receiveHandle.TrySetResult(receive); return Task.CompletedTask; };

        await sender.ConnectAsync();
        await receiver.ConnectAsync();

        // Neither client asks for anything: the host names both ends.
        var initiator = host.Services.GetRequiredService<ITransferInitiator>();
        var transferId = await initiator.InitTransferAsync(sender.ClientId, receiver.ClientId, batchSize: 10, timeoutMs: 20_000);

        // Both handles are handed out inside the handshake, before the host has the id to match
        // them against - a host that routes by transfer id has to hold them until it does.
        Assert.True(sendHandle.Task.IsCompleted, "the sender's handle must be handed out before InitTransferAsync returns");
        Assert.True(receiveHandle.Task.IsCompleted, "the receiver's handle must be handed out before InitTransferAsync returns");

        await using var send = await sendHandle.Task;
        await using var receive = await receiveHandle.Task;
        Assert.Equal(transferId, send.TransferId);
        Assert.Equal(transferId, receive.TransferId);

        var expected = Enumerable.Range(0, 250).Select(i => $"item-{i}").ToList();
        var consumer = Task.Run(async () =>
        {
            var items = new List<string>();
            await foreach (var item in receive.ReceiveAsync())
            {
                items.Add(item);
            }
            await receive.WaitForCompletionAsync();
            return items;
        });

        await send.SendAsync(expected);
        await send.CompleteAsync().WaitAsync(TimeSpan.FromSeconds(30));

        Assert.Equal(expected, await consumer.WaitAsync(TimeSpan.FromSeconds(30)));
    }

    [Fact]
    public async Task SenderInitiatedTransfer_DeliversEveryItem()
    {
        using var host = TestHubServerFactory.CreateHost();
        var handler = host.GetTestServer().CreateHandler();

        await using var sender = CreateClient(handler, "http://localhost");
        await using var receiver = CreateClient(handler, "http://localhost");

        var receiveHandle = new TaskCompletionSource<Receive<string>>(TaskCreationOptions.RunContinuationsAsynchronously);
        receiver.OnTransferStarted += receive => { receiveHandle.TrySetResult(receive); return Task.CompletedTask; };

        await sender.ConnectAsync();
        await receiver.ConnectAsync();

        await using var send = await sender.InitTransferAsync(receiver.ClientId, batchSize: 10);
        await using var receive = await receiveHandle.Task;

        var expected = Enumerable.Range(0, 250).Select(i => $"item-{i}").ToList();
        var consumer = Task.Run(async () =>
        {
            var items = new List<string>();
            await foreach (var item in receive.ReceiveAsync())
            {
                items.Add(item);
            }
            await receive.WaitForCompletionAsync();
            return items;
        });

        await send.SendAsync(expected);
        await send.CompleteAsync().WaitAsync(TimeSpan.FromSeconds(30));

        Assert.Equal(expected, await consumer.WaitAsync(TimeSpan.FromSeconds(30)));
    }

    [Fact]
    public async Task SenderInitiatedTransfer_Opens_WhenTheSenderInvokesTheHubDuringIt()
    {
        // The hub runs one invocation at a time per client, and InitTransfer is one. Anything else the
        // sender invokes while it is in flight - its presence beat, the statistics of another transfer -
        // queues behind it, and so does every message after that one. A handshake that waits on the
        // sender itself waits behind its own queue. The sender here invokes the hub from inside the
        // callback a handshake makes to it, which puts an invocation in that window every time; a
        // handshake that makes no such callback never meets it.
        using var host = TestHubServerFactory.CreateHost();
        var handler = host.GetTestServer().CreateHandler();

        await using var receiver = await StartRawClientAsync(handler, connection =>
            connection.On<string, Guid, bool>(nameof(ICommandClient.GetReadyToReceive), (_, _) => true));
        await using var sender = await StartRawClientAsync(handler, connection =>
            connection.On<string, int, Guid, bool>(nameof(ICommandClient.GetReadyToSend), async (_, _, _) =>
            {
                await connection.SendAsync(nameof(ICommandHub.Heartbeat));
                return true;
            }));

        var receiverId = Guid.NewGuid();
        await receiver.InvokeAsync(nameof(ICommandHub.Connect), receiverId, new[] { "test" });
        await sender.InvokeAsync(nameof(ICommandHub.Connect), Guid.NewGuid(), new[] { "test" });

        // A stalled handshake runs out its whole timeout; an unstalled one takes milliseconds.
        const int handshakeTimeoutMs = 3_000;
        var clock = System.Diagnostics.Stopwatch.StartNew();
        var transferId = await sender.InvokeAsync<string>(nameof(ICommandHub.InitTransfer), receiverId, 10, handshakeTimeoutMs);

        Assert.False(string.IsNullOrEmpty(transferId));
        Assert.True(clock.ElapsedMilliseconds < handshakeTimeoutMs / 2, $"the handshake took {clock.ElapsedMilliseconds} ms");
    }

    private static async Task<HubConnection> StartRawClientAsync(HttpMessageHandler handler, Action<HubConnection> register)
    {
        var connection = new HubConnectionBuilder()
            .WithUrl(TestHubServerFactory.HubUrl, options => options.HttpMessageHandlerFactory = _ => handler)
            .Build();
        register(connection);
        await connection.StartAsync();
        return connection;
    }

    [Fact]
    public async Task Transfer_DeliversEveryItem_WhenTheHostRegisteredNoProgressMonitor()
    {
        using var host = CreateHostWithoutProgressMonitor();
        var handler = host.GetTestServer().CreateHandler();

        await using var sender = CreateClient(handler, "http://localhost");
        await using var receiver = CreateClient(handler, "http://localhost");

        var receiveHandle = new TaskCompletionSource<Receive<string>>(TaskCreationOptions.RunContinuationsAsynchronously);
        receiver.OnTransferStarted += receive => { receiveHandle.TrySetResult(receive); return Task.CompletedTask; };

        await sender.ConnectAsync().WaitAsync(TimeSpan.FromSeconds(10));
        await receiver.ConnectAsync().WaitAsync(TimeSpan.FromSeconds(10));

        await using var send = await sender.InitTransferAsync(receiver.ClientId, batchSize: 10).WaitAsync(TimeSpan.FromSeconds(10));
        await using var receive = await receiveHandle.Task.WaitAsync(TimeSpan.FromSeconds(10));

        var expected = Enumerable.Range(0, 50).Select(i => $"item-{i}").ToList();
        var consumer = Task.Run(async () =>
        {
            var items = new List<string>();
            await foreach (var item in receive.ReceiveAsync())
            {
                items.Add(item);
            }
            await receive.WaitForCompletionAsync();
            return items;
        });

        await send.SendAsync(expected).WaitAsync(TimeSpan.FromSeconds(10));
        await send.CompleteAsync().WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Equal(expected, await consumer.WaitAsync(TimeSpan.FromSeconds(10)));
    }

    private static IHost CreateHostWithoutProgressMonitor()
    {
        // Deliberately omits the IHubProgressMonitor registration every other factory in this
        // project adds, to exercise AddDataHub()'s own default.
        return new HostBuilder()
            .ConfigureWebHost(webBuilder =>
            {
                webBuilder
                    .UseTestServer()
                    .ConfigureServices(services =>
                    {
                        services.AddLogging(logging => logging.AddConsole().SetMinimumLevel(LogLevel.Debug));

                        services.AddDataHub()
                                .UseDevMode(options =>
                                {
                                    options.AllowAnonymous = true;
                                    options.DefaultGroups = ["test"];
                                })
                                .Build();
                    })
                    .Configure(app =>
                    {
                        app.UseRouting();
                        app.UseEndpoints(endpoints =>
                        {
                            endpoints.MapDataHub(requireAuth: false);
                        });
                    });
            })
            .Start();
    }

    [Fact]
    public async Task Abort_WhenReceiverReleasesEarly_MakesTheSenderFailFast()
    {
        using var host = TestHubServerFactory.CreateHost();
        var handler = host.GetTestServer().CreateHandler();

        // A window of one batch: SendAsync has nowhere to go once the first batch goes
        // unacknowledged, which is what "parked on the window" means here.
        await using var sender = CreateClient(handler, "http://localhost", o => o.MaxInFlightBatches = 1);
        await using var receiver = CreateClient(handler, "http://localhost");

        var receiveHandle = new TaskCompletionSource<Receive<string>>(TaskCreationOptions.RunContinuationsAsynchronously);
        receiver.OnTransferStarted += receive => { receiveHandle.TrySetResult(receive); return Task.CompletedTask; };

        await sender.ConnectAsync();
        await receiver.ConnectAsync();

        await using var send = await sender.InitTransferAsync(receiver.ClientId, batchSize: 1);
        var receive = await receiveHandle.Task;

        // Fills the window's only slot. Never acknowledged: the receiver gives up next, without
        // ever reading it.
        await send.SendAsync("item-1");
        await receive.DisposeAsync();

        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        await Assert.ThrowsAsync<TransferFailedException>(
            () => send.SendAsync("item-2").WaitAsync(TimeSpan.FromSeconds(5)));
        stopwatch.Stop();

        // Measured 60s on the code before Abort (CompleteStallTimeout). The 5s bound above only
        // keeps a regression from hanging the test; this is the actual claim.
        Assert.True(stopwatch.Elapsed < TimeSpan.FromSeconds(1),
            $"Expected the Abort to fail the send in under a second; took {stopwatch.Elapsed}.");
    }

    [Fact]
    public async Task Receiver_FailsFast_WhenTheSenderFailsOnItsStallBound()
    {
        using var host = TestHubServerFactory.CreateHost();
        var handler = host.GetTestServer().CreateHandler();

        // A window of one batch and a short bound: the sender gives up as soon as the receiver stops
        // acknowledging, which is what the bound is for.
        await using var sender = CreateClient(handler, "http://localhost", o =>
        {
            o.MaxInFlightBatches = 1;
            o.CompleteStallTimeout = TimeSpan.FromMilliseconds(500);
        });

        // A batch is acknowledged only once it lands in the receive buffer. With room for one item
        // and nobody consuming, the second batch is delivered but never acknowledged.
        await using var receiver = CreateClient(handler, "http://localhost", o => o.ReceiveBufferCapacity = BufferCapacity.Items(1));

        var receiveHandle = new TaskCompletionSource<Receive<string>>(TaskCreationOptions.RunContinuationsAsynchronously);
        receiver.OnTransferStarted += receive => { receiveHandle.TrySetResult(receive); return Task.CompletedTask; };

        await sender.ConnectAsync();
        await receiver.ConnectAsync();

        var send = await sender.InitTransferAsync(receiver.ClientId, batchSize: 1);
        var receive = await receiveHandle.Task;

        await send.SendAsync("item-1");
        await send.SendAsync("item-2");
        await Assert.ThrowsAsync<TransferFailedException>(
            () => send.SendAsync("item-3").WaitAsync(TimeSpan.FromSeconds(10)));
        await send.DisposeAsync();

        // The consumer only starts now, after the sender has failed, so the receiver has not yet
        // read past the batch it was parked on.
        var consumer = Task.Run(async () =>
        {
            await foreach (var _ in receive.ReceiveAsync())
            {
            }
            await receive.WaitForCompletionAsync();
        });

        var outcome = await Task.WhenAny(consumer, Task.Delay(TimeSpan.FromSeconds(10)));
        Assert.True(outcome == consumer,
            $"The receiver reached no outcome 10s after its sender failed (status: {receiver.GetTransfers().Single().Status}).");
        await Assert.ThrowsAsync<TransferFailedException>(() => consumer);

        await receive.DisposeAsync();
    }

    [Fact]
    public async Task Abort_WhenSenderReleasesMidFlight_MakesTheReceiverFailFast()
    {
        // On Kestrel: in memory the receiver's DisposeAsync below waited, in a fifth to a quarter of
        // repeated runs, on a stream request the hub had not let go of, and the run sat there until
        // the bridge's idle timeout. Over a socket the client ends its own request; 200 repeated runs
        // there never waited.
        await using var host = await TestHubServerFactory.StartLoopbackAsync();

        await using var sender = CreateClient(null, host.BaseUrl);
        await using var receiver = CreateClient(null, host.BaseUrl);

        var receiveHandle = new TaskCompletionSource<Receive<string>>(TaskCreationOptions.RunContinuationsAsynchronously);
        receiver.OnTransferStarted += receive => { receiveHandle.TrySetResult(receive); return Task.CompletedTask; };

        await sender.ConnectAsync();
        await receiver.ConnectAsync();

        var send = await sender.InitTransferAsync(receiver.ClientId, batchSize: 1);
        var receive = await receiveHandle.Task;

        var receiveTask = Task.Run(async () =>
        {
            var items = new List<string>();
            await foreach (var item in receive.ReceiveAsync())
            {
                items.Add(item);
            }
            return items;
        });

        // A couple of items make it through, then the sender walks away mid-stream without ever
        // calling CompleteAsync.
        await send.SendAsync("item-1");
        await send.SendAsync("item-2");
        await send.DisposeAsync();

        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        await Assert.ThrowsAsync<TransferFailedException>(() => receiveTask.WaitAsync(TimeSpan.FromSeconds(10)));
        stopwatch.Stop();

        // Measured over 150s on the code before Abort (the sender kept heartbeating, so the hub's
        // cleanup never judged the transfer inactive). The 10s bound above only keeps a regression
        // from hanging the test; this is the actual claim.
        Assert.True(stopwatch.Elapsed < TimeSpan.FromSeconds(1),
            $"Expected the Abort to fail the receive in under a second; took {stopwatch.Elapsed}.");

        await receive.DisposeAsync();
    }

    [Fact]
    public async Task Abort_WhileTheReceiverConsumerIsStuck_StillFailsTheReceiver()
    {
        await using var host = await TestHubServerFactory.StartLoopbackAsync();

        // Capped at 50 opens: a receiver that reopens in a loop would otherwise exhaust the
        // machine's ephemeral ports while this test reports it.
        var opens = new DataStreamOpenCounter(limit: 50);

        await using var sender = CreateClient(null, host.BaseUrl, o =>
        {
            o.MaxInFlightBatches = 1;
            o.CompleteStallTimeout = TimeSpan.FromSeconds(1);
        });
        await using var receiver = CreateClient(null, host.BaseUrl, o => o.ReceiveBufferCapacity = BufferCapacity.Items(1), networkHandler: opens);

        var receiveHandle = new TaskCompletionSource<Receive<string>>(TaskCreationOptions.RunContinuationsAsynchronously);
        receiver.OnTransferStarted += receive => { receiveHandle.TrySetResult(receive); return Task.CompletedTask; };

        await sender.ConnectAsync();
        await receiver.ConnectAsync();

        var send = await sender.InitTransferAsync(receiver.ClientId, batchSize: 1);
        await using var receive = await receiveHandle.Task;

        // Nobody enumerates yet. The receiver admits the first item and holds the second, so its read
        // loop parks with the rest of the data stream unread in its socket, and the third item
        // parks the sender on its window until it gives up.
        await send.SendAsync("item-1");
        await send.SendAsync("item-2");
        await Assert.ThrowsAsync<TransferFailedException>(
            () => send.SendAsync("item-3").WaitAsync(TimeSpan.FromSeconds(10)));

        // The Abort the sender wrote on giving up is in that socket, behind the item the reader never
        // took. Releasing the handle then cancels the sender's control read, which makes the hub cut
        // the receiver's control write, and the receiver restarts both of its streams.
        await send.DisposeAsync();

        var consumer = Task.Run(async () =>
        {
            await foreach (var _ in receive.ReceiveAsync()) { }
        });

        if (await Task.WhenAny(consumer, Task.Delay(TimeSpan.FromSeconds(5))) != consumer)
        {
            Assert.Fail($"The receiver was never told its sender gave up; it opened its data stream {opens.Opens} times.");
        }

        await Assert.ThrowsAsync<TransferFailedException>(() => consumer);
        Assert.True(opens.Opens < 10, $"The receiver opened its data stream {opens.Opens} times.");
    }

    [Fact]
    public async Task Abort_BehindMoreDataThanThePipeHolds_StillEndsTheReceiversTransfer()
    {
        await using var host = await TestHubServerFactory.StartLoopbackAsync();

        // Capped at 50 opens, as above: a receiver that reopens in a loop would exhaust the ports.
        var opens = new DataStreamOpenCounter(limit: 50);

        await using var sender = CreateClient(null, host.BaseUrl, o =>
        {
            o.MaxInFlightBatches = 4;
            o.CompleteStallTimeout = TimeSpan.FromSeconds(1);
        });
        await using var receiver = CreateClient(null, host.BaseUrl, o => o.ReceiveBufferCapacity = BufferCapacity.Items(1), networkHandler: opens);

        var receiveHandle = new TaskCompletionSource<Receive<string>>(TaskCreationOptions.RunContinuationsAsynchronously);
        receiver.OnTransferStarted += receive => { receiveHandle.TrySetResult(receive); return Task.CompletedTask; };

        await sender.ConnectAsync();
        await receiver.ConnectAsync();

        var send = await sender.InitTransferAsync(receiver.ClientId, batchSize: 1);
        await using var receive = await receiveHandle.Task;

        // Nobody enumerates. The receiver holds one item and stops reading; four batches of 4 MB are
        // then in flight, more than the sockets and the hub's buffers hold, so the Abort the sender
        // writes on giving up stays queued behind them and cannot reach the receiver or the hub.
        // Random, because the serializer compresses and a run of one character would take no room at all.
        var item = Convert.ToBase64String(System.Security.Cryptography.RandomNumberGenerator.GetBytes(3 * 1024 * 1024));
        var gaveUp = false;
        for (var i = 0; i < 100 && !gaveUp; i++)
        {
            try
            {
                await send.SendAsync(item).WaitAsync(TimeSpan.FromSeconds(30));
            }
            catch (TransferFailedException)
            {
                gaveUp = true;
            }
        }
        Assert.True(gaveUp, "The sender never gave up on its window.");

        await send.DisposeAsync();

        var verdict = receive.WaitForCompletionAsync();
        if (await Task.WhenAny(verdict, Task.Delay(TimeSpan.FromSeconds(8))) != verdict)
        {
            Assert.Fail($"The receiver was never told its sender gave up; it opened its data stream {opens.Opens} times.");
        }

        await Assert.ThrowsAsync<TransferFailedException>(() => verdict);
    }

    [Fact]
    public async Task HostTermination_MakesBothEndsFailFast_SendAsyncIncluded()
    {
        using var host = TestHubServerFactory.CreateHost();
        var handler = host.GetTestServer().CreateHandler();

        // A window of one batch and a receive buffer of one item that nobody drains: the receiver
        // admits the first item and acknowledges it, then holds the acknowledgement of the second
        // until there is room, so the window stays full for as long as the test needs it to.
        await using var sender = CreateClient(handler, "http://localhost", o => o.MaxInFlightBatches = 1);
        await using var receiver = CreateClient(handler, "http://localhost", o => o.ReceiveBufferCapacity = BufferCapacity.Items(1));

        var receiveHandle = new TaskCompletionSource<Receive<string>>(TaskCreationOptions.RunContinuationsAsynchronously);
        receiver.OnTransferStarted += receive => { receiveHandle.TrySetResult(receive); return Task.CompletedTask; };

        await sender.ConnectAsync();
        await receiver.ConnectAsync();

        await using var send = await sender.InitTransferAsync(receiver.ClientId, batchSize: 1);
        var receive = await receiveHandle.Task;

        // SendAsync returns once the item is queued, not once anything has crossed. The second call
        // only returns once the first item has been acknowledged, which takes every stream of the
        // transfer being up: terminating earlier catches streams still opening, and a stream
        // parked at the hub waiting for its peer is not released at once.
        await send.SendAsync("item-1");
        await send.SendAsync("item-2").WaitAsync(TimeSpan.FromSeconds(10));

        // Nothing can acknowledge item-2 now, so this call is parked on the window. It has to be
        // parked before the termination: a call made after it races the notification, and the
        // outcome then depends on which side of the release it lands.
        var parked = send.SendAsync("item-3");
        await Task.Delay(100);
        Assert.False(parked.IsCompleted, "item-3 should be parked on a full window.");

        var terminator = host.Services.GetRequiredService<ITransferTerminator>();
        await terminator.TerminateAsync(send.TransferId);

        var sendStopwatch = System.Diagnostics.Stopwatch.StartNew();
        await Assert.ThrowsAsync<TransferFailedException>(() => parked.WaitAsync(TimeSpan.FromSeconds(5)));
        sendStopwatch.Stop();
        Assert.True(sendStopwatch.Elapsed < TimeSpan.FromSeconds(1),
            $"Expected termination to fail SendAsync in under a second; took {sendStopwatch.Elapsed}.");

        // Consuming earlier would make room, release the acknowledgement and unpark the call above.
        var receiveStopwatch = System.Diagnostics.Stopwatch.StartNew();
        await Assert.ThrowsAsync<TransferFailedException>(async () =>
        {
            await foreach (var _ in receive.ReceiveAsync())
            {
            }
            await receive.WaitForCompletionAsync();
        }).WaitAsync(TimeSpan.FromSeconds(5));
        receiveStopwatch.Stop();
        Assert.True(receiveStopwatch.Elapsed < TimeSpan.FromSeconds(1),
            $"Expected termination to fail the receive in under a second; took {receiveStopwatch.Elapsed}.");
    }

    [Fact]
    public async Task NormalCompletion_NeverReleasesTheSessionBeforeItsOutcome()
    {
        using var host = TestHubServerFactory.CreateHost();
        var handler = host.GetTestServer().CreateHandler();

        await using var sender = CreateClient(handler, "http://localhost");
        await using var receiver = CreateClient(handler, "http://localhost");

        var receiveHandle = new TaskCompletionSource<Receive<string>>(TaskCreationOptions.RunContinuationsAsynchronously);
        receiver.OnTransferStarted += receive => { receiveHandle.TrySetResult(receive); return Task.CompletedTask; };

        await sender.ConnectAsync();
        await receiver.ConnectAsync();

        await using var send = await sender.InitTransferAsync(receiver.ClientId, batchSize: 10);
        await using var receive = await receiveHandle.Task;

        var consumer = Task.Run(async () =>
        {
            var items = new List<string>();
            await foreach (var item in receive.ReceiveAsync())
            {
                items.Add(item);
            }
            await receive.WaitForCompletionAsync();
            return items;
        });

        var expected = Enumerable.Range(0, 20).Select(i => $"item-{i}").ToList();
        await send.SendAsync(expected);
        await send.CompleteAsync().WaitAsync(TimeSpan.FromSeconds(30));

        // A transfer that ends normally reaches Completed before either handle is disposed, so
        // DisposeAsync's OnReleased() finds nothing left to abandon - no Abort is ever queued.
        Assert.Equal(expected, await consumer.WaitAsync(TimeSpan.FromSeconds(30)));
    }

    [Fact]
    public async Task HostInitiatedTransfer_IsRefused_ByASenderWithNoHandler()
    {
        using var host = TestHubServerFactory.CreateHost();
        var handler = host.GetTestServer().CreateHandler();

        await using var sender = CreateClient(handler, "http://localhost");
        await using var receiver = CreateClient(handler, "http://localhost");
        receiver.OnTransferStarted += _ => Task.CompletedTask;

        await sender.ConnectAsync();
        await receiver.ConnectAsync();

        var initiator = host.Services.GetRequiredService<ITransferInitiator>();
        var ex = await Assert.ThrowsAsync<HubException>(
            () => initiator.InitTransferAsync(sender.ClientId, receiver.ClientId, batchSize: 10, timeoutMs: 20_000));

        Assert.Contains("refused", ex.Message, StringComparison.OrdinalIgnoreCase);

        // The sender refused before creating anything; the receiver was readied first and is told
        // to cancel, which releases its session.
        Assert.Empty(sender.GetTransfers());
        await AssertEventuallyEmptyAsync(receiver);
    }

    [Fact]
    public async Task HostInitiatedTransfer_ReleasesTheSendersSession_WhenItsHandlerFails()
    {
        using var host = TestHubServerFactory.CreateHost();
        var handler = host.GetTestServer().CreateHandler();

        await using var sender = CreateClient(handler, "http://localhost");
        await using var receiver = CreateClient(handler, "http://localhost");
        sender.OnSendRequested += _ => throw new InvalidOperationException("no room for another transfer");
        receiver.OnTransferStarted += _ => Task.CompletedTask;

        await sender.ConnectAsync();
        await receiver.ConnectAsync();

        var initiator = host.Services.GetRequiredService<ITransferInitiator>();
        await Assert.ThrowsAsync<HubException>(
            () => initiator.InitTransferAsync(sender.ClientId, receiver.ClientId, batchSize: 10, timeoutMs: 20_000));

        Assert.Empty(sender.GetTransfers());
        await AssertEventuallyEmptyAsync(receiver);
    }

    private static async Task AssertEventuallyEmptyAsync(SignalRDataClient<string> client)
    {
        // CancelTransfer is sent without waiting for the client to act on it.
        var deadline = DateTime.UtcNow.AddSeconds(10);
        while (client.GetTransfers().Count > 0 && DateTime.UtcNow < deadline)
        {
            await Task.Delay(50);
        }

        Assert.Empty(client.GetTransfers());
    }

    [Fact]
    public async Task GetReceivers_ShouldReportDisconnection_WithoutHidingTheClient()
    {
        // 1. Start Server
        using var host = TestHubServerFactory.CreateHost();
        var server = host.GetTestServer();
        var handler = server.CreateHandler();

        await using var sender = CreateClient(handler, "http://localhost");
        var receiver = CreateClient(handler, "http://localhost");

        await sender.ConnectAsync();
        await receiver.ConnectAsync();
        await Task.Delay(500);

        // 2. While connected, the receiver carries no disconnection marker.
        var whileUp = await sender.GetReceiversAsync();
        var liveEntry = Assert.Single(whileUp, c => c.ClientId == receiver.ClientId);
        Assert.Null(liveEntry.DisconnectedAtUtc);

        // 3. Drop the receiver's control connection.
        var receiverId = receiver.ClientId;
        await receiver.DisposeAsync();
        await Task.Delay(500);

        // 4. The registration is still returned - deleting it would break a transfer that was
        //    about to resume - but it now says when the connection went away.
        var afterDrop = await sender.GetReceiversAsync();
        var staleEntry = Assert.Single(afterDrop, c => c.ClientId == receiverId);
        Assert.NotNull(staleEntry.DisconnectedAtUtc);
    }

    [Fact]
    public async Task InitTransfer_AgainstStaleRegistration_ShouldFailWithAnActionableMessage()
    {
        // 1. Start Server
        using var host = TestHubServerFactory.CreateHost();
        var server = host.GetTestServer();
        var handler = server.CreateHandler();

        await using var sender = CreateClient(handler, "http://localhost");
        var receiver = CreateClient(handler, "http://localhost");

        await sender.ConnectAsync();
        await receiver.ConnectAsync();
        await Task.Delay(500);

        var receiverId = receiver.ClientId;
        await receiver.DisposeAsync();
        await Task.Delay(500);

        // 2. The registration outlived the connection, so the handshake invokes a connection that
        //    no longer exists. SignalR always prefixes its generic sentence, but it appends a
        //    HubException's own message after it - so an untranslated IOException here leaves the
        //    caller with the prefix and nothing else.
        var ex = await Assert.ThrowsAsync<HubException>(
            () => sender.InitTransferAsync(receiverId, batchSize: 100));

        Assert.Contains("not reachable", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Heartbeat_ReportsWhetherTheHubStillKnowsTheConnection()
    {
        // 1. Start Server
        using var host = TestHubServerFactory.CreateHost();
        var server = host.GetTestServer();
        var handler = server.CreateHandler();

        // A raw connection: this exercises the hub method, not the client that drives it.
        await using var connection = new HubConnectionBuilder()
            .WithUrl("http://localhost" + WireRoutes.CommandHub, o => o.HttpMessageHandlerFactory = _ => handler)
            .Build();
        await connection.StartAsync();

        var clientId = Guid.NewGuid();
        await connection.InvokeAsync(nameof(ICommandHub.Connect), clientId, new[] { "senders" });

        // 2. Registered: the hub knows this connection.
        Assert.True(await connection.InvokeAsync<bool>(nameof(ICommandHub.Heartbeat)));

        // 3. Drop the registration the way an expiry would - the connection stays up.
        var store = host.Services.GetRequiredService<IStateStore>();
        await store.DeleteClientAsync(clientId);

        // 4. Connected but unknown. This is what discovery sees, and why the answer has to carry
        //    more than a keep-alive: the client must know to register again.
        Assert.False(await connection.InvokeAsync<bool>(nameof(ICommandHub.Heartbeat)));
    }

    [Fact]
    public async Task PresenceHeartbeat_ReRegistersAfterTheRegistrationIsLost()
    {
        // 1. Start Server. A short presence interval so the test does not wait a minute.
        using var host = TestHubServerFactory.CreateHost();
        var server = host.GetTestServer();
        var handler = server.CreateHandler();

        await using var sender = CreateClient(handler, "http://localhost");
        await using var receiver = CreateClient(
            handler, "http://localhost", o => o.PresenceHeartbeatInterval = TimeSpan.FromMilliseconds(300));

        await sender.ConnectAsync();
        await receiver.ConnectAsync();
        await Task.Delay(500);

        Assert.Contains(await sender.GetReceiversAsync(), c => c.ClientId == receiver.ClientId);

        // 2. Lose the receiver's registration while it stays connected - a hub restart, or an
        //    eviction past the grace period.
        var store = host.Services.GetRequiredService<IStateStore>();
        await store.DeleteClientAsync(receiver.ClientId);
        Assert.DoesNotContain(await sender.GetReceiversAsync(), c => c.ClientId == receiver.ClientId);

        // 3. The presence heartbeat notices and registers again, with no transfer in flight and
        //    nothing else prompting it.
        var deadline = DateTime.UtcNow.AddSeconds(10);
        List<ClientInfo> receivers;
        do
        {
            await Task.Delay(200);
            receivers = await sender.GetReceiversAsync();
        }
        while (!receivers.Any(c => c.ClientId == receiver.ClientId) && DateTime.UtcNow < deadline);

        Assert.Contains(receivers, c => c.ClientId == receiver.ClientId);
    }

    /// <param name="handler">The <c>TestServer</c> handler, or null to go over the network.</param>
    /// <param name="networkHandler">What carries the data streams over the network; the default socket handler if null.</param>
    private SignalRDataClient<string> CreateClient(
        HttpMessageHandler? handler, string url, Action<ClientOptions>? configure = null,
        HttpMessageHandler? networkHandler = null)
    {
        // Custom Connection Manager that uses TestServer's HttpMessageHandler
        var options = new ClientOptions();
        configure?.Invoke(options);
        var logger = NullLogger.Instance; // Can use XunitLogger if needed
        
        // We need to bypass the standard HubConnectionBuilder.WithUrl and inject the handler
        // But HubConnectionBuilderExtensions only supports standard HttpClient or options.
        // THE TRICK: Use WithUrl(url, options => options.HttpMessageHandlerFactory = _ => handler)
        
        var connectionManager = new HubConnectionManager(
            url + WireRoutes.CommandHub,
            options,
            logger,
            initialAccessToken: null,
            tokenProvider: null,
             configureHttpConnection: (httpOpts) => 
             {
                 if (handler != null)
                 {
                     httpOpts.HttpMessageHandlerFactory = _ => handler;
                 }
             }
        );
        
        var registry = new SessionRegistry<string>(logger);
        var serializer = new TransportR.Serialization.MessagePack.Services.MessagePackBatchSerializer<string>();
        var streamManager = new Microsoft.IO.RecyclableMemoryStreamManager();
        var httpClient = handler != null
            ? new HttpClient(handler) { BaseAddress = new Uri(url) }
            : new HttpClient(networkHandler ?? new SocketsHttpHandler()) { BaseAddress = new Uri(url), Timeout = Timeout.InfiniteTimeSpan };

        return new SignalRDataClient<string>(
            connectionManager,
            registry,
            serializer,
            logger,
            defaultBatchSize: 100,
            options,
            url + WireRoutes.StreamBase,
            url + WireRoutes.ClientBase,
            httpClient,
            streamManager
        );
    }

    /// <summary>
    /// Counts the data streams a client opens and, past <paramref name="limit"/>, parks any further
    /// one until it is cancelled.
    /// </summary>
    private sealed class DataStreamOpenCounter(int limit) : DelegatingHandler(new SocketsHttpHandler())
    {
        private int _opens;

        public int Opens => Volatile.Read(ref _opens);

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var isDataRead = request.Method == HttpMethod.Get
                && request.Headers.TryGetValues("X-Connection-Type", out var types)
                && types.Contains(ConnectionType.Data.ToString());

            if (isDataRead && Interlocked.Increment(ref _opens) > limit)
            {
                await Task.Delay(Timeout.Infinite, cancellationToken);
            }

            return await base.SendAsync(request, cancellationToken);
        }
    }
}
