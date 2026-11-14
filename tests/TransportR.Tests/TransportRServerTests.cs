using System.Net;
using System.Threading.Channels;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using TransportR.Hub;
using TransportR.Hub.Controllers;
using TransportR.Hub.Interfaces;
using TransportR.Hub.Models;
using TransportR.Hub.Services;
using TransportR.Hub.Streams;
using TransportR.Tests.Infrastructure;

namespace TransportR.Tests;

/// <summary>
/// The hub's stream handling with the streams replaced by fakes, so that what fails and when is the
/// test's to say. The real thing dies of a client vanishing behind a link that stayed silent, which
/// no in-process host can produce; these pin what the hub must do once it has noticed.
/// </summary>
public class TransportRServerTests
{
    private const string TransferId = "transfer-1";
    private static readonly TimeSpan Patience = TimeSpan.FromSeconds(5);

    private sealed class Harness
    {
        public BridgeManager Bridges { get; } = new(NullLoggerFactory.Instance);
        public RecordingRetirer Retirer { get; } = new();
        public LogRecorder Logs { get; } = new();
        public KnownTransfer Transfers { get; } = new();
        public TransportRServer Server { get; }

        public Harness()
        {
            Server = new TransportRServer(
                Logs.Factory.CreateLogger<TransportRServer>(),
                new IgnoredProgress(),
                Transfers,
                Bridges,
                Retirer);
        }

        public ConnectionBridge BridgeFor(ConnectionType type) =>
            Bridges.GetOrCreate(TransferId, type);
    }

    [Fact]
    public async Task SendEndingCleanly_OnTheDataStream_CompletesTheBridgeAndRetiresTheTransfer()
    {
        var hub = new Harness();
        var bridge = hub.BridgeFor(ConnectionType.Data);
        _ = bridge.RegisterReceiverAsync(new MemoryStream(), Guid.NewGuid());

        using var input = new PushedStream();
        var send = hub.Server.HandleSendAsync(TransferId, ConnectionType.Data, input, CancellationToken.None);
        await UntilAsync(() => bridge.State == BridgeState.Active);

        input.End();
        await EndsAsync(send);

        Assert.Equal(BridgeState.Completed, bridge.State);
        Assert.Equal([TransferId], hub.Retirer.Retired);
    }

    [Theory]
    [InlineData(ConnectionType.Data)]
    [InlineData(ConnectionType.Control)]
    public async Task BytesOnAStream_KeepTheTransferAlive(ConnectionType type)
    {
        // A heartbeat is a request of its own, which can fail while the streams carry on; the shared
        // session a replica takes the transfer over from is renewed here.
        var hub = new Harness();
        var bridge = hub.BridgeFor(type);
        _ = bridge.RegisterReceiverAsync(new MemoryStream(), Guid.NewGuid());

        using var input = new PushedStream();
        var send = hub.Server.HandleSendAsync(TransferId, type, input, CancellationToken.None);
        await UntilAsync(() => bridge.State == BridgeState.Active);

        input.Push(new byte[64]);
        await UntilAsync(() => hub.Transfers.KeepAlives > 0);

        input.End();
        await EndsAsync(send);
    }

    [Fact]
    public async Task SendEndingCleanly_OnTheControlStream_RetiresNothing()
    {
        // The receiver ends its control POST right after the acknowledgement, before the sender is
        // known to have read it: only the sender's data stream says the transfer is over.
        var hub = new Harness();
        var bridge = hub.BridgeFor(ConnectionType.Control);
        _ = bridge.RegisterReceiverAsync(new MemoryStream(), Guid.NewGuid());

        using var input = new PushedStream();
        var send = hub.Server.HandleSendAsync(TransferId, ConnectionType.Control, input, CancellationToken.None);
        await UntilAsync(() => bridge.State == BridgeState.Active);

        input.End();
        await EndsAsync(send);

        Assert.Empty(hub.Retirer.Retired);
        Assert.NotEqual(BridgeState.Completed, bridge.State);
    }

    [Fact]
    public async Task SendCutByARupture_RetiresNothing()
    {
        // A connection that fails mid-stream is a transfer that may resume, not one that ended.
        var hub = new Harness();
        var bridge = hub.BridgeFor(ConnectionType.Data);
        _ = bridge.RegisterReceiverAsync(new MemoryStream(), Guid.NewGuid());

        using var input = new PushedStream();
        var send = hub.Server.HandleSendAsync(TransferId, ConnectionType.Data, input, CancellationToken.None);
        await UntilAsync(() => bridge.State == BridgeState.Active);

        input.Cut(new IOException("connection reset"));
        await EndsAsync(send);

        Assert.Empty(hub.Retirer.Retired);
        Assert.NotEqual(BridgeState.Completed, bridge.State);
    }

    [Fact]
    public async Task SendCutByARupture_OnTheDataStream_CutsTheReceiverItFed()
    {
        // The rupture can fall inside a chunk. Bytes of the next stream written behind it would be read
        // as the rest of that chunk, so the receiver has to start again on a stream of its own.
        var hub = new Harness();
        var fed = new MemoryStream();
        var receive = hub.Server.HandleReceiveAsync(TransferId, ConnectionType.Data, fed, CancellationToken.None);

        var partial = Enumerable.Range(0, 100).Select(i => (byte)i).ToArray();
        using var first = new PushedStream();
        var send = hub.Server.HandleSendAsync(TransferId, ConnectionType.Data, first, CancellationToken.None);
        first.Push(partial);
        await UntilAsync(() => fed.Length == partial.Length);
        first.Cut(new IOException("connection reset"));
        await EndsAsync(send);

        var ping = Enumerable.Repeat((byte)0xEE, 30).ToArray();
        using var second = new PushedStream();
        _ = hub.Server.HandleSendAsync(TransferId, ConnectionType.Data, second, CancellationToken.None);
        second.Push(ping);
        await EndsAsync(receive);

        var next = new MemoryStream();
        _ = hub.Server.HandleReceiveAsync(TransferId, ConnectionType.Data, next, CancellationToken.None);
        await UntilAsync(() => next.Length == ping.Length);

        Assert.Equal(partial, fed.ToArray());
        Assert.Equal(ping, next.ToArray());
        Assert.Empty(hub.Retirer.Retired);
    }

    [Fact]
    public async Task ANewEpochOfTheDataStream_CutsTheReceiverTheEpochItReplacesFed()
    {
        var hub = new Harness();
        var epochs = new UploadEpochs(TimeProvider.System, NullLogger<UploadEpochs>.Instance);
        var bridgeKey = IBridgeManager.KeyFor(TransferId, ConnectionType.Data);
        Func<Stream, CancellationToken, Task> handle = (input, token) =>
            hub.Server.HandleSendAsync(TransferId, ConnectionType.Data, input, token);
        var fed = new MemoryStream();
        var receive = hub.Server.HandleReceiveAsync(TransferId, ConnectionType.Data, fed, CancellationToken.None);

        var partial = Enumerable.Range(0, 100).Select(i => (byte)i).ToArray();
        var first = epochs.Open(bridgeKey, Guid.NewGuid(), null, null, handle);
        var relayed = await first.AcceptAsync(0, new MemoryStream(partial), final: false, CancellationToken.None).WaitAsync(Patience);
        Assert.Equal(SegmentStatus.Relayed, relayed.Status);

        // The client gave up on the first epoch without the hub seeing it fail: the next one replaces it.
        var ping = Enumerable.Repeat((byte)0xEE, 30).ToArray();
        var second = epochs.Open(bridgeKey, Guid.NewGuid(), null, null, handle);
        var pinged = second.AcceptAsync(0, new MemoryStream(ping), final: false, CancellationToken.None);
        await EndsAsync(receive);

        var next = new MemoryStream();
        _ = hub.Server.HandleReceiveAsync(TransferId, ConnectionType.Data, next, CancellationToken.None);
        Assert.Equal(SegmentStatus.Relayed, (await pinged.WaitAsync(Patience)).Status);

        Assert.Equal(partial, fed.ToArray());
        Assert.Equal(ping, next.ToArray());
        Assert.Empty(hub.Retirer.Retired);
        second.Abandon();
    }

    [Fact]
    public async Task TheLastSegmentOfTheDataStream_EndsTheReceiverCleanly()
    {
        // The receiver's stream carries the end of the transfer: cut instead of closed, it can lose it.
        var hub = new Harness();
        var epochs = new UploadEpochs(TimeProvider.System, NullLogger<UploadEpochs>.Instance);
        var fed = new MemoryStream();
        var receive = hub.Server.HandleReceiveAsync(TransferId, ConnectionType.Data, fed, CancellationToken.None);

        var bytes = Enumerable.Range(0, 100).Select(i => (byte)i).ToArray();
        var epoch = epochs.Open(IBridgeManager.KeyFor(TransferId, ConnectionType.Data), Guid.NewGuid(), null, null,
            (input, token) => hub.Server.HandleSendAsync(TransferId, ConnectionType.Data, input, token));
        await epoch.AcceptAsync(0, new MemoryStream(bytes[..60]), final: false, CancellationToken.None).WaitAsync(Patience);
        await epoch.AcceptAsync(60, new MemoryStream(bytes[60..]), final: true, CancellationToken.None).WaitAsync(Patience);
        await EndsAsync(receive);

        Assert.Equal(bytes, fed.ToArray());
        Assert.Equal(BridgeState.Completed, hub.BridgeFor(ConnectionType.Data).State);
        Assert.True(hub.Logs.Has(LogLevel.Information, "Sink Closing cleanly"), "The receiver's stream did not end with the transfer.");
        Assert.False(hub.Logs.Has(LogLevel.Information, "Sink cancelled"), "The receiver's stream was cut at a clean end.");
    }

    [Fact]
    public async Task SendCutByARupture_OfATransferTheHubStillCarries_IsAWarning()
    {
        // The warning means a client that should reconnect, and for a live transfer that is what it
        // is. Whatever is done about the transfers the hub ended itself must leave this one alone.
        var hub = new Harness();
        var bridge = hub.BridgeFor(ConnectionType.Data);
        _ = bridge.RegisterReceiverAsync(new MemoryStream(), Guid.NewGuid());

        using var input = new PushedStream();
        var send = hub.Server.HandleSendAsync(TransferId, ConnectionType.Data, input, CancellationToken.None);
        await UntilAsync(() => bridge.State == BridgeState.Active);

        input.Cut(new IOException("connection reset"));
        await EndsAsync(send);

        Assert.True(hub.Logs.Has(LogLevel.Warning, "RUPTURE"), "A rupture of a live transfer was not logged as a warning.");
    }

    [Fact]
    public async Task SendCutByARupture_OfATransferTheHubForgot_IsNotAWarning()
    {
        // The hub removes a transfer it ends before it tells anyone, so a stream that breaks after that
        // is the transfer's end, and nobody is going to reconnect.
        var hub = new Harness();
        var bridge = hub.BridgeFor(ConnectionType.Data);
        _ = bridge.RegisterReceiverAsync(new MemoryStream(), Guid.NewGuid());

        using var input = new PushedStream();
        var send = hub.Server.HandleSendAsync(TransferId, ConnectionType.Data, input, CancellationToken.None);
        await UntilAsync(() => bridge.State == BridgeState.Active);

        hub.Transfers.Forget();
        input.Cut(new IOException("connection reset"));
        await EndsAsync(send);

        Assert.False(hub.Logs.Has(LogLevel.Warning, "RUPTURE"), "A rupture of a transfer the hub had forgotten was logged as a warning.");
        Assert.True(hub.Logs.Has(LogLevel.Information, "Sender stream ended with RUPTURE"), "The end of the stream was not logged at all.");
    }

    [Fact]
    public async Task SourceLosingItsDestination_OfATransferTheHubForgot_IsNotAWarning()
    {
        var hub = new Harness();
        var bridge = hub.BridgeFor(ConnectionType.Data);
        _ = bridge.RegisterReceiverAsync(new FailingStream(new IOException("destination reset")), Guid.NewGuid());

        using var input = new PushedStream();
        var send = hub.Server.HandleSendAsync(TransferId, ConnectionType.Data, input, CancellationToken.None);
        await UntilAsync(() => bridge.State == BridgeState.Active);

        hub.Transfers.Forget();
        input.Push(new byte[] { 1, 2, 3 });
        await AssertEndsAbnormallyAsync(send);

        Assert.False(hub.Logs.Has(LogLevel.Warning, "Source Disconnected"), "A destination lost on a transfer the hub had forgotten was logged as a warning.");
        Assert.True(hub.Logs.Has(LogLevel.Information, "Source Disconnected"), "The end of the stream was not logged at all.");
    }

    [Fact]
    public async Task SourceLosingItsDestination_OfATransferTheHubStillCarries_IsAWarning()
    {
        var hub = new Harness();
        var bridge = hub.BridgeFor(ConnectionType.Data);
        _ = bridge.RegisterReceiverAsync(new FailingStream(new IOException("destination reset")), Guid.NewGuid());

        using var input = new PushedStream();
        var send = hub.Server.HandleSendAsync(TransferId, ConnectionType.Data, input, CancellationToken.None);
        await UntilAsync(() => bridge.State == BridgeState.Active);

        input.Push(new byte[] { 1, 2, 3 });
        await AssertEndsAbnormallyAsync(send);

        Assert.True(hub.Logs.Has(LogLevel.Warning, "Source Disconnected"));
    }

    [Fact]
    public async Task SendCancelled_RetiresNothing()
    {
        var hub = new Harness();
        var bridge = hub.BridgeFor(ConnectionType.Data);
        _ = bridge.RegisterReceiverAsync(new MemoryStream(), Guid.NewGuid());

        using var input = new PushedStream();
        using var request = new CancellationTokenSource();
        var send = hub.Server.HandleSendAsync(TransferId, ConnectionType.Data, input, request.Token);
        await UntilAsync(() => bridge.State == BridgeState.Active);

        request.Cancel();
        await EndsAsync(send);

        Assert.Empty(hub.Retirer.Retired);
    }

    [Fact]
    public async Task ReceiveCancelled_ReleasesTheSinkRegistration()
    {
        var hub = new Harness();
        var bridge = hub.BridgeFor(ConnectionType.Control);
        _ = bridge.RegisterSenderAsync(Stream.Null, Guid.NewGuid());

        using var request = new CancellationTokenSource();
        var receive = hub.Server.HandleReceiveAsync(TransferId, ConnectionType.Control, new MemoryStream(), request.Token);
        await UntilAsync(() => bridge.State == BridgeState.Active);

        // The receiver's client is gone: its request is aborted. Whatever the wait does with that,
        // the registration it left behind has to go, or the bridge stays Active for a stream nobody
        // is on, is never judged idle, and a source keeps writing into a response that is dead.
        request.Cancel();
        await EndsAsync(receive);

        Assert.Equal(BridgeState.WaitingForReceiver, bridge.State);
    }

    [Fact]
    public async Task ReceiveCancelledAfterBeingReplaced_LeavesTheNewSink()
    {
        var hub = new Harness();
        var bridge = hub.BridgeFor(ConnectionType.Control);
        _ = bridge.RegisterSenderAsync(Stream.Null, Guid.NewGuid());

        using var firstRequest = new CancellationTokenSource();
        var first = hub.Server.HandleReceiveAsync(TransferId, ConnectionType.Control, new MemoryStream(), firstRequest.Token);
        await UntilAsync(() => bridge.State == BridgeState.Active);

        // The reconnected client takes the first one's place. That resets the first registration
        // and ends its wait; whatever it does on the way out has to be about its own registration,
        // not the bridge's current one.
        using var secondRequest = new CancellationTokenSource();
        var second = hub.Server.HandleReceiveAsync(TransferId, ConnectionType.Control, new MemoryStream(), secondRequest.Token);
        await EndsAsync(first);
        await Task.Delay(100);

        Assert.Equal(BridgeState.Active, bridge.State);
        Assert.False(second.IsCompleted);
    }

    [Fact]
    public async Task SourceParkedOnItsInput_IsCutWhenItsDestinationIsLost()
    {
        var hub = new Harness();
        var bridge = hub.BridgeFor(ConnectionType.Data);
        var sinkId = Guid.NewGuid();
        _ = bridge.RegisterReceiverAsync(new MemoryStream(), sinkId);

        using var input = new PushedStream();
        var send = hub.Server.HandleSendAsync(TransferId, ConnectionType.Data, input, CancellationToken.None);
        await UntilAsync(() => bridge.State == BridgeState.Active);

        // A live destination costs the source nothing.
        await Task.Delay(100);
        Assert.False(send.IsCompleted);

        // Nothing is being written, so the source cannot find out by failing a write. It has to be
        // told, or it sits on a connection whose far end went away and its client, which is waiting
        // for an acknowledgement that cannot come, waits until its own window gives up.
        bridge.OnDisconnected(sinkId);

        await AssertEndsAbnormallyAsync(send);
    }

    [Fact]
    public async Task SendCancelled_ReleasesTheSourceRegistration()
    {
        var hub = new Harness();
        var bridge = hub.BridgeFor(ConnectionType.Data);
        _ = bridge.RegisterReceiverAsync(new MemoryStream(), Guid.NewGuid());

        using var input = new PushedStream();
        using var request = new CancellationTokenSource();
        var send = hub.Server.HandleSendAsync(TransferId, ConnectionType.Data, input, request.Token);
        await UntilAsync(() => bridge.State == BridgeState.Active);

        // The source's client is gone. Its request ending is an ordinary end for the controller, but
        // the registration it left has to go: kept, the bridge stays Active for a stream nobody is
        // on, is never judged idle, and the receiver waits on it for good. The receiver it was writing
        // to goes with it.
        request.Cancel();
        await EndsAsync(send);

        Assert.Equal(BridgeState.Idle, bridge.State);
    }

    [Fact]
    public async Task SendCancelledWhileWaitingForADestination_EndsAndReleasesTheSource()
    {
        var hub = new Harness();
        var bridge = hub.BridgeFor(ConnectionType.Data);

        using var input = new PushedStream();
        using var request = new CancellationTokenSource();
        var send = hub.Server.HandleSendAsync(TransferId, ConnectionType.Data, input, request.Token);
        await UntilAsync(() => bridge.State == BridgeState.WaitingForReceiver);

        // The source's client went away before any destination came. A wait deaf to that holds the
        // request until the bridge is cancelled, which for a bridge nobody completes is the idle
        // timeout, and an in-memory host keeps the client's own cancelled request open as long.
        request.Cancel();
        await EndsAsync(send);

        Assert.Equal(BridgeState.Idle, bridge.State);
    }

    [Fact]
    public async Task SendCancelledAfterItsDestinationWasLostOnArrival_EndsAndReleasesTheSource()
    {
        // The destination registers, which ends the source's wait, and goes before the source's
        // handler has resumed to look for it: the source is found a destination that is no longer
        // there and waits for the next one. The window is a few instructions wide, so this repeats
        // until the handler lands in it; a few runs in a hundred do.
        const int attempts = 1000;
        var parked = 0;

        for (var i = 0; i < attempts; i++)
        {
            var hub = new Harness();
            var bridge = hub.BridgeFor(ConnectionType.Data);

            using var input = new PushedStream();
            using var request = new CancellationTokenSource();
            var send = hub.Server.HandleSendAsync(TransferId, ConnectionType.Data, input, request.Token);
            Assert.Equal(BridgeState.WaitingForReceiver, bridge.State);

            var destinationId = Guid.NewGuid();
            _ = bridge.RegisterReceiverAsync(new MemoryStream(), destinationId);
            bridge.OnDisconnected(destinationId);

            request.Cancel();
            if (await Task.WhenAny(send, Task.Delay(TimeSpan.FromMilliseconds(100))) != send)
            {
                parked++;
            }
            else
            {
                Assert.Equal(BridgeState.Idle, bridge.State);
            }
        }

        Assert.True(parked == 0, $"{parked} of {attempts} sources stayed parked on a destination after their request was cancelled.");
    }

    [Fact]
    public async Task ReceiveCancelledWhileWaitingForASource_EndsAndReleasesTheSink()
    {
        var hub = new Harness();
        var bridge = hub.BridgeFor(ConnectionType.Control);

        using var request = new CancellationTokenSource();
        var receive = hub.Server.HandleReceiveAsync(TransferId, ConnectionType.Control, new MemoryStream(), request.Token);
        await UntilAsync(() => bridge.State == BridgeState.WaitingForSender);

        request.Cancel();
        await EndsAsync(receive);

        Assert.Equal(BridgeState.Idle, bridge.State);
    }

    [Fact]
    public async Task SendCancelledAfterBeingReplaced_LeavesTheNewSource()
    {
        var hub = new Harness();
        var bridge = hub.BridgeFor(ConnectionType.Data);
        _ = bridge.RegisterReceiverAsync(new MemoryStream(), Guid.NewGuid());

        using var firstInput = new PushedStream();
        var first = hub.Server.HandleSendAsync(TransferId, ConnectionType.Data, firstInput, CancellationToken.None);
        await UntilAsync(() => bridge.State == BridgeState.Active);

        // The reconnected client takes the first source's place. That ends the first one's wait;
        // whatever it does on the way out has to be about its own registration, not the bridge's
        // current one. The receiver the first one was writing to is cut with it, so the new source
        // waits for the next.
        using var secondInput = new PushedStream();
        var second = hub.Server.HandleSendAsync(TransferId, ConnectionType.Data, secondInput, CancellationToken.None);
        await EndsAsync(first);
        await Task.Delay(100);

        Assert.Equal(BridgeState.WaitingForReceiver, bridge.State);
        Assert.False(second.IsCompleted);
    }

    [Fact]
    public Task SourceWritingToAResetDestination_EndsAbnormally() =>
        AssertSourceEndsWhenItsDestinationFailsAsync(new IOException("destination reset"));

    [Fact]
    public Task SourceWritingToACompletedResponse_EndsAbnormally() =>
        AssertSourceEndsWhenItsDestinationFailsAsync(
            new ObjectDisposedException("HttpResponse", "Cannot write to the response body, the response has completed."));

    private static async Task AssertSourceEndsWhenItsDestinationFailsAsync(Exception failure)
    {
        var hub = new Harness();
        var bridge = hub.BridgeFor(ConnectionType.Data);
        var destination = new FailingStream(failure);
        _ = bridge.RegisterReceiverAsync(destination, Guid.NewGuid());

        using var input = new PushedStream();
        var send = hub.Server.HandleSendAsync(TransferId, ConnectionType.Data, input, CancellationToken.None);
        await UntilAsync(() => bridge.State == BridgeState.Active);

        // The peer that is healthy is not the one to answer for this. A source that returns as if
        // its stream had ended answers 200, and its client reads that as an ordinary end of stream
        // rather than as a connection to reopen.
        input.Push(new byte[] { 1, 2, 3 });

        await AssertEndsAbnormallyAsync(send);
    }

    [Fact]
    public async Task Receive_OnADataStreamTheSourceAlreadyFinished_AnswersGone()
    {
        var hub = new Harness();
        hub.BridgeFor(ConnectionType.Data).Complete();
        var server = new RecordingServer();

        var http = await ReceiveAsync(hub, server, ConnectionType.Data);

        // A 200 with nothing in it is what a stream that broke looks like, and its client reopens
        // it. This one is over for good, and says so in the one way a client already reads as final.
        Assert.Equal(StatusCodes.Status410Gone, http.Response.StatusCode);
        Assert.False(server.Reached);
    }

    [Fact]
    public async Task Receive_OnADataStreamStillOpen_IsServed()
    {
        var hub = new Harness();
        var server = new RecordingServer();

        var http = await ReceiveAsync(hub, server, ConnectionType.Data);

        Assert.Equal(StatusCodes.Status200OK, http.Response.StatusCode);
        Assert.True(server.Reached);
    }

    [Fact]
    public async Task Receive_OnAControlStream_IsServedWhateverBecameOfTheDataStream()
    {
        var hub = new Harness();
        hub.BridgeFor(ConnectionType.Data).Complete();
        var server = new RecordingServer();

        var http = await ReceiveAsync(hub, server, ConnectionType.Control);

        Assert.Equal(StatusCodes.Status200OK, http.Response.StatusCode);
        Assert.True(server.Reached);
    }

    [Fact]
    public async Task Receive_OverASocket_SendsItsHeadersBeforeThereIsAnythingToRelay()
    {
        // In memory, a response counts as started on StartAsync; Kestrel sends nothing until a write.
        await using var host = await TestHubServerFactory.StartLoopbackAsync();
        await host.Services.GetRequiredService<ITransferManager>().RegisterParticipantsAsync(TransferId, Guid.NewGuid(), Guid.NewGuid());

        using var client = new HttpClient(new SocketsHttpHandler()) { Timeout = Timeout.InfiniteTimeSpan };
        using var request = new HttpRequestMessage(HttpMethod.Get, $"{host.BaseUrl}{WireRoutes.StreamBase}/{TransferId}/receive")
        {
            Version = HttpVersion.Version11,
            VersionPolicy = HttpVersionPolicy.RequestVersionExact,
        };
        request.Headers.Add("X-Connection-Type", nameof(ConnectionType.Data));

        using var response = await client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead).WaitAsync(Patience);

        Assert.Equal(System.Net.HttpStatusCode.OK, response.StatusCode);
    }

    private static async Task<DefaultHttpContext> ReceiveAsync(Harness hub, ITransportRServer server, ConnectionType type)
    {
        var http = new DefaultHttpContext();
        http.Request.Headers["X-Connection-Type"] = type.ToString();

        // The identity provider, the state store and the policy are only read once a caller is
        // authenticated, and this one is not: dev mode, where the data plane is open.
        var controller = new StreamController(
            server,
            new KnownTransfer(),
            stateStore: null!,
            identityProvider: null!,
            hub.Bridges,
            policy: null!,
            terminator: null!,
            new UploadEpochs(TimeProvider.System, NullLogger<UploadEpochs>.Instance),
            Options.Create(new DataPlaneConfiguration()),
            TimeProvider.System,
            NullLogger<StreamController>.Instance)
        {
            ControllerContext = new ControllerContext { HttpContext = http }
        };

        await controller.Receive(TransferId);
        return http;
    }

    [Fact]
    public async Task Send_TheLastSegmentOfAStreamThatEndsShortOfIt_IsAnsweredWithAConflict()
    {
        var http = new DefaultHttpContext();
        http.Request.Headers["X-Connection-Type"] = nameof(ConnectionType.Data);

        // The identity provider, the state store and the policy are only read once a caller is
        // authenticated, and this one is not: dev mode, where the data plane is open.
        var controller = new StreamController(
            new FailingServer(new IOException("destination gone")),
            new KnownTransfer(),
            stateStore: null!,
            identityProvider: null!,
            new BridgeManager(NullLoggerFactory.Instance),
            policy: null!,
            terminator: null!,
            new UploadEpochs(TimeProvider.System, NullLogger<UploadEpochs>.Instance),
            Options.Create(new DataPlaneConfiguration()),
            TimeProvider.System,
            NullLogger<StreamController>.Instance)
        {
            ControllerContext = new ControllerContext { HttpContext = http }
        };

        await controller.Send(TransferId, Guid.NewGuid(), offset: 0, final: true, heartbeatMs: 15_000).WaitAsync(Patience);

        // A clean answer would tell the client its stream ended; it has to open a new one instead.
        Assert.Equal(StatusCodes.Status409Conflict, http.Response.StatusCode);
    }

    private static async Task UntilAsync(Func<bool> condition)
    {
        var deadline = DateTime.UtcNow + Patience;
        while (!condition())
        {
            Assert.True(DateTime.UtcNow < deadline, "The condition was not reached in time.");
            await Task.Delay(10);
        }
    }

    private static async Task EndsAsync(Task task)
    {
        var finished = await Task.WhenAny(task, Task.Delay(Patience));
        Assert.Same(task, finished);
        try { await task; } catch (OperationCanceledException) { }
    }

    private static async Task AssertEndsAbnormallyAsync(Task task)
    {
        var finished = await Task.WhenAny(task, Task.Delay(Patience));
        Assert.True(ReferenceEquals(task, finished), "The source is still connected; nobody told it its destination is gone.");

        // Not a cancellation: the controller answers those as a clean end and leaves the
        // connection up, which is the thing being ruled out.
        var failure = await Assert.ThrowsAnyAsync<Exception>(() => task);
        Assert.False(failure is OperationCanceledException, "A cancellation reaches the client as a normal end of stream.");
    }

    private sealed class IgnoredProgress : IHubProgressMonitor
    {
        public void OnBytesTransferred(long bytes) { }
    }

    private sealed class RecordingRetirer : ITransferRetirer
    {
        public List<string> Retired { get; } = [];

        public Task RetireAsync(string transferId)
        {
            Retired.Add(transferId);
            return Task.CompletedTask;
        }
    }

    private sealed class KnownTransfer : ITransferManager
    {
        private volatile bool _forgotten;

        public void Forget() => _forgotten = true;

        public Task<TransferParticipants?> GetParticipantsAsync(string transferId) =>
            Task.FromResult<TransferParticipants?>(_forgotten ? null : new TransferParticipants(Guid.NewGuid(), Guid.NewGuid()));

        public Task<TransferState?> TrackAsync(string transferId) =>
            Task.FromResult<TransferState?>(_forgotten ? null : new TransferState(transferId));

        public Task RegisterParticipantsAsync(string transferId, Guid senderId, Guid receiverId) => throw new NotSupportedException();
        public Task UpdateSenderStatsAsync(string transferId, long itemsSent, long acksReceived) => throw new NotSupportedException();
        public Task UpdateReceiverStatsAsync(string transferId, long itemsReceived, long acksSent) => throw new NotSupportedException();
        public TransferState? GetTransfer(string transferId) => throw new NotSupportedException();
        public Task RemoveTransferAsync(string transferId) => throw new NotSupportedException();
        public IEnumerable<TransferState> GetAllTransfers() => throw new NotSupportedException();
        public IReadOnlyDictionary<string, TransferParticipants> GetAllParticipants() => throw new NotSupportedException();
        private int _keepAlives;

        public int KeepAlives => Volatile.Read(ref _keepAlives);

        public Task KeepAliveAsync(string transferId)
        {
            Interlocked.Increment(ref _keepAlives);
            return Task.CompletedTask;
        }
    }

    private sealed class FailingServer(Exception failure) : ITransportRServer
    {
        public Task HandleSendAsync(string transferId, ConnectionType type, Stream input, CancellationToken cancellationToken = default) =>
            Task.FromException(failure);

        public Task HandleReceiveAsync(string transferId, ConnectionType type, Stream output, CancellationToken cancellationToken = default) =>
            Task.FromException(failure);
    }

    private sealed class RecordingServer : ITransportRServer
    {
        public bool Reached { get; private set; }

        public Task HandleSendAsync(string transferId, ConnectionType type, Stream input, CancellationToken cancellationToken = default)
        {
            Reached = true;
            return Task.CompletedTask;
        }

        public Task HandleReceiveAsync(string transferId, ConnectionType type, Stream output, CancellationToken cancellationToken = default)
        {
            Reached = true;
            return Task.CompletedTask;
        }
    }

    /// <summary>
    /// A request body that holds until the test hands it bytes, the way a client that has nothing to
    /// send holds an HTTP request open.
    /// </summary>
    private sealed class PushedStream : Stream
    {
        private readonly Channel<byte[]> _chunks = Channel.CreateUnbounded<byte[]>();
        private byte[] _current = [];
        private int _offset;

        public void Push(byte[] chunk) => _chunks.Writer.TryWrite(chunk);

        /// <summary>The client closes its request body: a clean end of the stream.</summary>
        public void End() => _chunks.Writer.TryComplete();

        /// <summary>The client's connection fails: the read throws, which the hub calls a rupture.</summary>
        public void Cut(Exception failure) => _chunks.Writer.TryComplete(failure);

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            while (_offset >= _current.Length)
            {
                if (!await _chunks.Reader.WaitToReadAsync(cancellationToken)) return 0;
                _current = await _chunks.Reader.ReadAsync(cancellationToken);
                _offset = 0;
            }

            var count = Math.Min(buffer.Length, _current.Length - _offset);
            _current.AsMemory(_offset, count).CopyTo(buffer);
            _offset += count;
            return count;
        }

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
            ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();

        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
        public override void Flush() { }
        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }

    /// <summary>
    /// A response that is already over, which is what a write finds after the client's request was
    /// aborted: Kestrel answers with an exception, not a quiet no-op.
    /// </summary>
    private sealed class FailingStream(Exception failure) : Stream
    {
        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default) =>
            ValueTask.FromException(failure);

        public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
            Task.FromException(failure);

        public override Task FlushAsync(CancellationToken cancellationToken) => Task.FromException(failure);

        public override bool CanRead => false;
        public override bool CanSeek => false;
        public override bool CanWrite => true;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
        public override void Flush() => throw failure;
        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw failure;
    }
}
