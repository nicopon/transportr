using System.Threading.Channels;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using TransportR.Hub.SignalR.Controllers;
using TransportR.Hub.SignalR.Hubs;
using TransportR.Hub.SignalR.Services;
using TransportR.Interfaces;
using TransportR.Models;
using TransportR.Services;
using TransportR.Tests.Infrastructure;

namespace TransportR.Tests;

/// <summary>
/// The hub's stream handling with the streams replaced by fakes, so that what fails and when is the
/// test's to say. The real thing dies of a client vanishing behind a link that stayed silent, which
/// no in-process host can produce; these pin what the hub must do once it has noticed.
/// </summary>
public class SignalRDataServerTests
{
    private const string TransferId = "transfer-1";
    private static readonly TimeSpan Patience = TimeSpan.FromSeconds(5);

    private sealed class Harness
    {
        public BridgeManager Bridges { get; } = new(NullLoggerFactory.Instance);
        public RecordingTerminator Terminator { get; } = new();
        public LogRecorder Logs { get; } = new();
        public KnownTransfer Transfers { get; } = new();
        public SignalRDataServer Server { get; }

        public Harness()
        {
            Server = new SignalRDataServer(
                Logs.Factory.CreateLogger<SignalRDataServer>(),
                new IgnoredProgress(),
                new UnusedHubContext(),
                Transfers,
                Bridges,
                Terminator);
        }

        public ConnectionBridge BridgeFor(ConnectionType type) =>
            Bridges.GetOrCreate(IBridgeManager.KeyFor(TransferId, type));
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
        Assert.Equal([TransferId], hub.Terminator.Retired);
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

        Assert.Empty(hub.Terminator.Retired);
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

        Assert.Empty(hub.Terminator.Retired);
        Assert.NotEqual(BridgeState.Completed, bridge.State);
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
        Assert.True(hub.Logs.Has(LogLevel.Information, "Sender POST ended with RUPTURE"), "The end of the stream was not logged at all.");
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

        Assert.Empty(hub.Terminator.Retired);
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
        // on, is never judged idle, and the receiver waits on it for good.
        request.Cancel();
        await EndsAsync(send);

        Assert.Equal(BridgeState.WaitingForSender, bridge.State);
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
        // current one.
        using var secondInput = new PushedStream();
        var second = hub.Server.HandleSendAsync(TransferId, ConnectionType.Data, secondInput, CancellationToken.None);
        await EndsAsync(first);
        await Task.Delay(100);

        Assert.Equal(BridgeState.Active, bridge.State);
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
            NullLogger<StreamController>.Instance)
        {
            ControllerContext = new ControllerContext { HttpContext = http }
        };

        await controller.Receive(TransferId);
        return http;
    }

    [Fact]
    public async Task Send_WhenTheServerFailsAbnormally_AbortsTheConnectionInsteadOfAnswering()
    {
        var lifetime = new RecordingLifetime();
        var http = new DefaultHttpContext();
        http.Request.Headers["X-Connection-Type"] = nameof(ConnectionType.Data);
        http.Features.Set<IHttpRequestLifetimeFeature>(lifetime);

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
            NullLogger<StreamController>.Instance)
        {
            ControllerContext = new ControllerContext { HttpContext = http }
        };

        await controller.Send(TransferId);

        // A status line written while the client is still streaming its body is not a signal the
        // client acts on; a connection that closes is.
        Assert.True(lifetime.Aborted);
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
        public void InitializeTransfer(string transferId) { }
        public void OnBytesTransferred(long bytes) { }
        public void CompleteTransfer(string transferId) { }
    }

    private sealed class RecordingTerminator : ITransferTerminator
    {
        public List<string> Retired { get; } = [];

        public Task TerminateAsync(string transferId, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public Task RetireAsync(string transferId)
        {
            Retired.Add(transferId);
            return Task.CompletedTask;
        }
    }

    private sealed class UnusedHubContext : IHubContext<CommandHub>
    {
        public IHubClients Clients => throw new NotSupportedException();
        public IGroupManager Groups => throw new NotSupportedException();
    }

    private sealed class KnownTransfer : ITransferManager
    {
        private volatile bool _forgotten;

        public void Forget() => _forgotten = true;

        public Task<TransferParticipants?> GetParticipantsAsync(string transferId) =>
            Task.FromResult<TransferParticipants?>(_forgotten ? null : new TransferParticipants(Guid.NewGuid(), Guid.NewGuid()));

        public Task RegisterParticipantsAsync(string transferId, Guid senderId, Guid receiverId) => throw new NotSupportedException();
        public Task UpdateSenderStatsAsync(string transferId, long itemsSent, long acksReceived) => throw new NotSupportedException();
        public Task UpdateReceiverStatsAsync(string transferId, long itemsReceived, long acksSent) => throw new NotSupportedException();
        public TransferState? GetTransfer(string transferId) => throw new NotSupportedException();
        public Task RemoveTransferAsync(string transferId) => throw new NotSupportedException();
        public IEnumerable<TransferState> GetAllTransfers() => throw new NotSupportedException();
        public IReadOnlyDictionary<string, TransferParticipants> GetAllParticipants() => throw new NotSupportedException();
        public Task KeepAliveAsync(string transferId) => throw new NotSupportedException();
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

    private sealed class RecordingLifetime : IHttpRequestLifetimeFeature
    {
        public CancellationToken RequestAborted { get; set; }
        public bool Aborted { get; private set; }
        public void Abort() => Aborted = true;
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
