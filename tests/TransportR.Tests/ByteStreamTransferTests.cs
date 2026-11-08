using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using TransportR.Abstractions;
using TransportR.Client;
using TransportR.Client.Configuration;
using TransportR.Client.Internal;
using TransportR.Client.Services;
using TransportR.Interfaces;
using TransportR.Models;
using TransportR.Protocol;
using TransportR.Serialization.MessagePack.Services;
using TransportR.Tests.Infrastructure;

namespace TransportR.Tests;

/// <summary>
/// The byte-stream mode from one client to another through a hub, over <c>TestServer</c>: what the
/// public calls carry, and what two ends configured differently do to each other.
/// </summary>
public class ByteStreamTransferTests
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(20);

    private static (ControlChannel Channel, HttpClient Http, ClientOptions Options, ILogger Logger) Parts(
        HttpMessageHandler handler, Action<ClientOptions>? configure)
    {
        const string url = "http://localhost";
        var options = new ClientOptions
        {
            // A TestServer handler cannot carry a WebSocket.
            WriteTransportPolicy = WriteTransportPolicy.SegmentsOnly,
            MaxInFlightBatches = 4
        };
        configure?.Invoke(options);
        ILogger logger = NullLogger.Instance;
        var httpClient = new HttpClient(handler) { BaseAddress = new Uri(url), Timeout = Timeout.InfiniteTimeSpan };
        var channel = new ControlChannel(url, Guid.NewGuid(), httpClient, options, logger);
        return (channel, httpClient, options, logger);
    }

    private static ByteStreamClient CreateByteClient(
        HttpMessageHandler handler, int batchBytes, Action<ClientOptions>? configure = null)
    {
        var (channel, httpClient, options, logger) = Parts(handler, configure);

        return new ByteStreamClient(
            channel,
            new SessionRegistry<ByteTransferSession>(logger),
            logger,
            batchBytes,
            options,
            "http://localhost" + WireRoutes.StreamBase,
            httpClient,
            new Microsoft.IO.RecyclableMemoryStreamManager());
    }

    private static TransportRClient<byte[]> CreateItemClient(HttpMessageHandler handler)
    {
        var (channel, httpClient, options, logger) = Parts(handler, configure: null);

        return new TransportRClient<byte[]>(
            channel,
            new SessionRegistry<TransferSession<byte[]>>(logger),
            new MessagePackBatchSerializer<byte[]>(),
            logger,
            defaultBatchSize: 100,
            options,
            "http://localhost" + WireRoutes.StreamBase,
            httpClient,
            new Microsoft.IO.RecyclableMemoryStreamManager());
    }

    /// <summary>Opens a transfer between two byte-stream clients and hands back both handles.</summary>
    private static async Task<(ByteSend Send, ByteReceive Receive)> OpenAsync(ByteStreamClient sender, ByteStreamClient receiver)
    {
        var receiveHandle = new TaskCompletionSource<ByteReceive>(TaskCreationOptions.RunContinuationsAsynchronously);
        receiver.OnTransferStarted += receive => { receiveHandle.TrySetResult(receive); return Task.CompletedTask; };

        await sender.ConnectAsync();
        await receiver.ConnectAsync();

        var send = await sender.InitTransferAsync(receiver.ClientId);
        var receive = await receiveHandle.Task.WaitAsync(Bound);
        return (send, receive);
    }

    /// <summary>Opens a transfer from a byte-stream client to a client of items and hands back both handles.</summary>
    private static async Task<(ByteSend Send, Receive<byte[]> Receive)> OpenAsync(ByteStreamClient sender, TransportRClient<byte[]> receiver)
    {
        var receiveHandle = new TaskCompletionSource<Receive<byte[]>>(TaskCreationOptions.RunContinuationsAsynchronously);
        receiver.OnTransferStarted += receive => { receiveHandle.TrySetResult(receive); return Task.CompletedTask; };

        await sender.ConnectAsync();
        await receiver.ConnectAsync();

        var send = await sender.InitTransferAsync(receiver.ClientId);
        var receive = await receiveHandle.Task.WaitAsync(Bound);
        return (send, receive);
    }

    /// <summary>Opens a transfer from a client of items to a byte-stream client and hands back both handles.</summary>
    private static async Task<(Send<byte[]> Send, ByteReceive Receive)> OpenAsync(
        TransportRClient<byte[]> sender, ByteStreamClient receiver, int senderBatchSize)
    {
        var receiveHandle = new TaskCompletionSource<ByteReceive>(TaskCreationOptions.RunContinuationsAsynchronously);
        receiver.OnTransferStarted += receive => { receiveHandle.TrySetResult(receive); return Task.CompletedTask; };

        await sender.ConnectAsync();
        await receiver.ConnectAsync();

        var send = await sender.InitTransferAsync(receiver.ClientId, senderBatchSize);
        var receive = await receiveHandle.Task.WaitAsync(Bound);
        return (send, receive);
    }

    /// <summary>A source that gives back less than it is asked for, as a pipe does.</summary>
    private sealed class TrickleStream(byte[] data, int most) : Stream
    {
        private int _position;

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            await Task.Yield();
            var count = Math.Min(Math.Min(buffer.Length, most), data.Length - _position);
            data.AsMemory(_position, count).CopyTo(buffer);
            _position += count;
            return count;
        }

        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
        public override void Flush() { }
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }

    private static byte[] Payload(int length)
    {
        var data = new byte[length];
        new Random(20260101).NextBytes(data);
        return data;
    }

    [Fact]
    public async Task AStreamOfSeveralBatches_NotAMultipleOfTheBudget_ArrivesIdenticalToTheByte()
    {
        using var host = TestHubServerFactory.CreateHost();
        var handler = host.GetTestServer().CreateHandler();
        await using var sender = CreateByteClient(handler, batchBytes: 1000);
        await using var receiver = CreateByteClient(handler, batchBytes: 1000);
        var (send, receive) = await OpenAsync(sender, receiver);
        await using var _ = send;
        await using var __ = receive;

        var source = Payload(7321);
        var received = new MemoryStream();
        // CopyToAsync returns once the transfer has ended, not at the last byte, so its outcome is known
        // by then.
        var consumer = Task.Run(async () =>
        {
            await receive.CopyToAsync(received);
            return receive.WaitForCompletionAsync().IsCompletedSuccessfully;
        });

        // Reads of 700 bytes and batches of 1000: no read lines up with a batch.
        await send.CopyFromAsync(new TrickleStream(source, most: 700));
        await send.CompleteAsync().WaitAsync(Bound);
        Assert.True(await consumer.WaitAsync(Bound), "CopyToAsync returned before the transfer had ended.");

        Assert.Equal(source.Length, received.Length);
        Assert.True(source.AsSpan().SequenceEqual(received.ToArray()));
    }

    [Fact]
    public async Task CopyFromAsync_LeavesTheTransferOpen_UntilTheCallerCompletesIt()
    {
        using var host = TestHubServerFactory.CreateHost();
        var handler = host.GetTestServer().CreateHandler();
        await using var sender = CreateByteClient(handler, batchBytes: 1000);
        await using var receiver = CreateByteClient(handler, batchBytes: 1000);
        var (send, receive) = await OpenAsync(sender, receiver);
        await using var _ = send;
        await using var __ = receive;

        var received = new MemoryStream();
        var consumer = Task.Run(() => receive.CopyToAsync(received));

        await send.CopyFromAsync(new MemoryStream(Payload(2500)));
        await Task.Delay(500);
        Assert.False(consumer.IsCompleted, "CopyFromAsync completed the transfer it was given.");

        await send.CompleteAsync().WaitAsync(Bound);
        await consumer.WaitAsync(Bound);
        Assert.Equal(2500, received.Length);
    }

    [Fact]
    public async Task ASenderInTheByteStreamMode_AgainstAReceiverWithASerializer_FailsBothEnds_InsteadOfReconnecting()
    {
        using var host = TestHubServerFactory.CreateHost();
        var handler = host.GetTestServer().CreateHandler();
        await using var sender = CreateByteClient(handler, batchBytes: 1000);
        await using var receiver = CreateItemClient(handler);
        var (send, receive) = await OpenAsync(sender, receiver);
        await using var _ = send;
        await using var __ = receive;

        var consumer = Task.Run(async () =>
        {
            await foreach (var _ in receive.ReceiveAsync()) { }
        });

        var sent = await Assert.ThrowsAsync<TransferFailedException>(async () =>
        {
            await send.CopyFromAsync(new MemoryStream(Payload(5000)));
            await send.CompleteAsync();
        }).WaitAsync(Bound);
        var received = await Assert.ThrowsAsync<TransferFailedException>(() => consumer.WaitAsync(Bound));

        // Told by the receiver's Abort, not by a stall bound.
        Assert.Equal(AbortReason.UndecodableBatch.Describe(), sent.Message);
        Assert.NotEmpty(received.Message);
    }

    [Fact]
    public async Task ASenderWithASerializer_AgainstAReceiverInTheByteStreamMode_FailsBothEnds_InsteadOfReconnecting()
    {
        using var host = TestHubServerFactory.CreateHost();
        var handler = host.GetTestServer().CreateHandler();
        await using var sender = CreateItemClient(handler);
        await using var receiver = CreateByteClient(handler, batchBytes: 1000);
        var (send, receive) = await OpenAsync(sender, receiver, senderBatchSize: 5);
        await using var _ = send;
        await using var __ = receive;

        var consumer = Task.Run(() => receive.CopyToAsync(new MemoryStream()));

        var sent = await Assert.ThrowsAsync<TransferFailedException>(async () =>
        {
            await send.SendAsync(Enumerable.Range(0, 20).Select(i => new byte[] { (byte)i, 1, 2 }));
            await send.CompleteAsync();
        }).WaitAsync(Bound);
        var received = await Assert.ThrowsAsync<TransferFailedException>(() => consumer.WaitAsync(Bound));

        Assert.Equal(AbortReason.UndecodableBatch.Describe(), sent.Message);
        Assert.NotEmpty(received.Message);
    }

    [Fact]
    public async Task AProducerParkedOnAFullWindow_FailsOnTheStallBound_WithTheTransfersFailure()
    {
        using var host = TestHubServerFactory.CreateHost();
        var handler = host.GetTestServer().CreateHandler();
        await using var sender = CreateByteClient(handler, batchBytes: 1000, configure: o =>
        {
            o.MaxInFlightBatches = 1;
            o.CompleteStallTimeout = TimeSpan.FromSeconds(2);
        });
        // A receiver nobody reads from: its buffer fills, its read loop parks, and it acknowledges no more.
        await using var receiver = CreateByteClient(handler, batchBytes: 1000, configure: o =>
            o.ReceiveBufferCapacity = BufferCapacity.Bytes(1));
        var (send, receive) = await OpenAsync(sender, receiver);
        await using var _ = send;
        await using var __ = receive;

        await Assert.ThrowsAsync<TransferFailedException>(
            () => send.CopyFromAsync(new MemoryStream(Payload(50_000)))).WaitAsync(Bound);
    }

    [Fact]
    public async Task AHostInitiatedTransfer_ToASenderInTheByteStreamMode_DeliversEveryByte_WhateverBatchSizeTheHostNames()
    {
        using var host = TestHubServerFactory.CreateHost();
        var handler = host.GetTestServer().CreateHandler();
        await using var sender = CreateByteClient(handler, batchBytes: 1000);
        await using var receiver = CreateByteClient(handler, batchBytes: 1000);

        var sendHandle = new TaskCompletionSource<ByteSend>(TaskCreationOptions.RunContinuationsAsynchronously);
        var receiveHandle = new TaskCompletionSource<ByteReceive>(TaskCreationOptions.RunContinuationsAsynchronously);
        sender.OnSendRequested += send => { sendHandle.TrySetResult(send); return Task.CompletedTask; };
        receiver.OnTransferStarted += receive => { receiveHandle.TrySetResult(receive); return Task.CompletedTask; };
        await sender.ConnectAsync();
        await receiver.ConnectAsync();

        // The host does not know the sender's mode: the figure it passes is a count of items, and means nothing here.
        var initiator = host.Services.GetRequiredService<ITransferInitiator>();
        await initiator.InitTransferAsync(sender.ClientId, receiver.ClientId, batchSize: 10, timeoutMs: 20_000);

        await using var send = await sendHandle.Task.WaitAsync(Bound);
        await using var receive = await receiveHandle.Task.WaitAsync(Bound);

        var source = Payload(7321);
        var received = new MemoryStream();
        var consumer = Task.Run(() => receive.CopyToAsync(received));

        await send.CopyFromAsync(new TrickleStream(source, 700));
        await send.CompleteAsync().WaitAsync(Bound);
        await consumer.WaitAsync(Bound);

        Assert.True(source.AsSpan().SequenceEqual(received.ToArray()));

        // Eight batches of 1000 bytes at most: a batch cut at the host's 10 would have been several hundred.
        var info = Assert.Single(sender.GetTransfers());
        Assert.InRange(info.AcksReceived, 1, 20);
    }

    [Fact]
    public async Task CopyToAsync_DoesNotReturn_WhileTheHubHasNotHeardTheReceiversAnswerToComplete()
    {
        using var host = TestHubServerFactory.CreateHost();
        var gate = new SendGate(host.GetTestServer().CreateHandler());
        await using var sender = CreateByteClient(host.GetTestServer().CreateHandler(), batchBytes: 1000);
        await using var receiver = CreateByteClient(gate, batchBytes: 1000);
        var (send, receive) = await OpenAsync(sender, receiver);
        await using var _ = send;
        await using var __ = receive;

        // Whole batches only: nothing waits in the sender for Complete to cut it.
        var source = Payload(3000);
        var written = new CountingStream();
        var consumer = Task.Run(() => receive.CopyToAsync(written));

        await send.CopyFromAsync(new MemoryStream(source));
        await WaitUntil(() => written.Count == source.Length);

        // The receiver now has every byte. Its answer to Complete is held on its way to the hub, so the
        // transfer is not over for the hub, and the last chunk is not the last word.
        gate.Close();
        var completing = send.CompleteAsync();
        await Task.Delay(TimeSpan.FromSeconds(1.5));

        Assert.False(consumer.IsCompleted, "CopyToAsync returned before the hub ended the transfer");

        gate.Open();
        await completing.WaitAsync(Bound);
        await consumer.WaitAsync(Bound);
    }

    [Fact]
    public async Task SendingBytes_OnceCompleteAsyncHasBegun_IsRefused_RatherThanDroppedInSilence()
    {
        using var host = TestHubServerFactory.CreateHost();
        var gate = new SendGate(host.GetTestServer().CreateHandler());
        await using var sender = CreateByteClient(host.GetTestServer().CreateHandler(), batchBytes: 1000);
        await using var receiver = CreateByteClient(gate, batchBytes: 1000);
        var (send, receive) = await OpenAsync(sender, receiver);
        await using var _ = send;
        await using var __ = receive;

        var source = Payload(3000);
        var written = new CountingStream();
        var consumer = Task.Run(() => receive.CopyToAsync(written));
        await send.CopyFromAsync(new MemoryStream(source));
        await WaitUntil(() => written.Count == source.Length);

        // Complete is under way and cannot end: the receiver's answer is held. A send made now is
        // not part of the transfer, and its caller is told.
        gate.Close();
        var completing = send.CompleteAsync();
        await Assert.ThrowsAsync<InvalidOperationException>(() => send.WriteAsync(new byte[] { 1 }));
        await Assert.ThrowsAsync<InvalidOperationException>(() => send.CopyFromAsync(new MemoryStream(new byte[10])));

        gate.Open();
        await completing.WaitAsync(Bound);
        await consumer.WaitAsync(Bound);

        await Assert.ThrowsAsync<InvalidOperationException>(() => send.CopyFromAsync(new MemoryStream(new byte[10])));
        Assert.Equal(source.Length, written.Count);
    }

    private static async Task WaitUntil(Func<bool> condition)
    {
        var deadline = DateTime.UtcNow + Bound;
        while (!condition())
        {
            if (DateTime.UtcNow > deadline) throw new TimeoutException("The condition did not become true in time.");
            await Task.Delay(10);
        }
    }

    /// <summary>A destination that only counts, safe to read from the test while the copy writes.</summary>
    private sealed class CountingStream : Stream
    {
        private long _count;
        public long Count => Interlocked.Read(ref _count);

        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            Interlocked.Add(ref _count, buffer.Length);
            return ValueTask.CompletedTask;
        }

        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override bool CanRead => false;
        public override bool CanSeek => false;
        public override bool CanWrite => true;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
        public override void Flush() { }
        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
    }

    /// <summary>Holds the segments of a client's write stream while closed, and lets them through when opened.</summary>
    private sealed class SendGate(HttpMessageHandler inner) : DelegatingHandler(inner)
    {
        private volatile TaskCompletionSource? _closed;

        public void Close() => _closed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        public void Open()
        {
            var closed = _closed;
            _closed = null;
            closed?.TrySetResult();
        }

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var closed = _closed;
            if (closed is not null && request.Method == HttpMethod.Post
                && request.RequestUri!.AbsolutePath.EndsWith("/send", StringComparison.Ordinal))
            {
                await closed.Task.WaitAsync(cancellationToken);
            }

            return await base.SendAsync(request, cancellationToken);
        }
    }
}
