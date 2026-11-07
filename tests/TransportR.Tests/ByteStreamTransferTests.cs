using Microsoft.AspNetCore.TestHost;
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

    private static TransportRClient<byte[]> CreateClient(
        HttpMessageHandler handler, int? byteStreamBytes, IBatchSerializer<byte[]>? serializer = null,
        Action<ClientOptions>? configure = null)
    {
        const string url = "http://localhost";
        var options = new ClientOptions
        {
            // A TestServer handler cannot carry a WebSocket.
            WriteTransportPolicy = WriteTransportPolicy.SegmentsOnly,
            MaxInFlightBatches = 4,
            ByteStreamBatchBytes = byteStreamBytes
        };
        configure?.Invoke(options);
        ILogger logger = NullLogger.Instance;
        var httpClient = new HttpClient(handler) { BaseAddress = new Uri(url), Timeout = Timeout.InfiniteTimeSpan };
        var channel = new ControlChannel(url, Guid.NewGuid(), httpClient, options, logger);

        return new TransportRClient<byte[]>(
            channel,
            new SessionRegistry<byte[]>(logger),
            serializer,
            logger,
            defaultBatchSize: 100,
            options,
            url + WireRoutes.StreamBase,
            httpClient,
            new Microsoft.IO.RecyclableMemoryStreamManager());
    }

    private static IBatchSerializer<byte[]> Serializer() => new MessagePackBatchSerializer<byte[]>();

    /// <summary>Opens a transfer between two clients and hands back both handles.</summary>
    private static async Task<(Send<byte[]> Send, Receive<byte[]> Receive)> OpenAsync(
        TransportRClient<byte[]> sender, TransportRClient<byte[]> receiver, int? senderBatchSize = null)
    {
        var receiveHandle = new TaskCompletionSource<Receive<byte[]>>(TaskCreationOptions.RunContinuationsAsynchronously);
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
        await using var sender = CreateClient(handler, byteStreamBytes: 1000);
        await using var receiver = CreateClient(handler, byteStreamBytes: 1000);
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
        await using var sender = CreateClient(handler, byteStreamBytes: 1000);
        await using var receiver = CreateClient(handler, byteStreamBytes: 1000);
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
        await using var sender = CreateClient(handler, byteStreamBytes: 1000);
        await using var receiver = CreateClient(handler, byteStreamBytes: null, Serializer());
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
        await using var sender = CreateClient(handler, byteStreamBytes: null, Serializer());
        await using var receiver = CreateClient(handler, byteStreamBytes: 1000);
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
        await using var sender = CreateClient(handler, byteStreamBytes: 1000, configure: o =>
        {
            o.MaxInFlightBatches = 1;
            o.CompleteStallTimeout = TimeSpan.FromSeconds(2);
        });
        // A receiver nobody reads from: its buffer fills, its read loop parks, and it acknowledges no more.
        await using var receiver = CreateClient(handler, byteStreamBytes: 1000, configure: o =>
            o.ReceiveBufferCapacity = BufferCapacity.Bytes(1));
        var (send, receive) = await OpenAsync(sender, receiver);
        await using var _ = send;
        await using var __ = receive;

        await Assert.ThrowsAsync<TransferFailedException>(
            () => send.CopyFromAsync(new MemoryStream(Payload(50_000)))).WaitAsync(Bound);
    }
}
