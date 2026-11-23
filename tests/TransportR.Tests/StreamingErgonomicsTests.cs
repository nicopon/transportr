using System.IO.Compression;
using System.Runtime.CompilerServices;
using System.Text;
using Microsoft.AspNetCore.TestHost;
using TransportR.Client;
using TransportR.Serialization.MessagePack;
using TransportR.Tests.Infrastructure;

namespace TransportR.Tests;

public class StreamingErgonomicsTests
{
    [Fact]
    public async Task SendAsync_IAsyncEnumerable_StreamsItemsAndHonorsCancellation()
    {
        using var host = TestHubServerFactory.CreateHost();
        var handler = host.GetTestServer().CreateHandler();

        await using var sender = new TransportRClientBuilder<int>()
            .WithUrl("http://localhost")
            .WithHttpMessageHandler(handler)
            .WithWriteTransport(WriteTransportPolicy.SegmentsOnly)
            .WithMessagePackSerialization()
            .Build();
        await using var receiver = new TransportRClientBuilder<int>()
            .WithUrl("http://localhost")
            .WithHttpMessageHandler(handler)
            .WithWriteTransport(WriteTransportPolicy.SegmentsOnly)
            .WithMessagePackSerialization()
            .Build();

        var received = new List<int>();
        var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        receiver.OnTransferStarted(async receive =>
        {
            await using (receive)
            {
                await foreach (var item in receive.ReceiveAsync())
                {
                    received.Add(item);
                }
            }
            done.TrySetResult();
        });

        await sender.ConnectAsync();
        await receiver.ConnectAsync();

        await using var send = await sender.InitTransferAsync(receiver.ClientId, batchSize: 3);
        await Assert.ThrowsAsync<ArgumentNullException>(() => send.SendAsync((IAsyncEnumerable<int>)null!));

        await send.SendAsync(GenerateAsync(7));
        await send.CompleteAsync();
        await done.Task.WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Equal([0, 1, 2, 3, 4, 5, 6], received);

        static async IAsyncEnumerable<int> GenerateAsync(int count, [EnumeratorCancellation] CancellationToken ct = default)
        {
            for (var i = 0; i < count; i++)
            {
                ct.ThrowIfCancellationRequested();
                await Task.Yield();
                yield return i;
            }
        }
    }

    [Fact]
    public async Task ByteSendAndReceive_AsStream_InteropsWithGZipStream_WithoutDisposingHandles()
    {
        using var host = TestHubServerFactory.CreateHost();
        var handler = host.GetTestServer().CreateHandler();

        await using var sender = new ByteStreamClientBuilder(batchBytes: 512)
            .WithUrl("http://localhost")
            .WithHttpMessageHandler(handler)
            .WithWriteTransport(WriteTransportPolicy.SegmentsOnly)
            .Build();
        await using var receiver = new ByteStreamClientBuilder(batchBytes: 512)
            .WithUrl("http://localhost")
            .WithHttpMessageHandler(handler)
            .WithWriteTransport(WriteTransportPolicy.SegmentsOnly)
            .Build();

        var payloadText = string.Concat(Enumerable.Repeat("transportr-stream-adapter-line\n", 200));
        var decompressed = new MemoryStream();
        var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        receiver.OnTransferStarted(async receive =>
        {
            await using (receive)
            {
                await using var rawIn = receive.AsStream();
                Assert.True(rawIn.CanRead);
                Assert.False(rawIn.CanWrite);
                Assert.False(rawIn.CanSeek);
                Assert.Equal(0, await rawIn.ReadAsync(Memory<byte>.Empty));

                await using var gzipIn = new GZipStream(rawIn, CompressionMode.Decompress, leaveOpen: true);
                await gzipIn.CopyToAsync(decompressed);

                // Disposing rawIn must leave receive valid until the caller awaits completion/disposal.
                await rawIn.DisposeAsync();
                await Assert.ThrowsAsync<ObjectDisposedException>(() => rawIn.ReadAsync(new byte[16]).AsTask());
                await receive.WaitForCompletionAsync();
            }
            done.TrySetResult();
        });

        await sender.ConnectAsync();
        await receiver.ConnectAsync();

        await using var send = await sender.InitTransferAsync(receiver.ClientId);
        await using (var rawOut = send.AsStream())
        {
            Assert.True(rawOut.CanWrite);
            Assert.False(rawOut.CanRead);
            Assert.False(rawOut.CanSeek);

            await using var gzipOut = new GZipStream(rawOut, CompressionLevel.Fastest, leaveOpen: true);
            await gzipOut.WriteAsync(Encoding.UTF8.GetBytes(payloadText));
            await gzipOut.FlushAsync();
        }

        // Wrapper disposal must not have completed or disposed ByteSend: CompleteAsync still succeeds.
        await send.CompleteAsync();
        await done.Task.WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Equal(payloadText, Encoding.UTF8.GetString(decompressed.ToArray()));
    }
}
