using System.Runtime.CompilerServices;
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

        // Mid-stream cancellation must stop enumeration and surface OperationCanceledException.
        using var cts = new CancellationTokenSource();
        await using var cancelledSend = await sender.InitTransferAsync(receiver.ClientId, batchSize: 3);
        var yielded = 0;
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await cancelledSend.SendAsync(GenerateCancellableAsync(cts, () =>
            {
                if (++yielded == 2) cts.Cancel();
            }), cts.Token));
        Assert.Equal(2, yielded);

        static async IAsyncEnumerable<int> GenerateAsync(int count, [EnumeratorCancellation] CancellationToken ct = default)
        {
            for (var i = 0; i < count; i++)
            {
                ct.ThrowIfCancellationRequested();
                await Task.Yield();
                yield return i;
            }
        }

        static async IAsyncEnumerable<int> GenerateCancellableAsync(
            CancellationTokenSource cts,
            Action onYield,
            [EnumeratorCancellation] CancellationToken ct = default)
        {
            for (var i = 0; i < 100; i++)
            {
                ct.ThrowIfCancellationRequested();
                await Task.Yield();
                onYield();
                yield return i;
            }
        }
    }
}
