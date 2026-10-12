using System.Diagnostics;
using Microsoft.AspNetCore.TestHost;
using TransportR.Abstractions;
using TransportR.Client.SignalR.Configuration;
using TransportR.Client.SignalR.Sessions;
using TransportR.Tests.Infrastructure;
using TransportR.Tests.TestHelpers;

namespace TransportR.Tests;

/// <summary>
/// What a host that waits for the outcome beside its enumeration can rely on. It is the contract
/// dtpipe reads <c>WaitForCompletionAsync</c> by when it wants to wake a consumer that is blocked
/// on its own output: the wait is a failure signal at once, and a success signal that says nothing
/// about what the consumer has taken.
/// </summary>
public class ReceiveCompletionContractTests
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(5);

    [Fact]
    public async Task WaitForCompletionAsync_NextToAnEnumerationHoldingAnItem_ThrowsAtOnceWhenTheHubEndsTheTransfer()
    {
        // The consumer has taken the first item and is not back for the next: the second chunk waits
        // for room, as it does when a host is blocked writing the item somewhere.
        var context = new StubSessionContext(new HttpClient(new ReceivingHub(batches: 2)));
        context.Options.ReceiveBufferCapacity = BufferCapacity.Items(1);
        context.Options.CompleteStallTimeout = TimeSpan.FromMinutes(5);
        var session = new DataSession<string>(context, Guid.NewGuid().ToString(), batchSize: 1, dataUrl: "http://test/data");
        await session.StartReceiverAsync();

        await using var items = session.ReceiveAsync(CancellationToken.None).GetAsyncEnumerator();
        Assert.True(await items.MoveNextAsync().AsTask().WaitAsync(Bound));

        var wait = session.WaitForCompletionAsync(CancellationToken.None);
        await Task.Delay(300);
        Assert.False(wait.IsCompleted, "The transfer has not ended; the wait has nothing to report.");

        var clock = Stopwatch.StartNew();
        await session.EndByHubAsync();
        var failure = await Assert.ThrowsAsync<TransferFailedException>(() => wait.WaitAsync(Bound));
        clock.Stop();

        Assert.Contains("hub ended", failure.Message);
        Assert.True(clock.Elapsed < TimeSpan.FromSeconds(1), $"The wait took {clock.Elapsed} to report a failure that was already there.");

        // The loop that holds the item learns it at its next step, which is the only place it did before.
        await Assert.ThrowsAsync<TransferFailedException>(async () => await items.MoveNextAsync());
    }

    [Fact]
    public async Task WaitForCompletionAsync_ReturnsOnceTheTransferIsAcknowledged_WhateverTheConsumerHasTaken()
    {
        using var host = TestHubServerFactory.CreateHost();
        var handler = host.GetTestServer().CreateHandler();

        await using var sender = TestClients.Create(handler, "http://localhost");
        await using var receiver = TestClients.Create(handler, "http://localhost");

        var handle = new TaskCompletionSource<Receive<string>>(TaskCreationOptions.RunContinuationsAsynchronously);
        receiver.OnTransferStarted += r => { handle.TrySetResult(r); return Task.CompletedTask; };
        await sender.ConnectAsync();
        await receiver.ConnectAsync();

        await using var send = await sender.InitTransferAsync(receiver.ClientId, batchSize: 10);
        await using var receive = await handle.Task.WaitAsync(Bound);

        var expected = Enumerable.Range(0, 25).Select(i => $"item-{i}").ToList();
        await send.SendAsync(expected);
        await send.CompleteAsync().WaitAsync(TimeSpan.FromSeconds(30));

        // Nobody has enumerated. The transfer is over for the protocol: every batch is in the buffer
        // and the sender has been told so.
        await receive.WaitForCompletionAsync().WaitAsync(Bound);

        // A host that stopped consuming when the wait returned would have lost all of it.
        var items = new List<string>();
        await foreach (var item in receive.ReceiveAsync()) items.Add(item);
        Assert.Equal(expected, items);
    }
}
