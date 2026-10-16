using System.Diagnostics;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using TransportR.Abstractions;
using TransportR.Client.SignalR.Configuration;
using TransportR.Client.SignalR.Sessions;
using TransportR.Interfaces;
using TransportR.Models;
using TransportR.Services;
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

    [Fact]
    public async Task AHandlerThatOnlyEnumerates_SeesTheTransferComplete_AndDoesNotAbandonIt()
    {
        var hubLogs = new LogRecorder();
        using var host = TestHubServerFactory.CreateHost(services =>
        {
            // ITransferTerminator.TerminateAsync is the hub ending a transfer unilaterally (abort,
            // cleanup); RetireAsync is the normal domino and must keep working undisturbed.
            services.AddSingleton<TransferTerminator>();
            services.AddSingleton<ITransferTerminator>(sp => new RecordingTerminator(sp.GetRequiredService<TransferTerminator>()));

            services.AddLogging(logging =>
            {
                logging.ClearProviders();
                logging.AddProvider(hubLogs);
                logging.SetMinimumLevel(LogLevel.Information);
            });
        });
        var terminator = (RecordingTerminator)host.Services.GetRequiredService<ITransferTerminator>();
        var handler = host.GetTestServer().CreateHandler();

        var clientLogs = new LogRecorder();
        await using var sender = TestClients.Create(handler, "http://localhost");
        await using var receiver = TestClients.Create(handler, "http://localhost", logger: clientLogs.Factory.CreateLogger("receiver"));

        var handle = new TaskCompletionSource<Receive<string>>(TaskCreationOptions.RunContinuationsAsynchronously);
        receiver.OnTransferStarted += r => { handle.TrySetResult(r); return Task.CompletedTask; };
        await sender.ConnectAsync();
        await receiver.ConnectAsync();

        await using var send = await sender.InitTransferAsync(receiver.ClientId, batchSize: 10);
        var receive = await handle.Task.WaitAsync(Bound);

        // Started before a single item is sent, so the loop ends as soon as the buffer closes -
        // right after the Complete chunk is processed - rather than after also racing the sender's
        // own CompleteAsync through the same round trip. That leaves the receiver's own closing
        // exchange (CompleteAck written, then its own control POST completing) as the window this
        // test is about.
        var items = new List<string>();
        TransferStatus? statusAtLoopEnd = null;
        var consumer = Task.Run(async () =>
        {
            await using (receive)
            {
                // Just the loop: no WaitForCompletionAsync. This is the evident form the contract
                // is meant to make correct.
                await foreach (var item in receive.ReceiveAsync()) items.Add(item);

                // Read before releasing: GetTransfers() drops a session once it is disposed.
                statusAtLoopEnd = receiver.GetTransfers().Single(t => t.TransferId == send.TransferId).Status;
            }
        });

        var expected = Enumerable.Range(0, 25).Select(i => $"item-{i}").ToList();
        await send.SendAsync(expected);
        await send.CompleteAsync().WaitAsync(TimeSpan.FromSeconds(15));
        await consumer.WaitAsync(TimeSpan.FromSeconds(15));

        Assert.Equal(TransferStatus.Completed, statusAtLoopEnd);
        Assert.Equal(expected, items);
        Assert.False(clientLogs.Has(LogLevel.Warning, "Session released before the transfer ended"),
            "the receiver declared its own transfer abandoned, although it received everything.");

        // Positive control: without it, a domino that has not finished yet would pass the next
        // assertion for the wrong reason.
        await UntilAsync(
            () => hubLogs.MessagesFrom(LogLevel.Information).Any(m =>
                m.Contains("Completed normally", StringComparison.Ordinal) && m.Contains(send.TransferId, StringComparison.Ordinal)),
            TimeSpan.FromSeconds(5),
            "the hub never logged a normal completion of the transfer's streams");

        Assert.Empty(terminator.Terminated);
    }

    [Fact]
    public async Task TheEnumeration_Throws_WhenTheHandleIsReleasedBeforeTheOutcome()
    {
        using var host = TestHubServerFactory.CreateHost();
        var handler = host.GetTestServer().CreateHandler();

        await using var sender = TestClients.Create(handler, "http://localhost");
        await using var receiver = TestClients.Create(handler, "http://localhost");

        var handle = new TaskCompletionSource<Receive<string>>(TaskCreationOptions.RunContinuationsAsynchronously);
        receiver.OnTransferStarted += r => { handle.TrySetResult(r); return Task.CompletedTask; };
        await sender.ConnectAsync();
        await receiver.ConnectAsync();

        // batchSize 1: the batch is full as soon as the item is added, so it flushes at once instead
        // of waiting on the batch timeout.
        await using var send = await sender.InitTransferAsync(receiver.ClientId, batchSize: 1);
        var receive = await handle.Task.WaitAsync(Bound);

        await send.SendAsync("item-1");

        var enumeration = Task.Run(async () =>
        {
            var items = new List<string>();
            await foreach (var item in receive.ReceiveAsync()) items.Add(item);
            return items;
        });

        // The one item sent is delivered, and the loop then parks on the next one. Nobody has
        // completed or failed the transfer: it is simply still going.
        await Task.Delay(300);
        Assert.False(enumeration.IsCompleted, "the enumeration should still be parked on the next item.");

        await receive.DisposeAsync();

        // Measured from here, not from the call above: DisposeAsync itself may wait (confirming an
        // Abort reached the wire) before it returns, and that wait is not what this bound is about.
        var clock = Stopwatch.StartNew();
        var failure = await Assert.ThrowsAsync<TransferFailedException>(() => enumeration.WaitAsync(Bound));
        clock.Stop();

        Assert.Contains("released", failure.Message, StringComparison.OrdinalIgnoreCase);
        Assert.True(clock.Elapsed < TimeSpan.FromSeconds(1),
            $"The enumeration took {clock.Elapsed} to report a release that had already happened.");
    }

    [Fact]
    public async Task ABreak_ReleasesTheEnumeratorWithoutWaitingForTheTransfer()
    {
        using var host = TestHubServerFactory.CreateHost();
        var handler = host.GetTestServer().CreateHandler();

        await using var sender = TestClients.Create(handler, "http://localhost");
        await using var receiver = TestClients.Create(handler, "http://localhost");

        var handle = new TaskCompletionSource<Receive<string>>(TaskCreationOptions.RunContinuationsAsynchronously);
        receiver.OnTransferStarted += r => { handle.TrySetResult(r); return Task.CompletedTask; };
        await sender.ConnectAsync();
        await receiver.ConnectAsync();

        await using var send = await sender.InitTransferAsync(receiver.ClientId, batchSize: 1);
        await using var receive = await handle.Task.WaitAsync(Bound);

        await send.SendAsync("item-1");
        await send.SendAsync("item-2");

        // Never completed or failed: the transfer is still very much in progress when the loop exits.
        var clock = Stopwatch.StartNew();
        await Task.Run(async () =>
        {
            await foreach (var item in receive.ReceiveAsync())
            {
                break;
            }
        }).WaitAsync(Bound);
        clock.Stop();

        Assert.True(clock.Elapsed < TimeSpan.FromSeconds(1), $"A break took {clock.Elapsed} to release the enumerator.");
    }

    private static async Task UntilAsync(Func<bool> condition, TimeSpan within, string failure)
    {
        var deadline = DateTime.UtcNow + within;
        while (!condition())
        {
            Assert.True(DateTime.UtcNow < deadline, failure);
            await Task.Delay(20);
        }
    }

    /// <summary>
    /// Counts calls to <see cref="TerminateAsync"/> - the hub ending a transfer unilaterally - while
    /// delegating everything, <see cref="RetireAsync"/> included, to the real terminator.
    /// </summary>
    private sealed class RecordingTerminator(ITransferTerminator inner) : ITransferTerminator
    {
        private readonly List<string> _terminated = [];

        public IReadOnlyList<string> Terminated => _terminated;

        public Task TerminateAsync(string transferId, CancellationToken cancellationToken = default)
        {
            lock (_terminated) _terminated.Add(transferId);
            return inner.TerminateAsync(transferId, cancellationToken);
        }

        public Task RetireAsync(string transferId) => inner.RetireAsync(transferId);
    }
}
