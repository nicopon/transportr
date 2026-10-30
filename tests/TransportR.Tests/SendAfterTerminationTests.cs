using System.Net;
using TransportR.Abstractions;
using TransportR.Client.Sessions;
using TransportR.Models;
using TransportR.Tests.TestHelpers;

namespace TransportR.Tests;

/// <summary>
/// A producer calling into a handle whose transfer has ended is told so with the exception the
/// contract names, whichever way the transfer ended. A call in the instant between the state turning
/// Failed and the queue closing is accepted; the failure reaches the producer on the next one.
/// The queue underneath closes with the transfer; a caller never sees its exception type.
/// </summary>
public class SendAfterTerminationTests
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(5);

    [Theory]
    [InlineData(null)]
    [InlineData(1)]
    public async Task SendAsync_AfterTheSessionWasReleased_ThrowsTransferFailed(int? window)
    {
        var session = await StartedSenderAsync(new StubHub(), window);
        await session.DisposeAsync();

        await Assert.ThrowsAsync<TransferFailedException>(
            () => session.SendAsync("a", CancellationToken.None).WaitAsync(Bound));
    }

    [Theory]
    [InlineData(null)]
    [InlineData(1)]
    public async Task SendRangeAsync_AfterTheSessionWasReleased_ThrowsTransferFailed(int? window)
    {
        var session = await StartedSenderAsync(new StubHub(), window);
        await session.DisposeAsync();

        await Assert.ThrowsAsync<TransferFailedException>(
            () => session.SendRangeAsync(["a", "b"], CancellationToken.None).WaitAsync(Bound));
    }

    [Theory]
    [InlineData(null)]
    [InlineData(1)]
    public async Task SendAsync_AfterTheHubEndedTheTransfer_ThrowsTransferFailed(int? window)
    {
        var hub = new StubHub();
        await using var session = await StartedSenderAsync(hub, window);
        hub.Forget();
        await UntilAsync(() => session.State == SessionState.Failed);

        await AssertFailureReachesTheProducerAsync(session);
    }

    [Fact]
    public async Task CompleteAsync_WithItemsStillQueued_AfterTheHubEndedTheTransfer_ThrowsTransferFailed()
    {
        var hub = new StubHub();
        await using var session = await StartedSenderAsync(hub, window: null, batchSize: 10);

        // The item sits in a batch that has not been cut, so completing has to flush it into a queue
        // that closed when the transfer failed.
        await session.SendAsync("a", CancellationToken.None);
        hub.Forget();
        await UntilAsync(() => session.State == SessionState.Failed);

        await Assert.ThrowsAsync<TransferFailedException>(
            () => session.CompleteAsync(CancellationToken.None).WaitAsync(Bound));
    }

    // The state turns Failed before the queue closes, so a call landing between the two is accepted,
    // exactly as one made just before the failure would be. What a producer can rely on is that the
    // failure reaches it on a later call, and that once it has it keeps reaching it.
    private static async Task AssertFailureReachesTheProducerAsync(DataSession<string> session)
    {
        var deadline = DateTime.UtcNow + Bound;
        while (true)
        {
            try
            {
                await session.SendAsync("a", CancellationToken.None).WaitAsync(Bound);
            }
            catch (TransferFailedException)
            {
                break;
            }

            Assert.True(DateTime.UtcNow < deadline, "No call was told the transfer had failed.");
            await Task.Yield();
        }

        await Assert.ThrowsAsync<TransferFailedException>(
            () => session.SendAsync("b", CancellationToken.None).WaitAsync(Bound));
    }

    private static async Task<DataSession<string>> StartedSenderAsync(StubHub hub, int? window, int batchSize = 1)
    {
        var context = new StubSessionContext(new HttpClient(hub));
        context.Options.MaxInFlightBatches = window;
        // Long enough that reaching the stall bound would be a hang, not a verdict.
        context.Options.CompleteStallTimeout = TimeSpan.FromMinutes(5);

        var session = new DataSession<string>(context, Guid.NewGuid().ToString(), batchSize, dataUrl: "http://test/data");
        await session.StartSenderAsync();
        return session;
    }

    private static async Task UntilAsync(Func<bool> condition)
    {
        var deadline = DateTime.UtcNow + Bound;
        while (!condition())
        {
            Assert.True(DateTime.UtcNow < deadline, "The condition was not reached in time.");
            await Task.Delay(10);
        }
    }

    /// <summary>
    /// A hub that holds every stream request open until told to forget the transfer, then answers 404
    /// to all of them: what a restarted hub answers for a transfer it lost.
    /// </summary>
    private sealed class StubHub : HttpMessageHandler
    {
        private readonly TaskCompletionSource _forgotten = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public void Forget() => _forgotten.TrySetResult();

        protected override async Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request, CancellationToken cancellationToken)
        {
            await _forgotten.Task.WaitAsync(cancellationToken);
            return new HttpResponseMessage(HttpStatusCode.NotFound);
        }
    }
}
