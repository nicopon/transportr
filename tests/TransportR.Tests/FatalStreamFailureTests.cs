using System.Net;
using TransportR.Abstractions;
using TransportR.Client.SignalR.Sessions;
using TransportR.Models;
using TransportR.Tests.TestHelpers;
using Xunit;

namespace TransportR.Tests;

/// <summary>
/// A stream the hub refuses for good - a transfer it no longer knows - fails the session. Whoever
/// is parked on that session has to learn it from the failure itself, as they do from a peer's
/// Abort: <c>WaitForCompletionAsync</c> is the one door the contract leaves open after the
/// enumeration, so it cannot be the only way out.
/// </summary>
public class FatalStreamFailureTests
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(5);

    [Fact]
    public async Task ReceiveAsync_ParkedOnAnEmptyBuffer_ThrowsWhenTheHubNoLongerKnowsTheTransfer()
    {
        var hub = new ForgetfulHub();
        await using var session = NewSession(hub);
        await session.StartReceiverAsync();

        var consumer = Task.Run(async () =>
        {
            await foreach (var _ in session.ReceiveAsync(CancellationToken.None)) { }
        });
        await AssertParkedAsync(consumer);

        hub.Forget();

        await Assert.ThrowsAsync<TransferFailedException>(() => consumer.WaitAsync(Bound));
    }

    [Fact]
    public async Task SendAsync_ParkedOnAFullWindow_ThrowsWhenTheHubNoLongerKnowsTheTransfer()
    {
        var hub = new ForgetfulHub();
        await using var session = NewSession(hub, o => o.MaxInFlightBatches = 1);
        await session.StartSenderAsync();

        // The first item takes the only slot and nobody acknowledges it, so the second parks.
        await session.SendAsync("a", CancellationToken.None);
        var parked = session.SendAsync("b", CancellationToken.None);
        await AssertParkedAsync(parked);

        hub.Forget();

        await Assert.ThrowsAsync<TransferFailedException>(() => parked.WaitAsync(Bound));
    }

    private static DataSession<string> NewSession(ForgetfulHub hub, Action<TransportR.Client.SignalR.Configuration.ClientOptions>? configure = null)
    {
        var context = new StubSessionContext(new HttpClient(hub));
        // Long enough that reaching the stall bound would be a hang, not a verdict.
        context.Options.CompleteStallTimeout = TimeSpan.FromMinutes(5);
        configure?.Invoke(context.Options);

        return new DataSession<string>(context, Guid.NewGuid().ToString(), batchSize: 1, dataUrl: "http://test/data");
    }

    private static async Task AssertParkedAsync(Task task)
    {
        await Task.Delay(TimeSpan.FromMilliseconds(300));
        Assert.False(task.IsCompleted, "The call was expected to be parked before the failure.");
    }

    /// <summary>
    /// A hub that holds every stream request until told to forget the transfer, then answers 404 to
    /// all of them: what a restarted hub answers for a transfer it lost.
    /// </summary>
    private sealed class ForgetfulHub : HttpMessageHandler
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
