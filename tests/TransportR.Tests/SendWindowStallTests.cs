using TransportR.Abstractions;
using TransportR.Client.SignalR.Sessions;
using TransportR.Models;
using TransportR.Tests.TestHelpers;

namespace TransportR.Tests;

/// <summary>
/// A producer parked on a full send window is released only by an acknowledgement. A receiver that
/// stopped early sends none, so without a bound the producer waits forever — measured from dtpipe,
/// where a consumer run with a row limit left its producer hanging until it was killed.
/// </summary>
public class SendWindowStallTests
{
    [Fact]
    public async Task SendAsync_ParkedWithNothingAcknowledged_FailsOnceTheStallTimeoutElapses()
    {
        var context = new StubSessionContext(new HttpClient(new NeverAnswers()));
        context.Options.MaxInFlightBatches = 1;
        context.Options.CompleteStallTimeout = TimeSpan.FromMilliseconds(200);

        await using var session = new DataSession<string>(
            context, Guid.NewGuid().ToString(), batchSize: 1, dataUrl: "http://test/data");
        await session.StartSenderAsync();

        await session.SendAsync("a", CancellationToken.None);
        var parked = session.SendAsync("b", CancellationToken.None);

        var failure = await Assert.ThrowsAsync<TransferFailedException>(
            () => parked.WaitAsync(TimeSpan.FromSeconds(10)));

        Assert.Equal(session.TransferId, failure.TransferId);
        Assert.Equal(SessionState.Failed, session.State);
    }

    [Fact]
    public async Task SendAsync_ParkedAndCancelled_DoesNotReportTheTransferAsFailed()
    {
        var context = new StubSessionContext(new HttpClient(new NeverAnswers()));
        context.Options.MaxInFlightBatches = 1;
        context.Options.CompleteStallTimeout = TimeSpan.FromMinutes(5);

        await using var session = new DataSession<string>(
            context, Guid.NewGuid().ToString(), batchSize: 1, dataUrl: "http://test/data");
        await session.StartSenderAsync();

        await session.SendAsync("a", CancellationToken.None);
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => session.SendAsync("b", cts.Token));

        // A caller who gave up says nothing about the receiver, which may still be draining.
        Assert.NotEqual(SessionState.Failed, session.State);
    }

    /// <summary>Keeps the session loop waiting on its streams, so nothing else changes its state.</summary>
    private sealed class NeverAnswers : HttpMessageHandler
    {
        protected override async Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request, CancellationToken cancellationToken)
        {
            await Task.Delay(Timeout.Infinite, cancellationToken);
            throw new OperationCanceledException(cancellationToken);
        }
    }
}
