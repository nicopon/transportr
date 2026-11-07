using TransportR.Abstractions;
using TransportR.Client.Sessions;
using TransportR.Models;
using TransportR.Tests.TestHelpers;

namespace TransportR.Tests;

/// <summary>
/// What bounds the completion loop is the absence of acknowledgements, not the number of Complete
/// signals it has sent: a receiver still draining its tail refuses Complete repeatedly while the
/// transfer progresses normally, and a measured run has done so 75 times before succeeding.
/// </summary>
public class CompleteAsyncBoundTests
{
    [Fact]
    public async Task CompleteAsync_WithNothingEverAcknowledged_FailsOnceTheStallTimeoutElapses()
    {
        var context = new StubSessionContext();
        context.Options.CompleteStallTimeout = TimeSpan.FromMilliseconds(200);

        await using var session = new TestSession(
            context, Guid.NewGuid().ToString(), batchSize: 10, dataUrl: "http://test/data", affinity: null);

        var failure = await Assert.ThrowsAsync<TransferFailedException>(
            () => session.CompleteAsync(CancellationToken.None));

        Assert.Equal(session.TransferId, failure.TransferId);
        Assert.Equal(SessionState.Failed, session.State);
    }

    [Fact]
    public async Task CompleteAsync_Cancelled_DoesNotReportTheTransferAsFailed()
    {
        var context = new StubSessionContext();
        context.Options.CompleteStallTimeout = TimeSpan.FromMinutes(5);

        await using var session = new TestSession(
            context, Guid.NewGuid().ToString(), batchSize: 10, dataUrl: "http://test/data", affinity: null);

        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => session.CompleteAsync(cts.Token));

        // A caller who gave up says nothing about the transfer, which may still be draining.
        Assert.NotEqual(SessionState.Failed, session.State);
    }
}
