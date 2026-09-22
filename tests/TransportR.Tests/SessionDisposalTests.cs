using TransportR.Abstractions;
using TransportR.Client.SignalR.Sessions;
using TransportR.Models;
using TransportR.Tests.TestHelpers;
using Xunit;

namespace TransportR.Tests;

/// <summary>
/// The other half of the contract <see cref="CompleteAsyncBoundTests"/> covers: a handle released
/// before its transfer reached a terminal state abandons that transfer, and the caller has to be
/// told. Releasing one twice is the documented usage pattern colliding with a defensive call, not
/// a caller error.
/// </summary>
public class SessionDisposalTests
{
    private static DataSession<string> NewSession(StubSessionContext context) =>
        new(context, Guid.NewGuid().ToString(), batchSize: 10, dataUrl: "http://test/data");

    [Fact]
    public async Task WaitForCompletionAsync_AfterTheSessionWasDisposed_ReportsTheAbandonedTransfer()
    {
        var session = NewSession(new StubSessionContext());

        await session.DisposeAsync();

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var failure = await Assert.ThrowsAsync<TransferFailedException>(
            () => session.WaitForCompletionAsync(cts.Token));

        Assert.Equal(session.TransferId, failure.TransferId);
    }

    [Fact]
    public async Task DisposeAsync_WhileAWaitIsPending_EndsThatWait()
    {
        var session = NewSession(new StubSessionContext());

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var wait = session.WaitForCompletionAsync(cts.Token);

        await session.DisposeAsync();

        var finished = await Task.WhenAny(wait, Task.Delay(TimeSpan.FromSeconds(2)));
        Assert.Same(wait, finished);
        await Assert.ThrowsAsync<TransferFailedException>(() => wait);
    }

    [Fact]
    public async Task DisposeAsync_OnANonTerminalSession_LeavesTheSessionSayingSo()
    {
        var session = NewSession(new StubSessionContext());

        await session.DisposeAsync();

        Assert.Equal(SessionState.Disposed, session.State);
    }

    [Fact]
    public async Task CompleteAsync_AfterTheSessionWasDisposed_ReportsTheAbandonedTransfer()
    {
        var context = new StubSessionContext();
        // Long enough that reaching the stall bound would be a hang, not a verdict.
        context.Options.CompleteStallTimeout = TimeSpan.FromMinutes(5);
        var session = NewSession(context);

        await session.DisposeAsync();

        var failure = await Assert.ThrowsAsync<TransferFailedException>(
            () => session.CompleteAsync(CancellationToken.None));

        Assert.Equal(session.TransferId, failure.TransferId);
    }

    [Fact]
    public async Task DisposeAsync_AfterTheTransferReachedAnOutcome_DoesNotOverwriteIt()
    {
        var context = new StubSessionContext();
        context.Options.CompleteStallTimeout = TimeSpan.FromMilliseconds(200);
        var session = NewSession(context);

        await Assert.ThrowsAsync<TransferFailedException>(() => session.CompleteAsync(CancellationToken.None));
        await session.DisposeAsync();

        Assert.Equal(SessionState.Failed, session.State);
    }

    [Fact]
    public async Task DisposeAsync_CalledTwice_IsNotAnError()
    {
        var context = new StubSessionContext();
        var session = NewSession(context);

        await session.DisposeAsync();
        await session.DisposeAsync();

        Assert.Equal(1, context.RemoveSessionCount);
    }
}
