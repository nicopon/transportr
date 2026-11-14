using Microsoft.Extensions.Time.Testing;
using TransportR.Hub.Services;
using TransportR.Protocol;

namespace TransportR.Tests;

/// <summary>
/// A client that refreshes its registration opens a new stream before it closes the old one. A question
/// the hub asked on the old stream is still answered: the reply is a request of its own, which names the
/// client and the question, not the stream.
/// </summary>
public class ClientStreamsTests
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan Heartbeat = TimeSpan.FromMinutes(1);
    private static readonly TimeSpan Presence = TimeSpan.FromSeconds(10);

    [Fact]
    public async Task AReplyAwaitedFromAReplacedStream_IsTaken_WhenTheClientHoldsAnotherStream()
    {
        var streams = new ClientStreams(TimeProvider.System);
        var clientId = Guid.NewGuid();
        var old = streams.Open(clientId, Heartbeat);
        var (answer, requestId) = Ask(streams, old);

        streams.Open(clientId, Heartbeat);
        streams.Remove(old);

        Assert.True(streams.TryReply(clientId, requestId, CallbackOutcome.Accepted),
            "the reply to a question asked on the replaced stream was refused");
        Assert.Equal(CallbackOutcome.Accepted, await answer.WaitAsync(Bound));
    }

    [Fact]
    public async Task AReplyAwaitedFromAReplacedStream_Fails_WhenTheClientsLastStreamCloses()
    {
        var streams = new ClientStreams(TimeProvider.System);
        var clientId = Guid.NewGuid();
        var old = streams.Open(clientId, Heartbeat);
        var (answer, _) = Ask(streams, old);

        var successor = streams.Open(clientId, Heartbeat);
        streams.Remove(old);
        streams.Remove(successor);

        await Assert.ThrowsAsync<IOException>(() => answer.WaitAsync(Bound));
    }

    [Fact]
    public async Task Touch_KeepsEveryStreamOfTheClientOpen_AndOnlyTheirs()
    {
        var clock = new FakeTimeProvider(new DateTimeOffset(2030, 1, 1, 12, 0, 0, TimeSpan.Zero));
        var streams = new ClientStreams(clock);
        var clientId = Guid.NewGuid();
        var first = streams.Open(clientId, Presence);
        var second = streams.Open(clientId, Presence);
        var stranger = streams.Open(Guid.NewGuid(), Presence);

        // A stream ends once nothing has been heard for two presence intervals (20 s): at 34 s only
        // the stranger's, heard from at 0 s, is past it; the client's, heard from at 15 s, has 1 s left.
        clock.Advance(TimeSpan.FromSeconds(15));
        streams.Touch(clientId);
        clock.Advance(TimeSpan.FromSeconds(19));
        var firstNext = await ReadPastRegistrationAsync(first);
        var secondNext = await ReadPastRegistrationAsync(second);
        var strangerNext = await ReadPastRegistrationAsync(stranger);

        Assert.False(await strangerNext.WaitAsync(Bound));
        Assert.NotNull(stranger.EndReason);
        Assert.False(firstNext.IsCompleted, "a stream of the client that was heard from ended");
        Assert.False(secondNext.IsCompleted, "a stream of the client that was heard from ended");

        clock.Advance(TimeSpan.FromSeconds(1));
        Assert.False(await firstNext.WaitAsync(Bound));
        Assert.False(await secondNext.WaitAsync(Bound));
    }

    [Fact]
    public async Task Touch_KeepsTheRemainingStreamOpen_WhenTheClientsOtherStreamWasRemoved()
    {
        var clock = new FakeTimeProvider(new DateTimeOffset(2030, 1, 1, 12, 0, 0, TimeSpan.Zero));
        var streams = new ClientStreams(clock);
        var clientId = Guid.NewGuid();
        var old = streams.Open(clientId, Presence);
        var current = streams.Open(clientId, Presence);
        streams.Remove(old);

        clock.Advance(TimeSpan.FromSeconds(15));
        streams.Touch(clientId);
        clock.Advance(TimeSpan.FromSeconds(19));
        var currentNext = await ReadPastRegistrationAsync(current);

        Assert.False(currentNext.IsCompleted, "the remaining stream was not reached by the client's heartbeat");
    }

    private static async Task<Task<bool>> ReadPastRegistrationAsync(ClientStream stream)
    {
        var events = stream.ReadAllAsync(CancellationToken.None).GetAsyncEnumerator();
        await events.MoveNextAsync();
        return events.MoveNextAsync().AsTask();
    }

    private static (Task<CallbackOutcome> Answer, Guid RequestId) Ask(ClientStreams streams, ClientStream stream)
    {
        var requestId = Guid.Empty;
        var answer = streams.AskAsync(stream.ConnectionId, ControlPlane.ReadyToReceive, id => { requestId = id; return id; }, CancellationToken.None);
        return (answer, requestId);
    }
}
