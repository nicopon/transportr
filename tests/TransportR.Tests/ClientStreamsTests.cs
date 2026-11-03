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

    private static (Task<CallbackOutcome> Answer, Guid RequestId) Ask(ClientStreams streams, ClientStream stream)
    {
        var requestId = Guid.Empty;
        var answer = streams.AskAsync(stream.ConnectionId, ControlPlane.ReadyToReceive, id => { requestId = id; return id; }, CancellationToken.None);
        return (answer, requestId);
    }
}
