using TransportR.Client.Sessions;
using TransportR.Protocol;
using TransportR.Tests.TestHelpers;

namespace TransportR.Tests;

/// <summary>
/// A sender with nothing to send still writes: the hub takes a write stream it has had no segment
/// from for two heartbeat intervals as cut, and a front end cuts a response that carries no byte.
/// </summary>
public class IdleSenderTests
{
    [Fact]
    public async Task AnIdleSender_WritesAPingEveryHeartbeatInterval()
    {
        var hub = new RecordingHub();
        var context = new StubSessionContext(new HttpClient(hub));
        context.Options.PresenceHeartbeatInterval = TimeSpan.FromMilliseconds(100);
        var session = new DataSession<string>(context, Guid.NewGuid().ToString(), batchSize: 1, dataUrl: "http://test/data", affinity: null);
        try
        {
            await session.StartSenderAsync();

            // The first Ping is the handshake that opens the stream; the ones after it are the idle sender.
            var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(10);
            while (hub.Pings() < 4)
            {
                Assert.True(DateTime.UtcNow < deadline, $"only {hub.Pings()} Pings were written");
                await Task.Delay(20);
            }
        }
        finally
        {
            await session.DisposeAsync();
        }
    }

    /// <summary>Relays the segments of the sender's data stream; answers nothing else.</summary>
    private sealed class RecordingHub : HttpMessageHandler
    {
        private readonly MemoryStream _wire = new();

        public int Pings()
        {
            byte[] bytes;
            lock (_wire) bytes = _wire.ToArray();

            var pings = 0;
            using var reader = new MemoryStream(bytes);
            while (ChunkIO.ReadChunkAsync(reader).GetAwaiter().GetResult() is { } chunk)
            {
                using (chunk)
                {
                    if (chunk.Header.Type == ChunkType.Ping) pings++;
                }
            }

            return pings;
        }

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            if (request.Method != HttpMethod.Post)
            {
                await Task.Delay(Timeout.Infinite, cancellationToken);
            }

            using var segment = new MemoryStream();
            var answer = await Segments.RelayAsync(request, segment, cancellationToken);
            lock (_wire) _wire.Write(segment.ToArray());
            return answer;
        }
    }
}
