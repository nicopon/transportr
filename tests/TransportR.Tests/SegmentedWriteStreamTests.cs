using System.Net;
using System.Text;
using TransportR.Client.Internal;
using TransportR.Client.Sessions;
using TransportR.Interfaces;
using TransportR.Protocol;
using TransportR.Tests.TestHelpers;

namespace TransportR.Tests;

/// <summary>
/// The client's side of a write stream sent in segments: one per flush or per full buffer, at its
/// offset in the epoch, one at a time; a segment the hub held, or refused for its token, goes again
/// in the same epoch, and any other refusal ends it.
/// </summary>
public class SegmentedWriteStreamTests
{
    [Fact]
    public async Task EachFlush_SendsWhatWasWrittenAsOneSegment_AtItsOffset_InOneEpoch()
    {
        var hub = new ScriptedHub();
        var stream = Open(hub);

        await WriteAsync(stream, "abc");
        await stream.FlushAsync();
        await WriteAsync(stream, "de");
        await stream.FlushAsync();
        await stream.FlushAsync();
        await stream.CompleteAsync(CancellationToken.None);

        Assert.Collection(hub.Segments,
            s => Assert.Equal((0L, "abc", false, true), (s.Offset, s.Body, s.Final, s.AnnouncesHeartbeat)),
            s => Assert.Equal((3L, "de", false, false), (s.Offset, s.Body, s.Final, s.AnnouncesHeartbeat)),
            s => Assert.Equal((5L, "", true, false), (s.Offset, s.Body, s.Final, s.AnnouncesHeartbeat)));
        Assert.Single(hub.Segments.Select(s => s.Epoch).Distinct());
    }

    [Fact]
    public async Task AFullBuffer_GoesOutWithoutWaitingForAFlush()
    {
        var hub = new ScriptedHub();
        var stream = Open(hub, bufferSize: 4);

        await WriteAsync(stream, "abcdef");
        Assert.Equal("abcd", Assert.Single(hub.Segments).Body);

        await stream.FlushAsync();
        Assert.Equal((4L, "ef"), (hub.Segments[1].Offset, hub.Segments[1].Body));
    }

    /// <summary>
    /// What a chunk's header and body written apart rely on: the segments depend on the bytes written
    /// and the flushes, not on how many writes carried them.
    /// </summary>
    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    [InlineData(5)]
    public async Task TwoWrites_SendTheSegmentsOneWriteWould_WhereverTheBufferFills(int firstWrite)
    {
        var hub = new ScriptedHub();
        var stream = Open(hub, bufferSize: 4);

        await WriteAsync(stream, "abcdef"[..firstWrite]);
        await WriteAsync(stream, "abcdef"[firstWrite..]);
        await stream.FlushAsync();

        Assert.Equal([(0L, "abcd"), (4L, "ef")], hub.Segments.Select(s => (s.Offset, s.Body)));
    }

    [Fact]
    public async Task ASegmentTheHubHeld_GoesAgainFromWhereThePeerStands()
    {
        var hub = new ScriptedHub();
        hub.Answers.Enqueue(r => Segments.Answer(HttpStatusCode.Accepted, 2));
        var stream = Open(hub);

        await WriteAsync(stream, "abcde");
        await stream.FlushAsync();

        Assert.Equal((2L, "cde"), (hub.Segments[1].Offset, hub.Segments[1].Body));
        Assert.Equal(hub.Segments[0].Epoch, hub.Segments[1].Epoch);
    }

    [Fact]
    public async Task ASegmentRefusedForItsToken_GoesAgainOnce_AtTheSameOffset_WithARenewedToken()
    {
        var hub = new ScriptedHub();
        hub.Answers.Enqueue(_ => new HttpResponseMessage(HttpStatusCode.Unauthorized));
        var transport = new Transport(hub);
        var stream = Open(transport);

        await WriteAsync(stream, "abc");
        await stream.FlushAsync();

        Assert.Equal(2, hub.Segments.Count);
        Assert.Equal((0L, "abc", "Bearer renewed"), (hub.Segments[1].Offset, hub.Segments[1].Body, hub.Segments[1].Authorization));
    }

    [Fact]
    public async Task ASegmentRefusedForItsTokenTwice_EndsTheAttempt_AsRetryable()
    {
        var hub = new ScriptedHub();
        hub.Answers.Enqueue(_ => new HttpResponseMessage(HttpStatusCode.Unauthorized));
        hub.Answers.Enqueue(_ => new HttpResponseMessage(HttpStatusCode.Unauthorized));
        var stream = Open(hub);

        await WriteAsync(stream, "abc");
        var refused = await Assert.ThrowsAsync<StreamRefusedException>(() => stream.FlushAsync());

        Assert.Equal(StreamOutcome.Retryable, refused.Outcome);
    }

    [Theory]
    [InlineData(HttpStatusCode.NotFound, nameof(StreamOutcome.Gone))]
    [InlineData(HttpStatusCode.Gone, nameof(StreamOutcome.Gone))]
    [InlineData(HttpStatusCode.Conflict, nameof(StreamOutcome.Retryable))]
    public async Task ARefusedSegment_EndsTheAttempt_WithWhatTheRefusalMeans(HttpStatusCode status, string outcome)
    {
        var hub = new ScriptedHub();
        hub.Answers.Enqueue(_ => HubResponse.With(status));
        var stream = Open(hub);

        await WriteAsync(stream, "abc");
        var refused = await Assert.ThrowsAsync<StreamRefusedException>(() => stream.FlushAsync());

        Assert.Equal(Enum.Parse<StreamOutcome>(outcome), refused.Outcome);
    }

    [Fact]
    public async Task AnAnswerThatDoesNotSayWhatWasRelayed_IsAProtocolViolation()
    {
        // What a hub that takes the request for a whole stream answers.
        var hub = new ScriptedHub();
        hub.Answers.Enqueue(_ => new HttpResponseMessage(HttpStatusCode.OK));
        var stream = Open(hub);

        await WriteAsync(stream, "abc");

        await Assert.ThrowsAsync<WireProtocolViolationException>(() => stream.FlushAsync());
    }

    [Fact]
    public async Task AStreamWithoutAHeartbeat_AnnouncesNone()
    {
        var hub = new ScriptedHub();
        var stream = Open(new Transport(hub), 512 * 1024, heartbeat: null);

        await WriteAsync(stream, "abc");
        await stream.CompleteAsync(CancellationToken.None);

        Assert.All(hub.Segments, s => Assert.False(s.AnnouncesHeartbeat));
    }

    private static SegmentedWriteStream Open(ScriptedHub hub, int bufferSize = 512 * 1024) => Open(new Transport(hub), bufferSize);

    private static SegmentedWriteStream Open(Transport transport, int bufferSize = 512 * 1024) =>
        Open(transport, bufferSize, TimeSpan.FromSeconds(15));

    private static SegmentedWriteStream Open(Transport transport, int bufferSize, TimeSpan? heartbeat) =>
        new(transport, new StubLogger(), query =>
        {
            var request = new HttpRequestMessage(HttpMethod.Post, $"http://test/data/t-1/send?{query}");
            request.Headers.Add("X-Connection-Type", nameof(ConnectionType.Data));
            return request;
        }, bufferSize, heartbeat);

    private static Task WriteAsync(Stream stream, string text) => stream.WriteAsync(Encoding.ASCII.GetBytes(text)).AsTask();

    private sealed record Segment(Guid Epoch, long Offset, string Body, bool Final, bool AnnouncesHeartbeat, string? Authorization);

    /// <summary>Answers each segment with the next scripted answer, then as relayed in full.</summary>
    private sealed class ScriptedHub : HttpMessageHandler
    {
        public Queue<Func<HttpRequestMessage, HttpResponseMessage>> Answers { get; } = new();

        public List<Segment> Segments { get; } = [];

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var body = await request.Content!.ReadAsByteArrayAsync(cancellationToken);
            Segments.Add(new Segment(
                TestHelpers.Segments.Epoch(request),
                TestHelpers.Segments.Offset(request),
                Encoding.ASCII.GetString(body),
                TestHelpers.Segments.IsFinal(request),
                request.RequestUri!.Query.Contains(DataPlane.HeartbeatParameter + "="),
                request.Headers.Authorization?.ToString()));

            return Answers.TryDequeue(out var answer)
                ? answer(request)
                : TestHelpers.Segments.Answer(HttpStatusCode.OK, TestHelpers.Segments.Offset(request) + body.Length);
        }
    }

    private sealed class Transport(HttpMessageHandler hub) : IStreamTransport
    {
        public HttpClient HttpClient { get; } = new(hub);

        public Task<string?> GetAccessTokenAsync(bool forceRefresh = false) =>
            Task.FromResult<string?>(forceRefresh ? "renewed" : "initial");
    }
}
