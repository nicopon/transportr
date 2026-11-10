using System.Net;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Time.Testing;
using TransportR.Hub.Streams;
using TransportR.Interfaces;
using TransportR.Models;
using TransportR.Protocol;
using TransportR.Tests.Infrastructure;

namespace TransportR.Tests;

/// <summary>
/// The stream routes carry the whole payload, so "authenticated" and "is a participant" have to
/// hold on them and not only on the control plane.
/// </summary>
public class DataPlaneAuthorizationTests
{
    private const string TransferId = "transfer-1";
    private const string SenderSubject = "sender-principal";
    private const string ReceiverSubject = "receiver-principal";

    private static readonly Guid SenderId = Guid.NewGuid();
    private static readonly Guid ReceiverId = Guid.NewGuid();

    [Fact]
    public async Task AnonymousCaller_IsChallenged_OnBothStreamRoutes()
    {
        using var host = await CreateHostWithTransferAsync();
        using var client = host.GetTestClient();

        using var receive = await SendAsync(client, HttpMethod.Get, "receive", ConnectionType.Data, subject: null);
        using var send = await SendAsync(client, HttpMethod.Post, "send", ConnectionType.Data, subject: null);

        Assert.Equal(HttpStatusCode.Unauthorized, receive.StatusCode);
        Assert.Equal(HttpStatusCode.Unauthorized, send.StatusCode);
    }

    [Fact]
    public async Task BothParticipants_AreAdmitted_OnTheirDataStreams()
    {
        using var host = await CreateHostWithTransferAsync();
        using var client = host.GetTestClient();

        // A stream is only served once its counterpart has joined the bridge, so an admitted
        // caller can only be observed as a pair. The sender sends only a last, empty segment: a clean
        // EOF ends the bridge and both requests return.
        var receiver = SendAsync(client, HttpMethod.Get, "receive", ConnectionType.Data, ReceiverSubject);
        var sender = SendAsync(client, HttpMethod.Post, "send", ConnectionType.Data, SenderSubject);

        await AssertBothOkAsync(receiver, sender);
    }

    [Fact]
    public async Task ReadStream_AsksProxiesNotToBufferIt()
    {
        using var host = await CreateHostWithTransferAsync();
        using var client = host.GetTestClient();

        var receiver = SendAsync(client, HttpMethod.Get, "receive", ConnectionType.Data, ReceiverSubject);
        var sender = SendAsync(client, HttpMethod.Post, "send", ConnectionType.Data, SenderSubject);

        using var read = await receiver.WaitAsync(TimeSpan.FromSeconds(30));
        using var written = await sender.WaitAsync(TimeSpan.FromSeconds(30));

        Assert.Equal(HttpStatusCode.OK, read.StatusCode);
        Assert.True(read.Headers.TryGetValues("X-Accel-Buffering", out var buffering));
        Assert.Equal("no", Assert.Single(buffering));
    }

    [Fact]
    public async Task BothParticipants_AreAdmitted_OnTheirControlStreams()
    {
        // The control plane runs the other way round: the sender reads it with a GET and the
        // receiver writes it with a POST. A check keyed on the HTTP method alone would swap the
        // two here, so this is the case that settles the discriminator.
        using var host = await CreateHostWithTransferAsync();
        using var client = host.GetTestClient();

        var sender = SendAsync(client, HttpMethod.Get, "receive", ConnectionType.Control, SenderSubject);
        var receiver = SendAsync(client, HttpMethod.Post, "send", ConnectionType.Control, ReceiverSubject);

        // The receiver's last segment ends its stream, but nothing ends the sender's control GET on its
        // own — that is what it is for. Cancelling the bridge is how it stops in production too.
        await WithTimeoutAsync(receiver);

        host.Services.GetRequiredService<IBridgeManager>()
            .Bridges[$"{TransferId}:{nameof(ConnectionType.Control)}"]
            .Cancel();

        await AssertBothOkAsync(sender, receiver);
    }

    [Theory]
    [InlineData("receive", ConnectionType.Data, SenderSubject)]
    [InlineData("receive", ConnectionType.Control, ReceiverSubject)]
    [InlineData("send", ConnectionType.Data, ReceiverSubject)]
    [InlineData("send", ConnectionType.Control, SenderSubject)]
    public async Task WrongParticipant_IsRefusedAsNotFound(string route, ConnectionType type, string subject)
    {
        using var host = await CreateHostWithTransferAsync();
        using var client = host.GetTestClient();

        var method = route == "send" ? HttpMethod.Post : HttpMethod.Get;

        using var response = await SendAsync(client, method, route, type, subject);

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
    }

    [Fact]
    public async Task Stranger_IsRefusedAsNotFound()
    {
        using var host = await CreateHostWithTransferAsync();
        using var client = host.GetTestClient();

        using var response = await SendAsync(client, HttpMethod.Get, "receive", ConnectionType.Data, "someone-else");

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
    }

    [Fact]
    public async Task UnattestedRegistration_CannotBeClaimed()
    {
        // A registration written without a subject must not become a participant slot that any
        // authenticated caller can step into.
        using var host = await CreateHostWithTransferAsync(receiverSubject: null);
        using var client = host.GetTestClient();

        using var response = await SendAsync(client, HttpMethod.Get, "receive", ConnectionType.Data, ReceiverSubject);

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
    }

    [Fact]
    public async Task UnknownTransfer_IsRefusedAsNotFound()
    {
        using var host = await CreateHostWithTransferAsync();
        using var client = host.GetTestClient();

        var request = new HttpRequestMessage(HttpMethod.Get, $"http://localhost{WireRoutes.StreamBase}/no-such-transfer/receive");
        request.Headers.Add("X-Connection-Type", nameof(ConnectionType.Data));
        request.Headers.Add("Authorization", $"{AuthenticatedHubServerFactory.Scheme} {ReceiverSubject}");

        using var response = await client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead);

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
    }

    [Theory]
    [InlineData("receive", null)]
    [InlineData("send", null)]
    [InlineData("receive", "")]
    [InlineData("receive", "7")]
    [InlineData("send", "1")]
    [InlineData("send", "Data,Control")]
    [InlineData("send", "Both")]
    public async Task MissingOrUndefinedConnectionType_IsRefusedAsBadRequest(string route, string? header)
    {
        // The role a caller must prove depends on this header, so it is never guessed.
        using var host = await CreateHostWithTransferAsync();
        using var client = host.GetTestClient();

        var method = route == "send" ? HttpMethod.Post : HttpMethod.Get;
        var request = new HttpRequestMessage(method, $"http://localhost{WireRoutes.StreamBase}/{TransferId}/{route}");
        if (header != null)
        {
            request.Headers.TryAddWithoutValidation("X-Connection-Type", header);
        }

        request.Headers.Add("Authorization", $"{AuthenticatedHubServerFactory.Scheme} {ReceiverSubject}");
        if (method == HttpMethod.Post)
        {
            request.Content = new ByteArrayContent([]);
        }

        using var response = await client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
    }

    private static async Task<HttpResponseMessage> WithTimeoutAsync(Task<HttpResponseMessage> request)
    {
        var finished = await Task.WhenAny(request, Task.Delay(TimeSpan.FromSeconds(30)));
        Assert.Same(request, finished);
        return await request;
    }

    [Fact]
    public async Task ASegmentFromAnotherIdentity_IsRefused_OnAStreamItsParticipantOpened()
    {
        using var host = await CreateHostWithTransferAsync();
        using var client = host.GetTestClient();
        var epoch = Guid.NewGuid();

        // Admission is judged when the stream opens; past that, a segment has to come from the same
        // identity, or anyone who learned the epoch could write into the transfer.
        using var opened = await SegmentAsync(client, SenderSubject, epoch, offset: 0, announcesHeartbeat: true);
        using var foreign = await SegmentAsync(client, ReceiverSubject, epoch, offset: 0, announcesHeartbeat: false);

        Assert.Equal(HttpStatusCode.OK, opened.StatusCode);
        Assert.Equal(HttpStatusCode.NotFound, foreign.StatusCode);
    }

    [Fact]
    public async Task ASegmentOfAStreamTheHubDoesNotHold_IsAConflict_NotAGoneTransfer()
    {
        using var host = await CreateHostWithTransferAsync();
        using var client = host.GetTestClient();

        using var response = await SegmentAsync(client, SenderSubject, Guid.NewGuid(), offset: 5, announcesHeartbeat: false);

        Assert.Equal(HttpStatusCode.Conflict, response.StatusCode);
    }

    [Fact]
    public async Task ASegmentOfAnOpenStream_WithoutItsOffset_IsABadRequest()
    {
        using var host = await CreateHostWithTransferAsync();
        using var client = host.GetTestClient();
        var epoch = Guid.NewGuid();

        using var opened = await SegmentAsync(client, SenderSubject, epoch, offset: 0, announcesHeartbeat: true);
        using var response = await SegmentAsync(client, SenderSubject, epoch, offset: null, announcesHeartbeat: false);

        Assert.Equal(HttpStatusCode.OK, opened.StatusCode);
        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
    }

    [Fact]
    public async Task TheOpeningOfAStream_WithoutItsHeartbeatInterval_IsABadRequest()
    {
        using var host = await CreateHostWithTransferAsync();
        using var client = host.GetTestClient();

        using var response = await SegmentAsync(client, SenderSubject, Guid.NewGuid(), offset: 0, announcesHeartbeat: false);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
    }

    [Fact]
    public async Task TheSendersDataStream_IsCut_AfterTwoHeartbeatIntervalsWithoutASegment()
    {
        var time = new FakeTimeProvider();
        using var host = await CreateHostWithTransferAsync(time: time);
        using var client = host.GetTestClient();
        var epoch = Guid.NewGuid();

        using var opened = await SegmentAsync(client, SenderSubject, epoch, offset: 0, announcesHeartbeat: true);
        Assert.Equal(HttpStatusCode.OK, opened.StatusCode);

        time.Advance(SilenceWait);
        await UntilAsync(() => !HoldsEpoch(host, ConnectionType.Data, epoch));
    }

    [Fact]
    public async Task TheReceiversControlStream_AnnouncesNoHeartbeat_AndIsNotCutOnSilence()
    {
        // What the receiver writes, its read loop gives it, and a consumer holding on to a batch parks
        // that loop: its silence says nothing. A dead receiver shows at its data GET instead.
        var time = new FakeTimeProvider();
        using var host = await CreateHostWithTransferAsync(time: time);
        using var client = host.GetTestClient();
        var epoch = Guid.NewGuid();

        using var opened = await SegmentAsync(client, ReceiverSubject, epoch, offset: 0, announcesHeartbeat: false, ConnectionType.Control);
        Assert.Equal(HttpStatusCode.OK, opened.StatusCode);

        time.Advance(SilenceWait);
        await Task.Delay(200);
        Assert.True(HoldsEpoch(host, ConnectionType.Control, epoch), "the receiver's control stream was cut on its silence");
    }

    // Past two of the 15 s intervals SegmentAsync announces, and short of the transfer's own inactivity bound.
    private static readonly TimeSpan SilenceWait = TimeSpan.FromSeconds(45);

    private static bool HoldsEpoch(IHost host, ConnectionType type, Guid epoch) =>
        host.Services.GetRequiredService<UploadEpochs>().TryGet(IBridgeManager.KeyFor(TransferId, type), epoch, out _);

    private static async Task UntilAsync(Func<bool> condition)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(10);
        while (!condition())
        {
            Assert.True(DateTime.UtcNow < deadline, "the condition did not hold in time");
            await Task.Delay(20);
        }
    }

    /// <summary>An empty segment of a write stream, which the hub answers at once.</summary>
    private static Task<HttpResponseMessage> SegmentAsync(HttpClient client, string subject, Guid epoch, long? offset, bool announcesHeartbeat, ConnectionType type = ConnectionType.Data)
    {
        var query = $"?{DataPlane.EpochParameter}={epoch}"
            + (offset is { } at ? $"&{DataPlane.OffsetParameter}={at}" : "")
            + (announcesHeartbeat ? $"&{DataPlane.HeartbeatParameter}=15000" : "");
        var request = new HttpRequestMessage(HttpMethod.Post, $"http://localhost{WireRoutes.StreamBase}/{TransferId}/send{query}")
        {
            Content = new ByteArrayContent([]),
        };
        request.Headers.Add("X-Connection-Type", type.ToString());
        request.Headers.Add("Authorization", $"{AuthenticatedHubServerFactory.Scheme} {subject}");
        return client.SendAsync(request).WaitAsync(TimeSpan.FromSeconds(30));
    }

    private static async Task AssertBothOkAsync(Task<HttpResponseMessage> first, Task<HttpResponseMessage> second)
    {
        var both = Task.WhenAll(first, second);
        var finished = await Task.WhenAny(both, Task.Delay(TimeSpan.FromSeconds(30)));
        Assert.Same(both, finished);

        foreach (var response in await both)
        {
            using (response)
            {
                Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            }
        }
    }

    private static async Task<IHost> CreateHostWithTransferAsync(
        string? senderSubject = SenderSubject,
        string? receiverSubject = ReceiverSubject,
        TimeProvider? time = null)
    {
        var host = AuthenticatedHubServerFactory.CreateHost(configureServices: time == null ? null : services => services.AddSingleton<TimeProvider>(time));

        var store = host.Services.GetRequiredService<IStateStore>();
        await store.SaveClientAsync(NewRegistration(SenderId, "conn-sender", senderSubject));
        await store.SaveClientAsync(NewRegistration(ReceiverId, "conn-receiver", receiverSubject));

        var transfers = host.Services.GetRequiredService<ITransferManager>();
        await transfers.RegisterParticipantsAsync(TransferId, SenderId, ReceiverId);

        return host;
    }

    private static ClientRegistration NewRegistration(Guid clientId, string connectionId, string? subject) =>
        new(clientId, connectionId, ["test"], null, null, DateTime.UtcNow, Subject: subject);

    private static Task<HttpResponseMessage> SendAsync(
        HttpClient client,
        HttpMethod method,
        string route,
        ConnectionType type,
        string? subject)
    {
        // A write is one stream in one segment, the last: it opens an epoch and ends it.
        var query = method == HttpMethod.Post
            ? $"?{DataPlane.EpochParameter}={Guid.NewGuid()}&{DataPlane.OffsetParameter}=0&{DataPlane.FinalParameter}=true&{DataPlane.HeartbeatParameter}=15000"
            : "";
        var request = new HttpRequestMessage(method, $"http://localhost{WireRoutes.StreamBase}/{TransferId}/{route}{query}");
        request.Headers.Add("X-Connection-Type", type.ToString());

        if (subject != null)
        {
            request.Headers.Add("Authorization", $"{AuthenticatedHubServerFactory.Scheme} {subject}");
        }

        if (method == HttpMethod.Post)
        {
            request.Content = new ByteArrayContent([]);
        }

        return client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead);
    }
}
