using System.Net;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Time.Testing;
using TransportR.Hub;
using TransportR.Hub.Interfaces;
using TransportR.Hub.Services;
using TransportR.Hub.Streams;
using TransportR.Protocol;
using TransportR.Tests.Infrastructure;

namespace TransportR.Tests;

/// <summary>
/// A transfer the hub ends is an access decision, which only the hub takes: its data streams end
/// whether or not a participant acts on the <c>CancelTransfer</c> event. Driven with bare requests,
/// because a TransportR client stops on that event and would end the streams itself.
/// </summary>
public class TerminatedTransferStreamsTests
{
    private const string TransferId = "transfer-1";
    private const string SenderSubject = "sender-principal";
    private const string ReceiverSubject = "receiver-principal";

    private static readonly Guid SenderId = Guid.NewGuid();
    private static readonly Guid ReceiverId = Guid.NewGuid();
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(10);

    [Fact]
    public async Task TerminateAsync_EndsBothDataStreams_WithoutTheParticipantsCooperation()
    {
        // The hub's clock never moves, so no idle or silence judgement can be what ends the streams.
        using var host = await CreateHostWithTransferAsync(new FakeTimeProvider());
        using var client = host.GetTestClient();
        var epoch = Guid.NewGuid();

        using var read = await ReceiveAsync(client).WaitAsync(Bound);
        Assert.Equal(HttpStatusCode.OK, read.StatusCode);
        await using var received = await read.Content.ReadAsStreamAsync();

        byte[] payload = [1, 2, 3, 4];
        using var written = await SegmentAsync(client, epoch, payload).WaitAsync(Bound);
        Assert.Equal(HttpStatusCode.OK, written.StatusCode);
        var buffer = new byte[payload.Length];
        await received.ReadExactlyAsync(buffer).AsTask().WaitAsync(Bound);
        Assert.Equal(payload, buffer);

        await host.Services.GetRequiredService<ITransferTerminator>().TerminateAsync(TransferId);

        Assert.Equal(0, await received.ReadAsync(buffer).AsTask().WaitAsync(Bound));
        Assert.False(
            host.Services.GetRequiredService<UploadEpochs>().TryGet(BridgeManager.KeyFor(TransferId, ConnectionType.Data), epoch, out _),
            "the sender's write stream outlived the transfer");
    }

    private static async Task<IHost> CreateHostWithTransferAsync(TimeProvider time)
    {
        var host = AuthenticatedHubServerFactory.CreateHost(configureServices: services => services.AddSingleton(time));

        var store = host.Services.GetRequiredService<IStateStore>();
        await store.SaveClientAsync(new ClientRegistration(SenderId, "conn-sender", ["test"], null, null, DateTime.UtcNow, Subject: SenderSubject));
        await store.SaveClientAsync(new ClientRegistration(ReceiverId, "conn-receiver", ["test"], null, null, DateTime.UtcNow, Subject: ReceiverSubject));
        await host.Services.GetRequiredService<ITransferManager>().RegisterParticipantsAsync(TransferId, SenderId, ReceiverId);

        return host;
    }

    private static Task<HttpResponseMessage> ReceiveAsync(HttpClient client)
    {
        var request = new HttpRequestMessage(HttpMethod.Get, $"http://localhost{WireRoutes.StreamBase}/{TransferId}/receive");
        request.Headers.Add("X-Connection-Type", nameof(ConnectionType.Data));
        request.Headers.Add("Authorization", $"{AuthenticatedHubServerFactory.Scheme} {ReceiverSubject}");
        return client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead);
    }

    /// <summary>The first segment of a write stream that stays open: it is not the last.</summary>
    private static Task<HttpResponseMessage> SegmentAsync(HttpClient client, Guid epoch, byte[] payload)
    {
        var query = $"?{DataPlane.EpochParameter}={epoch}&{DataPlane.OffsetParameter}=0&{DataPlane.HeartbeatParameter}=15000";
        var request = new HttpRequestMessage(HttpMethod.Post, $"http://localhost{WireRoutes.StreamBase}/{TransferId}/send{query}")
        {
            Content = new ByteArrayContent(payload),
        };
        request.Headers.Add("X-Connection-Type", nameof(ConnectionType.Data));
        request.Headers.Add("Authorization", $"{AuthenticatedHubServerFactory.Scheme} {SenderSubject}");
        return client.SendAsync(request);
    }
}
