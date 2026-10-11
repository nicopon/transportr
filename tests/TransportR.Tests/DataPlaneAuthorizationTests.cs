using System.Net;
using TransportR.Protocol;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using TransportR.Interfaces;
using TransportR.Models;
using TransportR.Tests.Infrastructure;

namespace TransportR.Tests;

/// <summary>
/// The stream routes carry the whole payload, so "authenticated" and "is a participant" have to
/// hold on them and not only on the SignalR endpoint.
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
        // caller can only be observed as a pair. The sender posts nothing: a clean EOF ends the
        // bridge and both requests return.
        var receiver = SendAsync(client, HttpMethod.Get, "receive", ConnectionType.Data, ReceiverSubject);
        var sender = SendAsync(client, HttpMethod.Post, "send", ConnectionType.Data, SenderSubject);

        await AssertBothOkAsync(receiver, sender);
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

        // The receiver's POST ends on EOF, but nothing ends a control stream on its own — that is
        // what it is for. Cancelling the bridge is how it stops in production too.
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

    private static async Task<HttpResponseMessage> WithTimeoutAsync(Task<HttpResponseMessage> request)
    {
        var finished = await Task.WhenAny(request, Task.Delay(TimeSpan.FromSeconds(30)));
        Assert.Same(request, finished);
        return await request;
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
        string? receiverSubject = ReceiverSubject)
    {
        var host = AuthenticatedHubServerFactory.CreateHost();

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
        var request = new HttpRequestMessage(method, $"http://localhost{WireRoutes.StreamBase}/{TransferId}/{route}");
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
