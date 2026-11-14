using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using TransportR.Hub;
using TransportR.Hub.Interfaces;
using TransportR.Tests.Infrastructure;

namespace TransportR.Tests;

/// <summary>
/// A stream request for a transfer the hub no longer knows is what a client does after the hub
/// retired the transfer, so it says nothing about the caller's right to be there. A request from
/// someone who is not the participant does, and keeps its Warning.
/// </summary>
public class UnknownTransferLogTests
{
    private const string TransferId = "transfer-1";
    private const string SenderSubject = "sender-principal";
    private const string ReceiverSubject = "receiver-principal";

    private static IHost CreateHost(LogRecorder logs) =>
        AuthenticatedHubServerFactory.CreateHost(configureServices: services =>
            services.AddLogging(logging => logging.AddProvider(logs).SetMinimumLevel(LogLevel.Trace)));

    private static Task<HttpResponseMessage> GetReceiveAsync(HttpClient client, string transferId, string subject)
    {
        var request = new HttpRequestMessage(HttpMethod.Get, $"http://localhost{WireRoutes.StreamBase}/{transferId}/receive");
        request.Headers.Add("X-Connection-Type", nameof(ConnectionType.Data));
        request.Headers.Add("Authorization", $"{AuthenticatedHubServerFactory.Scheme} {subject}");
        return client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead);
    }

    [Fact]
    public async Task ReopeningAnUnknownTransfer_IsNotLoggedAsAWarning()
    {
        var logs = new LogRecorder();
        using var host = CreateHost(logs);
        using var client = host.GetTestClient();

        using var response = await GetReceiveAsync(client, "retired-transfer", ReceiverSubject);

        Assert.Equal(System.Net.HttpStatusCode.NotFound, response.StatusCode);
        Assert.False(logs.Has(LogLevel.Warning, "retired-transfer"), "An unknown transfer is not an authorization failure.");
        Assert.True(logs.Has(LogLevel.Information, "retired-transfer"), "The refusal is still said, one level down.");
    }

    [Fact]
    public async Task ACallerThatIsNotTheParticipant_StillGetsTheWarning()
    {
        var logs = new LogRecorder();
        using var host = CreateHost(logs);
        using var client = host.GetTestClient();

        var store = host.Services.GetRequiredService<IStateStore>();
        var senderId = Guid.NewGuid();
        var receiverId = Guid.NewGuid();
        await store.SaveClientAsync(new ClientRegistration(senderId, "conn-sender", ["test"], null, null, DateTime.UtcNow, Subject: SenderSubject));
        await store.SaveClientAsync(new ClientRegistration(receiverId, "conn-receiver", ["test"], null, null, DateTime.UtcNow, Subject: ReceiverSubject));
        await host.Services.GetRequiredService<ITransferManager>().RegisterParticipantsAsync(TransferId, senderId, receiverId);

        using var response = await GetReceiveAsync(client, TransferId, "someone-else");

        Assert.Equal(System.Net.HttpStatusCode.NotFound, response.StatusCode);
        Assert.True(logs.Has(LogLevel.Warning, "Stream access denied"));
    }
}
