using System.Net;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using TransportR.Interfaces;
using TransportR.Models;
using TransportR.Protocol;
using TransportR.Tests.Infrastructure;

namespace TransportR.Tests;

/// <summary>
/// The route a participant uses to end its transfer when the Abort it wrote cannot reach the wire:
/// behind data nobody reads, the stream that carries it is full, and a request of its own is the only
/// way left to tell the hub. It is an HTTP request, authorized as the data streams are, and it ends
/// the transfer the way a host terminating a run does.
/// </summary>
public class AbortRouteTests
{
    private const string TransferId = "transfer-1";
    private const string SenderSubject = "sender-principal";
    private const string ReceiverSubject = "receiver-principal";

    private static readonly Guid SenderId = Guid.NewGuid();
    private static readonly Guid ReceiverId = Guid.NewGuid();

    [Theory]
    [InlineData(SenderSubject)]
    [InlineData(ReceiverSubject)]
    public async Task AParticipant_EndsTheTransfer_AndBothAreTold(string subject)
    {
        var notifier = new RecordingNotifier();
        using var host = await CreateHostWithTransferAsync(notifier);
        using var client = host.GetTestClient();

        using var response = await AbortAsync(client, TransferId, subject);

        Assert.Equal(HttpStatusCode.NoContent, response.StatusCode);
        Assert.Null(await host.Services.GetRequiredService<ITransferManager>().GetParticipantsAsync(TransferId));
        Assert.Equal(["conn-sender", "conn-receiver"], notifier.NotifiedConnectionIds);
    }

    [Fact]
    public async Task AStranger_IsRefusedAsNotFound_AndTheTransferSurvives()
    {
        var notifier = new RecordingNotifier();
        using var host = await CreateHostWithTransferAsync(notifier);
        using var client = host.GetTestClient();

        using var response = await AbortAsync(client, TransferId, "someone-else");

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
        Assert.NotNull(await host.Services.GetRequiredService<ITransferManager>().GetParticipantsAsync(TransferId));
        Assert.Empty(notifier.NotifiedConnectionIds);
    }

    [Fact]
    public async Task AnUnknownTransfer_IsRefusedAsNotFound()
    {
        var notifier = new RecordingNotifier();
        using var host = await CreateHostWithTransferAsync(notifier);
        using var client = host.GetTestClient();

        using var response = await AbortAsync(client, "no-such-transfer", SenderSubject);

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
        Assert.Empty(notifier.NotifiedConnectionIds);
    }

    [Fact]
    public async Task AnAnonymousCaller_IsChallenged()
    {
        var notifier = new RecordingNotifier();
        using var host = await CreateHostWithTransferAsync(notifier);
        using var client = host.GetTestClient();

        using var response = await AbortAsync(client, TransferId, subject: null);

        Assert.Equal(HttpStatusCode.Unauthorized, response.StatusCode);
        Assert.NotNull(await host.Services.GetRequiredService<ITransferManager>().GetParticipantsAsync(TransferId));
    }

    [Fact]
    public async Task ATokenThatHoldsNoGroup_IsRefusedAsNotFound_EvenWithAParticipantsSubject()
    {
        // What keeps a person's token off the data plane: it carries no transportr:group scope, and a
        // token that grants nothing is refused before its subject is compared. The subject is the
        // sender's here, so the group is the only thing that differs from the token admitted below.
        var notifier = new RecordingNotifier();
        using var host = await CreateHostWithTransferAsync(notifier);
        using var client = host.GetTestClient();
        var expiry = DateTime.UtcNow.AddHours(1);

        using var refused = await AbortWithTokenAsync(client, StubJwt.Create(SenderSubject, expiry));

        Assert.Equal(HttpStatusCode.NotFound, refused.StatusCode);
        Assert.NotNull(await host.Services.GetRequiredService<ITransferManager>().GetParticipantsAsync(TransferId));
        Assert.Empty(notifier.NotifiedConnectionIds);

        using var admitted = await AbortWithTokenAsync(client, StubJwt.Create(SenderSubject, expiry, "test"));

        Assert.Equal(HttpStatusCode.NoContent, admitted.StatusCode);
    }

    [Fact]
    public async Task ATokenThatHoldsNoGroup_IsRefusedWithAReasonThatSaysSo()
    {
        // The comparison with the participants' subjects never runs for such a token, so a line that lists
        // what was expected, empty, sends the operator looking for a subject mismatch that did not happen.
        var hubLogs = new LogRecorder();
        using var host = await CreateHostWithTransferAsync(new RecordingNotifier(), hubLogs: hubLogs);
        using var client = host.GetTestClient();

        using var response = await AbortWithTokenAsync(client, StubJwt.Create(SenderSubject, DateTime.UtcNow.AddHours(1)));

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
        var refusal = Assert.Single(hubLogs.MessagesFrom(LogLevel.Warning), m => m.Contains("Stream access denied"));
        Assert.Contains("holds no group", refusal);
        Assert.DoesNotContain("expected", refusal);
    }

    [Fact]
    public async Task ATokenWithNoSubject_IsRefusedWithAReasonThatSaysSo()
    {
        var hubLogs = new LogRecorder();
        using var host = await CreateHostWithTransferAsync(new RecordingNotifier(), hubLogs: hubLogs);
        using var client = host.GetTestClient();

        static string Encode(string json) =>
            Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes(json)).TrimEnd('=').Replace('+', '-').Replace('/', '_');
        var expiry = new DateTimeOffset(DateTime.UtcNow.AddHours(1), TimeSpan.Zero).ToUnixTimeSeconds();
        var token = $"{Encode("{\"alg\":\"none\"}")}.{Encode($"{{\"exp\":{expiry},\"scope\":[\"transportr:group:test\"]}}")}.signature";

        using var response = await AbortWithTokenAsync(client, token);

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
        var refusal = Assert.Single(hubLogs.MessagesFrom(LogLevel.Warning), m => m.Contains("Stream access denied"));
        Assert.Contains("no attested identity", refusal);
        Assert.DoesNotContain("expected", refusal);
    }

    [Fact]
    public async Task AStranger_IsRefusedWithTheSubjectItWasComparedTo()
    {
        var hubLogs = new LogRecorder();
        using var host = await CreateHostWithTransferAsync(new RecordingNotifier(), hubLogs: hubLogs);
        using var client = host.GetTestClient();

        using var response = await AbortAsync(client, TransferId, "someone-else");

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
        var refusal = Assert.Single(hubLogs.MessagesFrom(LogLevel.Warning), m => m.Contains("Stream access denied"));
        Assert.Contains("'someone-else'", refusal);
        Assert.Contains($"expected '{SenderSubject}' or '{ReceiverSubject}'", refusal);
    }

    [Fact]
    public async Task AReplicaThatHoldsNoBridgeOfTheTransfer_RefusesIt_AndLeavesTheTransferAlone()
    {
        // Behind a proxy with affinity, a request that lost its cookie reaches a replica that knows the
        // transfer from the shared state but never carried its streams. Acting there would delete the
        // shared state and tell nobody, because the participants' connections are on the other replica.
        var notifier = new RecordingNotifier();
        using var host = await CreateHostWithTransferAsync(notifier, holdsTheBridges: false);
        using var client = host.GetTestClient();

        using var response = await AbortAsync(client, TransferId, SenderSubject);

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
        Assert.NotNull(await host.Services.GetRequiredService<ITransferManager>().GetParticipantsAsync(TransferId));
        Assert.Empty(notifier.NotifiedConnectionIds);
    }

    [Fact]
    public async Task ATransferThatEndedWell_CannotBeAborted()
    {
        // A sender whose Abort did reach the wire a moment after the timeout, or whose transfer was
        // retired for any other reason, meets a transfer the hub no longer knows.
        var notifier = new RecordingNotifier();
        using var host = await CreateHostWithTransferAsync(notifier);
        using var client = host.GetTestClient();

        await host.Services.GetRequiredService<ITransferRetirer>().RetireAsync(TransferId);

        using var response = await AbortAsync(client, TransferId, SenderSubject);

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
        Assert.Empty(notifier.NotifiedConnectionIds);
    }

    private static async Task<IHost> CreateHostWithTransferAsync(
        RecordingNotifier notifier, bool holdsTheBridges = true, LogRecorder? hubLogs = null)
    {
        var host = AuthenticatedHubServerFactory.CreateHost(configureServices: services =>
        {
            services.AddSingleton<ITransferNotifier>(notifier);
            if (hubLogs != null) services.AddLogging(logging => logging.AddProvider(hubLogs));
        });

        var store = host.Services.GetRequiredService<IStateStore>();
        await store.SaveClientAsync(new ClientRegistration(SenderId, "conn-sender", ["test"], null, null, DateTime.UtcNow, Subject: SenderSubject));
        await store.SaveClientAsync(new ClientRegistration(ReceiverId, "conn-receiver", ["test"], null, null, DateTime.UtcNow, Subject: ReceiverSubject));
        await host.Services.GetRequiredService<ITransferManager>().RegisterParticipantsAsync(TransferId, SenderId, ReceiverId);

        if (holdsTheBridges)
        {
            var bridges = host.Services.GetRequiredService<IBridgeManager>();
            bridges.GetOrCreate(TransferId, ConnectionType.Data);
            bridges.GetOrCreate(TransferId, ConnectionType.Control);
        }

        return host;
    }

    private static Task<HttpResponseMessage> AbortWithTokenAsync(HttpClient client, string token)
    {
        var request = new HttpRequestMessage(
            HttpMethod.Post, $"http://localhost{WireRoutes.StreamBase}/{TransferId}/{WireRoutes.AbortSegment}");
        request.Headers.Add("Authorization", $"Bearer {token}");
        return client.SendAsync(request);
    }

    private static Task<HttpResponseMessage> AbortAsync(HttpClient client, string transferId, string? subject)
    {
        var request = new HttpRequestMessage(
            HttpMethod.Post, $"http://localhost{WireRoutes.StreamBase}/{transferId}/{WireRoutes.AbortSegment}");

        if (subject != null)
        {
            request.Headers.Add("Authorization", $"{AuthenticatedHubServerFactory.Scheme} {subject}");
        }

        return client.SendAsync(request);
    }

    private sealed class RecordingNotifier : ITransferNotifier
    {
        private readonly List<string> _notified = [];

        public IReadOnlyList<string> NotifiedConnectionIds
        {
            get { lock (_notified) return [.. _notified]; }
        }

        public Task NotifyCancelTransferAsync(string connectionId, string transferId, CancellationToken cancellationToken = default)
        {
            lock (_notified) _notified.Add(connectionId);
            return Task.CompletedTask;
        }
    }
}
