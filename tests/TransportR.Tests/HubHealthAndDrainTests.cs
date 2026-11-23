using System.Net;
using System.Net.Http.Json;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using TransportR.Client;
using TransportR.Hub;
using TransportR.Hub.Health;
using TransportR.Hub.Services;
using TransportR.Protocol;
using TransportR.Serialization.MessagePack;
using TransportR.Tests.Infrastructure;

namespace TransportR.Tests;

public class HubHealthAndDrainTests
{
    [Fact]
    public async Task HealthCheck_IsHealthyNormally_AndUnhealthyWhenStoreFailsOrDraining()
    {
        var store = new ToggleFailingStore();
        var drain = new HubDrainState();
        var check = new DataHubHealthCheck(store, drain);

        var healthy = await check.CheckHealthAsync(new HealthCheckContext());
        Assert.Equal(HealthStatus.Healthy, healthy.Status);

        store.Fail = true;
        var failed = await check.CheckHealthAsync(new HealthCheckContext());
        Assert.Equal(HealthStatus.Unhealthy, failed.Status);

        store.Fail = false;
        drain.BeginDraining();
        var draining = await check.CheckHealthAsync(new HealthCheckContext());
        Assert.Equal(HealthStatus.Unhealthy, draining.Status);
    }

    [Fact]
    public async Task AddDataHubHealthCheck_IntegratesWithHealthCheckService()
    {
        using var host = new HostBuilder()
            .ConfigureWebHost(webBuilder => webBuilder
                .UseTestServer()
                .ConfigureServices(services =>
                {
                    services.AddDataHub().UseDevMode(o => o.AllowAnonymous = true);
                    services.AddHealthChecks().AddDataHub();
                })
                .Configure(app =>
                {
                    app.UseRouting();
                    app.UseEndpoints(endpoints =>
                    {
                        endpoints.MapDataHub(requireAuth: false);
                        endpoints.MapHealthChecks("/health/ready");
                    });
                }))
            .Start();

        var healthService = host.Services.GetRequiredService<HealthCheckService>();
        var report = await healthService.CheckHealthAsync();
        Assert.Equal(HealthStatus.Healthy, report.Status);
        Assert.True(report.Entries.ContainsKey("transportr_hub"));

        using var http = host.GetTestClient();
        using var okResp = await http.GetAsync("/health/ready");
        Assert.Equal(HttpStatusCode.OK, okResp.StatusCode);

        host.Services.GetRequiredService<HubDrainState>().BeginDraining();
        using var drainingResp = await http.GetAsync("/health/ready");
        Assert.Equal(HttpStatusCode.ServiceUnavailable, drainingResp.StatusCode);
    }

    [Fact]
    public async Task WhenDraining_ClientAndHostTransferInitiationAreRefused()
    {
        using var host = TestHubServerFactory.CreateHost();
        var handler = host.GetTestServer().CreateHandler();

        await using var sender = new TransportRClientBuilder<string>()
            .WithUrl("http://localhost")
            .WithHttpMessageHandler(handler)
            .WithWriteTransport(WriteTransportPolicy.SegmentsOnly)
            .WithMessagePackSerialization()
            .Build();
        await using var receiver = new TransportRClientBuilder<string>()
            .WithUrl("http://localhost")
            .WithHttpMessageHandler(handler)
            .WithWriteTransport(WriteTransportPolicy.SegmentsOnly)
            .WithMessagePackSerialization()
            .Build();

        await sender.ConnectAsync();
        await receiver.ConnectAsync();

        host.Services.GetRequiredService<HubDrainState>().BeginDraining();

        // Client-side InitTransferAsync translates 503 HubError to HubRequestFailureCategory.Refused.
        var clientEx = await Assert.ThrowsAsync<HubRequestFailedException>(() =>
            sender.InitTransferAsync(receiver.ClientId));
        Assert.Equal(HubRequestFailureCategory.Refused, clientEx.Category);

        // Raw HTTP InitTransfer returns 503 Service Unavailable.
        using var rawClient = host.GetTestClient();
        using var resp = await rawClient.PostAsJsonAsync(
            $"{WireRoutes.ClientBase}/{sender.ClientId}/{WireRoutes.TransfersSegment}",
            new InitTransferRequest(receiver.ClientId, 5000),
            ControlPlane.Json);
        Assert.Equal(HttpStatusCode.ServiceUnavailable, resp.StatusCode);

        // Host-side ITransferInitiator refuses with HubRequestFailureCategory.Refused.
        var initiator = host.Services.GetRequiredService<ITransferInitiator>();
        var ex = await Assert.ThrowsAsync<HubRequestFailedException>(() =>
            initiator.InitTransferAsync(sender.ClientId, receiver.ClientId, TimeSpan.FromSeconds(5)));
        Assert.Equal(HubRequestFailureCategory.Refused, ex.Category);
    }

    [Fact]
    public async Task HubDrainService_StopAsync_WaitsUntilBridgesClear_OrTimeoutExpires()
    {
        var drain = new HubDrainState();
        var bridges = new BridgeManager(NullLoggerFactory.Instance);
        var lifetime = new StubLifetime();

        var service = new HubDrainService(
            drain,
            bridges,
            Options.Create(new DataPlaneConfiguration { DrainTimeoutSeconds = 5 }),
            lifetime,
            TimeProvider.System,
            NullLogger<HubDrainService>.Instance);

        await service.StartAsync(CancellationToken.None);

        // Open a bridge and start stopping; stop must wait until we remove the bridge.
        bridges.GetOrCreate("t-drain", ConnectionType.Data);
        var stopTask = service.StopAsync(CancellationToken.None);

        Assert.True(drain.IsDraining);
        await Task.Delay(150);
        Assert.False(stopTask.IsCompleted);

        bridges.TryRemove(BridgeManager.KeyFor("t-drain", ConnectionType.Data), out _);
        await stopTask.WaitAsync(TimeSpan.FromSeconds(3));
    }

    [Fact]
    public async Task HubDrainService_StopAsync_ReturnsWhenDrainTimeoutElapsesWithOpenBridge()
    {
        var drain = new HubDrainState();
        var bridges = new BridgeManager(NullLoggerFactory.Instance);

        var service = new HubDrainService(
            drain,
            bridges,
            Options.Create(new DataPlaneConfiguration { DrainTimeoutSeconds = 1 }),
            new StubLifetime(),
            TimeProvider.System,
            NullLogger<HubDrainService>.Instance);

        bridges.GetOrCreate("t-stuck", ConnectionType.Data);
        await service.StopAsync(CancellationToken.None).WaitAsync(TimeSpan.FromSeconds(4));
        Assert.True(drain.IsDraining);
    }

    private sealed class ToggleFailingStore : IStateStore
    {
        public bool Fail { get; set; }

        public Task SaveClientAsync(ClientRegistration client) => Task.CompletedTask;
        public Task<ClientRegistration?> GetClientAsync(Guid clientId) =>
            Fail ? Task.FromException<ClientRegistration?>(new InvalidOperationException("store down"))
                 : Task.FromResult<ClientRegistration?>(null);
        public Task MarkDisconnectedAsync(string connectionId, DateTime whenUtc) => Task.CompletedTask;
        public Task<ClientRegistration?> GetClientByConnectionIdAsync(string connectionId) => Task.FromResult<ClientRegistration?>(null);
        public Task<ClientRegistration?> RenewClientAsync(Guid clientId, string? expectedSubject, IReadOnlySet<string> groups, DateTime? tokenExpiry) => Task.FromResult<ClientRegistration?>(null);
        public Task<IReadOnlyList<ClientRegistration>> GetAllClientsAsync() => Task.FromResult<IReadOnlyList<ClientRegistration>>([]);
        public Task DeleteClientAsync(Guid clientId) => Task.CompletedTask;
        public Task SaveSessionAsync(TransferSession session) => Task.CompletedTask;
        public Task<TransferSession?> GetSessionAsync(string transferId) => Task.FromResult<TransferSession?>(null);
        public Task<int> CountTransfersOpenedByAsync(Guid senderId) => Task.FromResult(0);
        public Task ExtendSessionExpiryAsync(string transferId, DateTime newExpiry) => Task.CompletedTask;
        public Task DeleteSessionAsync(string transferId) => Task.CompletedTask;
    }

    private sealed class StubLifetime : IHostApplicationLifetime
    {
        public CancellationToken ApplicationStarted => CancellationToken.None;
        public CancellationToken ApplicationStopping => CancellationToken.None;
        public CancellationToken ApplicationStopped => CancellationToken.None;
        public void StopApplication() { }
    }
}
