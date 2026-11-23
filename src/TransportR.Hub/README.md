# TransportR.Hub

Server-side hub implementation for TransportR, providing the control plane (requests and a stream of server-sent events) and the HTTP streaming data plane.

## Purpose

Provides the hub services that:
1. Register clients, keep them registered and serve their discovery
2. Orchestrate transfer handshakes between senders and receivers
3. Act as a "dumb pipe" for data transfer (no deserialization)
4. Send a sender to the instance holding its receiver when several hubs share a state store (affinity handoff)

## Architecture

The control plane is served under `/api/client/{id}/…`, the data plane under `/api/stream/…`; every route is listed
under "Communication Channels" in `TransportRArchitecture.md`. A write stream arrives as a WebSocket (`GET` with an
upgrade) or as segments (`POST`) on the same path ("Data Plane Transport" there). What that asks of a front end is under
"Deploying the hub" below. The hub forwards opaque chunks and never reads a payload, so it registers no serializer.

## Integration

```csharp
var builder = WebApplication.CreateBuilder(args);

// The hub forwards opaque chunks and never deserializes a payload, so it registers no
// serializer: that is a client-side decision between the two ends of a transfer.
// Tokens are validated by default; UseDevMode() trusts the client's claims for development, and an
// IClientIdentityProvider the host registers, before or after this call, is the one that serves.
builder.Services.AddDataHub();

// For HA deployment (Redis + Affinity):
// builder.Services.AddDataHub()
//     .UseRedisStore("localhost:6379")
//     .UseAffinityHandoff()
//     .ConfigureDataPlane(o => o.TransferInactivityTimeoutSeconds = 60);
//     // Also available: ConfigureAccess(...), ConfigureClientRegistry(o => o.ExposeSubjectInDiscovery = true)

var app = builder.Build();

// With the JWT default the host also configures JwtBearer authentication and calls UseAuthentication()
// and UseAuthorization(); MapDataHub() then arms authorization on both planes (see SecurityArchitecture.md).
app.MapDataHub();
// Optional route prefix (for reverse proxies or multi-service hosts; clients use WithUrl("https://host/transportr")):
// app.MapDataHub("/transportr");

// Readiness health check (probes IStateStore and reports Unhealthy while draining):
// builder.Services.AddHealthChecks().AddDataHub();
// app.MapHealthChecks("/health/ready");

app.Run();
```

## Deploying the hub

### Before production

- **Validate tokens.** `UseDevMode()` trusts whatever group a client claims. Without it, the host configures JwtBearer
  authentication. Its clients carry tokens whose `transportr:group:<name>` scopes name their groups ("Scope
  Convention" in `SecurityArchitecture.md`).
- **Choose where the tokens come from**: an identity provider that issues client-credentials tokens, or an issuer the
  host embeds that exchanges a key each client holds for a short token (`WithClientCredentials`, the key as client
  secret). The hub validates both alike, so expiry, renewal and the access sweep are unchanged; the token's scopes name
  the groups, or the host maps its subject with its own `IClientIdentityProvider`. An embedded issuer is a security
  component: HTTPS only, keys stored hashed, expiring and revocable, failed exchanges limited.
  [DtPipe](https://github.com/nicopon/dtpipe)'s coordinator issues its nodes' tokens this way;
  `TransportR.Auth.SimpleIdp` has the same shape for development only (clients and secrets in memory).
- **Register the group access rules** with `UseFlowControl`. Without an `IFlowControlService`, any client holding a
  group may open a transfer to any other ("Group Access Rules" in `SecurityArchitecture.md`).
- **Update the hub and its clients together.** They negotiate no protocol version; a release that needs one before the
  other says so in its notes.
- **Two hubs or more** need the Redis state store and effective session affinity at the load balancer ("High
  Availability" in `TransportRArchitecture.md`).
- **Health checks and graceful drain.** Register `builder.Services.AddHealthChecks().AddDataHub()` and map a readiness
  endpoint: it probes `IStateStore` and reports the configured failure status (`Unhealthy` by default) as soon as the
  host enters shutdown. Entering shutdown (`ApplicationStopping`) always marks the hub draining and refuses new
  transfers (`503` with `TransportR-Refused: 1`); `DataPlaneConfiguration.DrainTimeoutSeconds` (0 by default) sets how
  long `StopAsync` waits for active transfers and bridges to clear. Raise `HostOptions.ShutdownTimeout` (30 s by
  default) whenever `DrainTimeoutSeconds` exceeds it, and keep `HostOptions.ServicesStopConcurrently` disabled when
  Kestrel must stay up during the drain.
- **Discovery subject.** `ClientInfo.Subject` stays `null` by default; enable `ClientRegistryConfiguration.ExposeSubjectInDiscovery`
  when callers need to identify peers by their attested JWT subject during discovery.
- **Built-in metrics and tracing.** Subscribe an OpenTelemetry or `dotnet-counters` listener to
  `TransportRHubDiagnostics.MeterName` (`"TransportR.Hub"`) and `TransportRHubDiagnostics.ActivitySourceName`
  (`"TransportR.Hub"`): clients connected, active/ended transfers (by `transportr.end_reason`), bytes relayed, write
  streams opened, and `transportr.hub.init_transfer` spans.
- **Reference deployment templates.** `samples/DataHub/Dockerfile` builds a multi-stage `.NET 10` container image from
  the repository root (`docker build -f samples/DataHub/Dockerfile .`). `scripts/nginx/ws.conf.template` is the
  test-harness nginx config (`nginx-ws`, HTTP on port 80 targeting `host.docker.internal`) showing the WebSocket upgrade,
  `proxy_buffering off`, and timeout directives to adapt with an `upstream` block and TLS in production.

### In front of the hub

- **Let both paths and both methods through.** `/api/client/*` carries the events stream, every control request and
  the token renewal, and `/api/stream/*` the data; a write stream is a `GET` with an upgrade or a `POST` on the same
  path. With the renewal alone held back, every stream closes at its token's expiry.
- **Relay a long GET as it is written**, the events stream and every read stream, and answer short requests in full.
  The hub sends `X-Accel-Buffering: no`, which nginx honours, and sends the headers before it has a byte to relay.
- **Keep the idle limit above what the hub writes.** A GET must carry a byte, and a segment must be answered, before
  the proxy's idle limit: 60 s for a default nginx, 240 s for App Service. The events stream carries the hub's
  keepalive (*Key Timings* in `ResilienceArchitecture.md`). A sender with nothing to write sends a `Ping` every presence
  interval (15 s): a byte on the receiver's data GET, and its `Pong` one on the sender's control GET. A WebSocket's
  keepalive frames go at the same interval.
- **For a WebSocket, relay the upgrade** (`Upgrade` and `Connection` on `GET /api/stream/{id}/send`) and the socket's
  bytes as they come, both ways. Where the reference proxy strips the two headers the hub answers `426` and the client
  uses segments at once. Where it accepts the upgrade and relays nothing, the client gives up on the proof after 5 s.
  The route brings `UseWebSockets` with it, so the host's pipeline needs nothing.
- **With segments, a request body is bounded by the proxy's limit**: nginx's `client_max_body_size` is 1 MB by
  default, which a segment, 512 KB, fits.
- **HTTP/2.** TransportR clients ask for HTTP/1.1 on every request, an upgrade included; Kestrel may offer HTTP/2 to
  other clients. A reverse proxy that speaks HTTP/2 to the hub is not validated, App Service's front end aside.
- **End-to-end encryption asks nothing of the hub.** A front end that terminates TLS then sees every batch sealed.
  Keep TLS all the same: the tokens, the control plane and the metadata rely on it.

The hub logs the transport of each write stream it takes on and of each client's write-transport probe, and hands both
to `IHubProgressMonitor.OnWriteStreamOpened` and `OnWriteTransportProbed`. That is where to read what a client obtained
behind a given front end.

### Validated environments

| Front end | Write stream | Run |
|---|---|---|
| Azure App Service, Linux, `http20Enabled=true`, `http20ProxyFlag=1` | WebSocket | clients on Linux, macOS and Windows, one behind a proxy taken from the system's proxy settings. Kestrel must keep accepting HTTP/1.1: a container restricted to HTTP/2 failed App Service's health check under this flag |
| App Service, Linux, `sitecontainers`, `http20Enabled=false`, `http20ProxyFlag=0` | segments | transfers of 200 items, of 2 million, and of 460 s to a throttled receiver, each complete to the item, no 408 or 499 on `/send` |
| `tests/TransportR.TestProxy` (YARP) | WebSocket | relays the upgrade and the affinity cookie. Note: YARP does not flush outgoing request bodies per write by default, so carrying segments through YARP requires a flushing `IForwarderHttpClientFactory` (see `tests/TransportR.TestProxy/Program.cs`) |
| Toxiproxy | WebSocket | the cut scenarios |
| stock nginx, a `proxy_pass` that sets neither upgrade header (`nginx-default`) | segments | |
| stock nginx relaying the upgrade (`nginx-ws`, `scripts/nginx/ws.conf.template`) | WebSocket | 2 million items, both write streams over WebSocket |

Not measured or not recorded: whether a front end counts a WebSocket's keepalive frames as traffic, and which HTTP
version App Service's front end spoke to the hub. The 460 s transfer on App Service had each end's streams cut and
reopened twice, for a cause not established; it completed to the item.

## Transfers the host opens

A host that coordinates its own clients, over a channel of its own, can open each transfer itself instead of leaving it to
a sender. It resolves `ITransferInitiator` from the container and names both clients by their `ClientId`:

```csharp
var initiator = app.Services.GetRequiredService<ITransferInitiator>();
var transferId = await initiator.InitTransferAsync(senderId, receiverId, TimeSpan.FromSeconds(20));

// To end the run before the transfer completes, on the instance that carries it:
await app.Services.GetRequiredService<ITransferTerminator>().TerminateAsync(transferId);
```

The transfer goes through the authorization and the handshake a sender's own request does, and `InitTransferAsync`
returns once both clients hold their handles. The receiver sees an ordinary `OnTransferStarted`. The sender, a
`TransportRClient<T>` or a `ByteStreamClient`, is handed its handle through `OnSendRequested`, inside the handshake; a
client with no handler attached refuses the transfer, and the host's call fails. Under end-to-end encryption the host
still chooses the receiver, which binding the client ids does not prevent ("End-to-End Encryption" in
`SecurityArchitecture.md`).

```csharp
sender.OnSendRequested(send =>
{
    _ = SendOrdersAsync(send);   // or send here: the hub is answered once the handler returns, not once its task ends
    return Task.CompletedTask;
});

async Task SendOrdersAsync(Send<Order> send)
{
    await using (send)
    {
        foreach (var order in GetOrders())
            await send.SendAsync(order);
        await send.CompleteAsync();
    }
}
```

**HA constraint.** With `UseAffinityHandoff()` enabled, `ITransferInitiator.InitTransferAsync` refuses the transfer unless both clients' event streams are held by the same hub instance, and `ITransferTerminator.TerminateAsync` only cancels bridges held on the instance where it is called ("Affinity Handoff" in `ResilienceArchitecture.md`).

`TerminateAsync` removes the transfer's state and bridges and tells both clients to cancel, and their sessions fail with
`TransferFailedException` (`EndedByHub`). `AccessOptions.ClientsMayOpenTransfers = false` reserves opening transfers to the host: a client
is then refused `GetReceivers` and `InitTransfer`.

A client that opens transfers itself is held to two ceilings of `AccessOptions`, on by default: `MaxHandshakesPerClient`
(32 handshakes at once on an instance) and `MaxOpenTransfersPerClient` (256 open transfers, counted through the state
store). Past either, its request is refused with a 429, which the client raises as `RateLimited`; `null` lifts a ceiling. Transfers the host opens through
`ITransferInitiator` are not counted.

## Testability

Every hub-side clock and timer reads the `TimeProvider` in the container. A test registers its own
next to `AddDataHub()`, before or after it, and moves it instead of waiting. The hub and most of
its collaborators are internal, so a test gets the hub as production does and drives it through its
endpoints, over an ASP.NET Core `TestServer`:

```csharp
var clock = new FakeTimeProvider();                  // Microsoft.Extensions.TimeProvider.Testing

using var host = new HostBuilder()
    .ConfigureWebHost(web => web
        .UseTestServer()                             // Microsoft.AspNetCore.TestHost
        .ConfigureServices(services =>
        {
            services.AddSingleton<TimeProvider>(clock);
            services.AddDataHub().UseDevMode();
        })
        .Configure(app =>
        {
            app.UseRouting();
            app.UseEndpoints(endpoints => endpoints.MapDataHub());
        }))
    .Start();

// Pass host.GetTestServer().CreateHandler() via WithHttpMessageHandler with WriteTransportPolicy.SegmentsOnly
// (see the client README); clock.Advance(...) runs the hub's timeouts.
await using var sender = new TransportRClientBuilder<Order>()
    .WithUrl("http://localhost")
    .WithHttpMessageHandler(host.GetTestServer().CreateHandler())
    .WithWriteTransport(WriteTransportPolicy.SegmentsOnly)
    .WithMessagePackSerialization()
    .Build();
```

## Dependencies

- `TransportR` - The contract shared with the client
- `Microsoft.AspNetCore.App` - ASP.NET Core framework
