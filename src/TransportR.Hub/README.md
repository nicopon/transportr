# TransportR.Hub

Server-side hub implementation for TransportR, providing the control plane (requests and a stream of server-sent events) and the HTTP streaming data plane.

## Purpose

Provides the hub services that:
1. Register clients, keep them registered and serve their discovery
2. Orchestrate transfer handshakes between senders and receivers
3. Act as a "dumb pipe" for data transfer (no deserialization)
4. Send a sender to the instance holding its receiver when several hubs share a state store (affinity handoff)

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      DataHub Server                         │
├─────────────────────────────────────────────────────────────┤
│  ClientController (control plane)                           │
│  ├── GET /api/client/{id}/events (server-sent events)       │
│  ├── GET /api/client/{id}/receivers → List<ClientInfo>      │
│  ├── POST /api/client/{id}/transfers                        │
│  ├── POST /api/client/{id}/replies/{requestId}              │
│  ├── POST /api/client/{id}/presence/{connectionId}          │
│  ├── POST /api/client/{id}/transfers/{tid}/heartbeat        │
│  ├── POST /api/client/{id}/transfers/{tid}/ping             │
│  └── PUT /api/client/{id}/token                             │
│                                                             │
│  StreamController (data plane)                              │
│  ├── GET /api/stream/{id}/send (WebSocket upgrade)          │
│  ├── POST /api/stream/{id}/send (one segment per request)   │
│  ├── GET /api/stream/{id}/receive                           │
│  └── POST /api/stream/{id}/abort                            │
├─────────────────────────────────────────────────────────────┤
│  Services                                                   │
│  ├── IStateStore → InMemoryStateStore or RedisStateStore    │
│  ├── TransferManager                                        │
│  └── TransportRServer (dumb pipe bridge)                    │
└─────────────────────────────────────────────────────────────┘
```

A write stream arrives as a WebSocket (`GET` with an upgrade) or as segments (`POST`) on the same path ("Data Plane
Transport" in `TransportRArchitecture.md`); what that asks of a front end is under "Deploying the hub" below.

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
//     .UseAffinityHandoff();

var app = builder.Build();

// With the JWT default the host also configures JwtBearer authentication and calls UseAuthentication()
// and UseAuthorization(); MapDataHub() then arms authorization on both planes (see SecurityArchitecture.md).
app.MapDataHub();

app.Run();
```

## Deploying the hub

### Before production

- **Validate tokens.** `UseDevMode()` trusts whatever group a client claims. Without it the host configures JwtBearer
  authentication, and its clients carry tokens whose `transportr:group:<name>` scopes name their groups ("Scope
  Convention" in `SecurityArchitecture.md`).
- **Choose where the tokens come from.** An identity provider that issues client-credentials tokens is one source.
  The other needs no provider: an issuer the host embeds exchanges a key, held by each client, for a short token. The
  hub validates that issuer's tokens like any others, so expiry, renewal and the access sweep work unchanged, and a
  client takes them with `WithClientCredentials`, its key as the client secret. The token carries the client's groups
  as scopes, or the host maps its subject to groups with its own `IClientIdentityProvider`. That issuer is a security
  component of the host: it exchanges keys over HTTPS only, stores them hashed, lets them expire and be revoked, and
  limits failed exchanges. [DtPipe](https://github.com/nicopon/dtpipe)'s coordinator issues its nodes' tokens this
  way. `TransportR.Auth.SimpleIdp` has the same shape for development only: its clients and secrets live in memory.
- **Register the group access rules** with `UseFlowControl`. Without an `IFlowControlService`, any client holding a
  group may open a transfer to any other ("Group Access Rules" in `SecurityArchitecture.md`).
- **Update the hub and its clients together.** They negotiate no protocol version; a release that needs one before the
  other says so in its notes.
- **Two hubs or more** need the Redis state store and effective session affinity at the load balancer ("High
  Availability" in `TransportRArchitecture.md`).

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
  keepalive frames go at the same interval; whether a front end counts them as traffic has not been measured.
- **For a WebSocket, relay the upgrade** (`Upgrade` and `Connection` on `GET /api/stream/{id}/send`) and the socket's
  bytes as they come, both ways. Where the reference proxy strips the two headers the hub answers `426` and the client
  uses segments at once; where it accepts the upgrade and relays nothing, the client gives up on the proof after 5 s.
  The route brings `UseWebSockets` with it, so the host's pipeline needs nothing.
- **With segments, a request body is bounded by the proxy's limit**: nginx's `client_max_body_size` is 1 MB by
  default, which a segment, 512 KB, fits.
- **HTTP/2.** TransportR clients ask for HTTP/1.1 on every request, an upgrade included; Kestrel may offer HTTP/2 to
  other clients. A reverse proxy that speaks HTTP/2 to the hub has not been validated, App Service's front end below
  aside, and what it spoke to the hub there was not recorded.
- **End-to-end encryption asks nothing of the hub.** A front end that terminates TLS then sees every batch sealed.
  Keep TLS all the same: the tokens, the control plane and the metadata rely on it.

The hub logs the transport of each write stream it takes on, and of each client's write-transport probe, and hands them
to `IHubProgressMonitor.OnWriteStreamOpened` and `OnWriteTransportProbed`: that is where to read what a client obtained
behind a given front end.

### Validated environments

- **Over a WebSocket**: Azure App Service (Linux, `http20Enabled=true`, `http20ProxyFlag=1`), with clients on Linux,
  macOS and Windows, one of them behind a proxy taken from the system's proxy settings. Kestrel keeps accepting HTTP/1.1 there: a container restricted to HTTP/2 failed App Service's health check
  under this flag. Also the reference proxy in `tests/TransportR.TestProxy` (YARP), which relays the upgrade and the
  affinity cookie, and Toxiproxy for the cuts.
- **In segments**: App Service (Linux, `sitecontainers`, `http20Enabled=false`, `http20ProxyFlag=0`), with a transfer
  of 200 items, one of 2 million and one of 460 s to a throttled receiver, each complete to the item with no 408 or
  499 on `/send`; the last had each end's streams cut and reopened twice, for a cause not established. Also a stock
  nginx (the `nginx-default` scenario, a `proxy_pass` that sets neither upgrade header).
- **Not run**: a stock nginx that relays the upgrade.

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
sender.OnSendRequested += send =>
{
    _ = SendOrdersAsync(send);   // or send here: the hub is answered once the handler returns, not once its task ends
    return Task.CompletedTask;
};

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

`TerminateAsync` removes the transfer's state and bridges and tells both clients to cancel, and their sessions fail with
`TransferFailedException` (`EndedByHub`). `AccessOptions.ClientsMayOpenTransfers = false` reserves opening transfers to the host: a client
is then refused `GetReceivers` and `InitTransfer`. With affinity handoff on, a transfer the host opens is refused unless
both clients' streams are on the instance that opens it ("Affinity Handoff" in `ResilienceArchitecture.md`).

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

// Clients connect through host.GetTestServer().CreateHandler(); clock.Advance(...) runs the hub's timeouts.
```

## Dumb Pipe Design

The hub never deserializes batch payloads:
1. Receives raw bytes from sender
2. Forwards directly to receiver
3. Forwards control messages (ACK) bidirectionally

## Dependencies

- `TransportR` - The contract shared with the client
- `Microsoft.AspNetCore.App` - ASP.NET Core framework
