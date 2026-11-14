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

A write stream arrives as a WebSocket (`GET` with an upgrade) or as segments (`POST`) on the same path: whatever filters
requests by method in front of the hub must let both through. For a client to use a WebSocket it has to relay `Upgrade`
and `Connection` and the socket's bytes as they come, both ways; a front end that does not relay the upgrade, or holds
back the opening proof, makes the client use segments. The route brings `UseWebSockets` with it, so the host's pipeline
needs nothing. See "Data Plane Transport" in `TransportRArchitecture.md`. The hub logs the transport
of each write stream it takes on, and of each client's write-transport probe, and hands them to
`IHubProgressMonitor.OnWriteStreamOpened` and `OnWriteTransportProbed`: that is where to read what a client obtained.

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
client with no handler attached refuses the transfer, and the host's call fails.

```csharp
sender.OnSendRequested += send =>
{
    _ = SendOrdersAsync(send);   // the handler runs inside the handshake: hand the handle off and return
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
`TransferFailedException`. `AccessOptions.ClientsMayOpenTransfers = false` reserves opening transfers to the host: a client
is then refused `GetReceivers` and `InitTransfer`. With affinity handoff on, a transfer the host opens is refused unless
both clients' streams are on the instance that opens it ("Affinity Handoff" in `ResilienceArchitecture.md`).

A client that opens transfers itself is held to two ceilings of `AccessOptions`, on by default: `MaxHandshakesPerClient`
(32 handshakes at once on an instance) and `MaxOpenTransfersPerClient` (256 open transfers, counted through the state
store). Past either, its request is refused with a 429; `null` lifts a ceiling. Transfers the host opens through
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
