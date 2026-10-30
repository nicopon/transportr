# TransportR.Hub.SignalR

Server-side hub implementation for TransportR, providing SignalR control plane and HTTP streaming data plane.

## Purpose

Provides the hub services that:
1. Manage client connections and discovery
2. Orchestrate transfer handshakes between senders and receivers
3. Act as a "dumb pipe" for data transfer (no deserialization)
4. Redirect a client to the instance holding its transfer when several hubs share a state store (affinity handoff)

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      DataHub Server                         │
├─────────────────────────────────────────────────────────────┤
│  CommandHub (SignalR)                                       │
│  ├── /hub/command                                           │
│  │   ├── Connect(clientId, groups)                          │
│  │   ├── GetReceivers() → List<ClientInfo>                  │
│  │   ├── InitTransfer(targetId, batchSize, timeoutMs)       │
│  │   └── Heartbeat() → bool                                 │
│                                                             │
│  StreamController (HTTP)                                    │
│  ├── POST /api/stream/{id}/send                             │
│  ├── GET /api/stream/{id}/receive                           │
│  └── POST /api/stream/{id}/abort                            │
│                                                             │
│  ClientController (HTTP)                                    │
│  └── PUT /api/client/{id}/token                             │
├─────────────────────────────────────────────────────────────┤
│  Services                                                   │
│  ├── IStateStore → InMemoryStateStore or RedisStateStore    │
│  ├── TransferManager                                        │
│  └── SignalRDataServer (dumb pipe bridge)                   │
└─────────────────────────────────────────────────────────────┘
```

## Integration

```csharp
var builder = WebApplication.CreateBuilder(args);

// The hub forwards opaque chunks and never deserializes a payload, so it registers no
// serializer: that is a client-side decision between the two ends of a transfer.
// Build() is required: it registers the identity provider (JWT by default, UseDevMode() for development).
builder.Services.AddDataHub().Build();

// For HA deployment (Redis + Affinity):
// builder.Services.AddDataHub()
//     .UseRedisStore("localhost:6379")
//     .UseAffinityHandoff()
//     .Build();

var app = builder.Build();

// With the JWT default the host also configures JwtBearer authentication, calls UseAuthentication()
// and UseAuthorization(), and maps with requireAuth: true (see SecurityArchitecture.md).
app.MapDataHub();

app.Run();
```

## Testability

Every hub-side clock and timer reads the `TimeProvider` in the container. A test registers its own
next to `AddDataHub()`, before or after it, and moves it instead of waiting. Most of the hub's
collaborators are internal, and `CommandHub` takes a `CommandHubDependencies` that only the container
builds, so a test gets the hub as production does and drives it through its endpoints, over an
ASP.NET Core `TestServer`:

```csharp
var clock = new FakeTimeProvider();                  // Microsoft.Extensions.TimeProvider.Testing

using var host = new HostBuilder()
    .ConfigureWebHost(web => web
        .UseTestServer()                             // Microsoft.AspNetCore.TestHost
        .ConfigureServices(services =>
        {
            services.AddSingleton<TimeProvider>(clock);
            services.AddDataHub().UseDevMode().Build();
        })
        .Configure(app =>
        {
            app.UseRouting();
            app.UseEndpoints(endpoints => endpoints.MapDataHub());
        }))
    .Start();

// Clients connect through host.GetTestServer().CreateHandler(); clock.Advance(...) runs the hub's timeouts.
```

A hub derived from `CommandHub` and registered with `UseHub<THub>()` receives `CommandHubDependencies`
from the container and passes it to `base` unopened.

## Dumb Pipe Design

The hub never deserializes batch payloads:
1. Receives raw bytes from sender
2. Forwards directly to receiver
3. Forwards control messages (ACK) bidirectionally

## Dependencies

- `TransportR` - Core interfaces
- `Microsoft.AspNetCore.App` - ASP.NET Core framework
