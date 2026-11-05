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

## Dumb Pipe Design

The hub never deserializes batch payloads:
1. Receives raw bytes from sender
2. Forwards directly to receiver
3. Forwards control messages (ACK) bidirectionally

## Dependencies

- `TransportR` - Core interfaces
- `Microsoft.AspNetCore.App` - ASP.NET Core framework
