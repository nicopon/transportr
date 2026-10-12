# TransportR.Hub.SignalR

Server-side hub implementation for TransportR, providing SignalR control plane and HTTP streaming data plane.

## Purpose

Provides the hub services that:
1. Manage client connections and discovery
2. Orchestrate transfer handshakes between senders and receivers
3. Act as a "dumb pipe" for data transfer (no deserialization)
4. Route clients to data plane endpoints (HA-ready)

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
│  Services (via interfaces for testability)                  │
│  ├── IStateStore → InMemory/Redis/TestProxy                 │
│  ├── ITransferManager → TransferManager                     │
│  └── SignalRDataServer (dumb pipe bridge)                   │
└─────────────────────────────────────────────────────────────┘
```

## Integration

```csharp
var builder = WebApplication.CreateBuilder(args);

// The hub forwards opaque chunks and never deserializes a payload, so it registers no
// serializer: that is a client-side decision between the two ends of a transfer.
builder.Services.AddDataHub();

// For HA deployment (Redis + Affinity):
// builder.Services.AddDataHub()
//     .UseRedisStore("localhost:6379")
//     .UseAffinityHandoff();

var app = builder.Build();

// 3. Map endpoints
app.MapDataHub();

app.Run();
```

## Testability

Hub components take their collaborators through interfaces and a `TimeProvider`, so a test can
build one by hand and move the clock instead of waiting:

```csharp
var clock = new FakeTimeProvider();
var stateStore = new InMemoryStateStore(timeProvider: clock);
var transferManager = new TransferManager(stateStore, clock);
var policy = new AccessPolicy(Options.Create(new AccessOptions()), clock);   // IFlowControlService is optional

var hub = new CommandHub(
    stateStore,
    transferManager,
    identityProvider,        // IClientIdentityProvider
    bridgeManager,           // IBridgeManager
    initiator,               // SignalRTransferInitiator
    policy,
    new ConnectionRegistry(),
    Options.Create(new AffinityOptions()),
    logger,                  // ILogger<CommandHub>
    clock);
```

## Dumb Pipe Design

The hub never deserializes batch payloads:
1. Receives raw bytes from sender
2. Forwards directly to receiver
3. Forwards control messages (ACK) bidirectionally

## Dependencies

- `TransportR` - Core interfaces
- `Microsoft.AspNetCore.App` - ASP.NET Core framework
