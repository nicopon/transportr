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
│                      DataHub Server                          │
├─────────────────────────────────────────────────────────────┤
│  CommandHub (SignalR)                                       │
│  ├── /hub/command                                           │
│  │   ├── Connect(clientId, groups)                          │
│  │   ├── GetReceivers() → List<ClientInfo>                  │
│  │   ├── InitTransfer(targetId, batchSize)                  │
│  │   └── Heartbeat(transferId, metrics)                     │
│                                                             │
│  StreamController (HTTP)                                    │
│  ├── POST /api/stream/{id}/send                             │
│  └── GET /api/stream/{id}/receive                           │
├─────────────────────────────────────────────────────────────┤
│  Services (via interfaces for testability)                  │
│  ├── IStateStore → InMemory/Redis/TestProxy                 │
│  ├── ITransferManager → TransferManager                     │
│  └── SignalRDataServer (dumb pipe bridge)                  │
└─────────────────────────────────────────────────────────────┘
```

## Structure

```
TransportR.Hub.SignalR/
├── Configuration/
│   ├── AffinityOptions.cs          # HA sticky session config
│   ├── DataPlaneConfiguration.cs   # Bridge timeout config

├── Controllers/
│   └── StreamController.cs         # HTTP streaming endpoints
├── Exceptions/
│   └── AffinityRedirectException.cs # HA redirect signal
├── Hubs/
│   └── CommandHub.cs               # SignalR hub (orchestration)
├── Models/
│   ├── BridgeState.cs              # Bridge state enum
│   └── ConnectionBridge.cs         # Stream bridge manager
├── Services/
│   ├── BridgeCleanupService.cs     # Background cleanup
│   ├── DevIdentityProvider.cs      # Dev mode identity
│   ├── InMemoryStateStore.cs       # Default state store
│   ├── JwtIdentityProvider.cs      # JWT identity extraction
│   ├── SignalRDataServer.cs        # Dumb pipe bridge
│   ├── TestProxyStateStore.cs      # HA test proxy store
│   └── TransferManager.cs          # Transfer state + checkpoints
└── TransportRHubExtensions.cs      # DI registration
```

## Integration

```csharp
var builder = WebApplication.CreateBuilder(args);

// 1. Register serializer (REQUIRED)
builder.Services.AddSingleton<IBatchSerializer, MessagePackBatchSerializer>();

// 2. Add TransportR services
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

Hub components use interfaces for easy mocking:

```csharp
// In tests
var stateStore = new Mock<IStateStore>();
var transferManager = new Mock<ITransferManager>();
var identityProvider = new Mock<IClientIdentityProvider>();
var affinityOptions = Options.Create(new AffinityOptions());
var logger = new Mock<ILogger<CommandHub>>();

var hub = new CommandHub(
    stateStore.Object,
    transferManager.Object,
    identityProvider.Object,
    affinityOptions,
    logger.Object);
```

## Dumb Pipe Design

The hub never deserializes batch payloads:
1. Receives raw bytes from sender
2. Forwards directly to receiver
3. Forwards control messages (ACK) bidirectionally

## Dependencies

- `TransportR` - Core interfaces
- `Microsoft.AspNetCore.App` - ASP.NET Core framework
- `OpenTelemetry` - Metrics support

