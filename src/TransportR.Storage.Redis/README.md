# TransportR.Storage.Redis

Redis state store implementation for TransportR High Availability deployments.

## Purpose

Provides `RedisStateStore`, an `IStateStore` implementation that stores client registrations and transfer checkpoints in Redis, enabling horizontal scaling of DataHub instances.

## Usage

```csharp
builder.Services.AddDataHub()
    .UseRedisStore("localhost:6379")
    .UseAffinityHandoff();
```

## Features

- **Client registrations**: Stored with 5-minute TTL (auto-renewed on activity)
- **Transfer checkpoints**: Stored with 24-hour TTL for session recovery
- **Secondary indexes**: O(1) lookup by connectionId

## Key Structure

| Key | Purpose | TTL |
|-----|---------|-----|
| `transportr:clients:{clientId}` | Client registration | 5 min |
| `transportr:conn:{connectionId}` | ConnectionId → ClientId index | 5 min |
| `transportr:checkpoints:{transferId}` | Transfer checkpoint | 24 h |

## Requirements

- Redis 6.0+
- Sticky sessions at load balancer level (for SignalR connections)

## Dependencies

- `TransportR.Hub.SignalR` — DataHubBuilder
- `StackExchange.Redis` — Redis client
