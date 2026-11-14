# TransportR.Storage.Redis

Redis state store implementation for TransportR High Availability deployments.

## Purpose

Provides `RedisStateStore`, an `IStateStore` implementation that stores client registrations and transfer sessions in Redis, so that several DataHub instances can share them.

## Usage

```csharp
builder.Services.AddDataHub()
    .UseRedisStore("localhost:6379")
    .UseAffinityHandoff();
```

## Features

- **Client registrations**: kept for `ConnectedTtlSeconds` (3600 s by default) while the client is connected, and for `DisconnectedGraceSeconds` (300 s by default) once its events stream ends. Each presence heartbeat pushes the expiry back.
- **Transfer sessions**: expire at the session's `ExpiresAtUtc`; heartbeats extend them.
- **Secondary index**: connection id → client id, with the same expiry as the registration.

## Key Structure

| Key | Purpose | Expiry |
|-----|---------|--------|
| `transportr:clients:{clientId}` | Client registration | connected TTL, or the grace period once disconnected |
| `transportr:conn:{connectionId}` | ConnectionId → ClientId index | same as the registration |
| `transportr:client_ids` | Set of known client ids, read by discovery | none; members whose registration expired are removed on read |
| `transportr:sessions:{transferId}` | Transfer session (HA routing) | session `ExpiresAtUtc` |

## Requirements

- Tested against Redis 7.0: the test suite runs on a `redis:7.0` container.
- Sticky sessions at the load balancer level (see "High Availability" in `TransportRArchitecture.md`).
- The per-transfer progress counters are not stored here; see "High Availability" in `TransportRArchitecture.md`.

## Dependencies

- `TransportR.Hub` — DataHubBuilder
- `StackExchange.Redis` — Redis client
