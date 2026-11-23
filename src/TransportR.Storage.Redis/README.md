# TransportR.Storage.Redis

Redis state store implementation for TransportR High Availability deployments.

## Purpose

Provides `RedisStateStore`, an `IStateStore` implementation that stores client registrations and transfer sessions in Redis, so that several DataHub instances can share them.

## Usage

```csharp
// Connection string
builder.Services.AddDataHub()
    .UseRedisStore("localhost:6379")
    .UseAffinityHandoff();

// Existing StackExchange.Redis IConnectionMultiplexer in DI (e.g. .NET Aspire or shared cache),
// with an isolated key prefix:
builder.Services.AddDataHub()
    .UseRedisStore(options => options.KeyPrefix = "myapp:")
    .UseAffinityHandoff();
```

`UseRedisStore` accepts a connection string, `ConfigurationOptions`, an `IConnectionMultiplexer` instance, or an
existing `IConnectionMultiplexer` in DI, each with an optional `Action<RedisStateStoreOptions>` to customize `KeyPrefix`
(default `"transportr:"`). The `connectionString` and `ConfigurationOptions` overloads keep their
`IConnectionMultiplexer` private to `RedisStateStore` (without registering it in the host's DI container) and dispose
it on shutdown.

## Features

- **Client registrations**: kept for `ConnectedTtlSeconds` (3600 s by default) while the client is connected, and for `DisconnectedGraceSeconds` (300 s by default) once its events stream ends. Each presence heartbeat pushes the expiry back.
- **Transfer sessions**: expire at the session's `ExpiresAtUtc`; heartbeats extend them.
- **Secondary index**: connection id → client id, with the same expiry as the registration.
- **Redis Cluster compatible**: uses only single-key commands and pipelined reads (no multi-key `MGET` / `CROSSSLOT`).

## Key Structure

| Key | Purpose | Expiry |
|-----|---------|--------|
| `transportr:clients:{clientId}` | Client registration | connected TTL, or the grace period once disconnected |
| `transportr:conn:{connectionId}` | ConnectionId → ClientId index | same as the registration |
| `transportr:client_ids` | Set of known client ids, read by discovery | none; members whose registration expired are removed on read |
| `transportr:sessions:{transferId}` | Transfer session (HA routing) | session `ExpiresAtUtc` |

## Requirements

- Tested against Redis 7.0: the test suite runs on a `redis:7.0` container.
- Sticky sessions at the load balancer, because the per-transfer progress counters are not stored here ("High Availability" in
  `TransportRArchitecture.md`).

## Dependencies

- `TransportR.Hub` — DataHubBuilder
- `StackExchange.Redis` — Redis client
