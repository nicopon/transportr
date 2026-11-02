# TransportR.Tests

> Unit and component tests for the TransportR solution.

## Purpose

Unit tests for the components, and component tests for the hub's state and access logic.
End-to-end transfers are covered in two places: `TransferIntegrationTests` carries one transfer in
process over `TestServer`, and the scenarios of `./scripts/run-benchmark.sh` run real processes
for faults, volume, HA and authentication.

---

## Running Tests

### Command Line

```bash
dotnet test tests/TransportR.Tests
```

### With Coverage

```bash
dotnet test --collect:"XPlat Code Coverage"
```

---

## Redis Tests

`RedisStateStoreTests` run on a Testcontainers `redis:7.0` when Docker answers. Otherwise they run on a
`redis-server` already on the machine (`tools/`, `PATH`, `/opt/homebrew/bin`), started on a free port per
test. They are skipped only when neither exists (`RedisServer.cs`).

---

## Test Coverage

| Component | Covers |
|-----------|--------|
| `BatchAccumulator` | Validation, flush by size and by timeout, batch IDs |
| `ReceiverState` | In-order, out-of-order and duplicate batches |
| `ChunkIO` | Binary header read and write, buffer management |
| `FlowControlService` | Group permissions, wildcards |
| `InMemoryStateStore` | Clients, connection index, disconnection grace, renewal, sessions |
| `TransferManager` | Participants, transfer statistics, state removal |
| `RedisStateStore` | Clients, connection index, TTLs, renewal, sessions |

---

## Dependencies

| Package | Usage |
|---------|-------|
| `xunit` | Test framework |
| `xunit.runner.visualstudio` | VS runner |
| `Microsoft.NET.Test.Sdk` | Test SDK |

---

## Notes

- For end-to-end runs with real processes, use `./scripts/run-benchmark.sh`.
