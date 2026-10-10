# TransportR.Tests

> Unit tests for the TransportR solution.

## Purpose

This project contains unit tests to validate the correct functioning of the solution components.

---

## Project Structure

```
TransportR.Tests/
├── BatchAccumulatorTests.cs       # Batch accumulation tests
├── ReceiverStateTests.cs          # Reordering logic tests
├── ChunkIOTests.cs                # Binary header IO tests
├── FlowControlServiceTests.cs     # Authorization tests
├── InMemoryStateStoreTests.cs     # State store tests (HA)
├── TransferManagerTests.cs        # Transfer management tests (HA)
├── RedisStateStoreTests.cs        # Redis integration (container, or a local redis-server)
├── RedisServer.cs                 # Picks the Redis; auto-skip when there is none
└── TransportR.Tests.csproj
```

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

## Test Coverage

| Component | Tests | Coverage |
|-----------|-------|----------|
| `BatchAccumulator` | 9 | Validation, flush by size/timeout, batch IDs |
| `ReceiverState` | 6 | In-order, out-of-order, duplicates |
| `ChunkIO` | 10 | Binary header read/write, buffer management |
| `FlowControlService` | 7 | Group permissions, wildcards |
| `InMemoryStateStore` | 9 | Clients, checkpoints, connection index |
| `TransferManager` | 8 | Participants, stats, checkpoints |
| `RedisStateStore` | 9 | Clients, checkpoints (container or local redis-server) |

> **Note:** Redis tests require Docker. They are automatically skipped if Docker is unavailable.

---

## Dependencies

| Package | Usage |
|---------|-------|
| `xunit` | Test framework |
| `xunit.runner.visualstudio` | VS runner |
| `Microsoft.NET.Test.Sdk` | Test SDK |

---

## Notes

- Integration tests (requiring Hub + Client) are not covered here.
- For end-to-end tests, use `./scripts/run-benchmark.sh`
