# Sender

Sample TransportR sender application that reads data and sends it to a receiver through the hub.

## Purpose

Demonstrates how to use `TransportRClient<T>` as a data sender to:
1. Connect to a TransportR hub
2. Discover available receivers
3. Initiate a transfer
4. Send data in batches with automatic retry

## Usage

```bash
dotnet run
```

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `Sender__HubUrl` | http://localhost:5252 | Hub URL |
| `Sender__BatchSize` | 500 (`appsettings.json`) | Items per batch |
| `Sender__BatchTimeoutMs` | 500 | Batch flush timeout |
| `Sender__SourceFilePath` | data/orders.bin | Data source file |
| `Sender__Group` | senders | Group announced in dev mode |
| `Sender__MaxInFlightBatches` | unbounded | Send window, in batches |
| `SENDER__PARALLELISM` | 1 | Transfers opened in parallel, to the discovered receivers in turn |
| `Sender__TransferStaggerSeconds` | 0 | Seconds between the opening of one transfer and the next |
| `Sender__EnableAuth`, `Sender__Auth__*` | off | Client credentials (`TokenEndpoint`, `ClientId`, `ClientSecret`, `Scope`) |
| `SERIALIZER_TYPE` | MessagePack | `MessagePack`, `Protobuf` or `Json`; the receiver must use the same |

## Data Flow

1. Read orders from binary file
2. Connect to hub (`ConnectAsync`, group `senders` by default)
3. Wait for receiver to connect
4. Initiate transfer
5. Stream items through batches
6. Send the completion signal and wait for the receiver to acknowledge everything (`CompleteAsync`)
