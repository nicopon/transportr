# Receiver

Sample TransportR receiver application that receives data from a sender and writes it to files.

## Purpose

Demonstrates how to use `TransportRClient<T>` as a data receiver to:
1. Connect to a TransportR hub
2. Wait for incoming transfers
3. Process received items asynchronously
4. Write results to CSV files

## Usage

```bash
dotnet run
```

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `Receiver__HubUrl` | http://localhost:5252 | Hub URL |
| `Receiver__OutputDirectory` | output | Output directory |
| `Receiver__ReceiveMaxItems` | unbounded | Receive buffer bound, in items |
| `Receiver__ReceiveMaxBytes` | unbounded | Receive buffer bound, in wire bytes |
| `Receiver__ConsumeItemsPerSecond` | 0 (no pacing) | Throttles the consumer |
| `Receiver__Group` | receivers | Group announced in dev mode |
| `Receiver__EnableAuth`, `Receiver__Auth__*` | off | Client credentials (`TokenEndpoint`, `ClientId`, `ClientSecret`, `Scope`) |
| `SERIALIZER_TYPE` | MessagePack | `MessagePack`, `Protobuf` or `Json`; the sender must use the same |

## Data Flow

1. Connect to hub (`ConnectAsync`, group `receivers` by default)
2. Register `OnTransferStarted` handler
3. Wait for sender to initiate transfer
4. Receive items via async enumerable
5. Write items to CSV file
6. Report final statistics
