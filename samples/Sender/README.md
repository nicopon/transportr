# Sender

Sample TransportR sender application that reads data and sends it to a receiver through the hub.

## Purpose

Demonstrates how to use `SignalRDataClient<T>` as a data sender to:
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
| `Sender__BatchSize` | 5000 | Items per batch |
| `Sender__BatchTimeoutMs` | 500 | Batch flush timeout |
| `Sender__SourceFilePath` | data/orders.bin | Data source file |

## Data Flow

1. Read orders from binary file
2. Connect to hub (`ConnectAsync`, group `senders` by default)
3. Wait for receiver to connect
4. Initiate transfer
5. Stream items through batches
6. Wait for all ACKs
7. Send completion signal
