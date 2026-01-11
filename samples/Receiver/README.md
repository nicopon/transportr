# Receiver

Sample TransportR receiver application that receives data from a sender and writes it to files.

## Purpose

Demonstrates how to use `SignalRDataClient<T>` as a data receiver to:
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

## Data Flow

1. Connect to hub (canReceive=true)
2. Register `OnTransferStarted` handler
3. Wait for sender to initiate transfer
4. Receive items via async enumerable
5. Write items to CSV file
6. Report final statistics
