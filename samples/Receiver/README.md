# Receiver

Sample TransportR receiver application: it uses `TransportRClient<T>` as a data receiver, taking data from a sender through the hub and writing it to files.

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
| `Receiver__ByteStreamBatchBytes` | unset (items) | Byte-stream mode: each transfer is written as it arrives to `orders_<transferId>.bin`. The figure is the sender's batch budget, which this end only needs to enter the mode. `SERIALIZER_TYPE` and `Receiver__BatchSize` are not read; `Receiver__ReceiveMaxItems` and `Receiver__ConsumeItemsPerSecond` are refused |
| `Receiver__ConsumeItemsPerSecond` | 0 (no pacing) | Throttles the consumer |
| `Receiver__Group` | receivers | Group announced in dev mode |
| `Receiver__EnableAuth`, `Receiver__Auth__*` | off | Client credentials (`TokenEndpoint`, `ClientId`, `ClientSecret`, `Scope`) |
| `Receiver__EndToEndEncryption` | false | Seals every batch end to end; the sender must set it alike |
| `Receiver__EndToEndPreSharedKey` | unset | With the encryption on: 32 bytes in base64, mixed into the keys; the same on the sender. Never printed (the start-up log says `set` or `not set`) |
| `SERIALIZER_TYPE` | MessagePack | `MessagePack` or `Json`; the sender must use the same |

## Data Flow

1. Connect to hub (`ConnectAsync`, group `receivers` by default)
2. Register `OnTransferStarted(...)` handler
3. Wait for sender to initiate transfer
4. Receive items via async enumerable and process them asynchronously
5. Write items to CSV file
6. Report final statistics
