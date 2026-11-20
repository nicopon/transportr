# TransportR - Apache Arrow Serialization

Moves Apache Arrow `RecordBatch` payloads over [TransportR](https://github.com/nicopon/TransportR),
as Arrow IPC.

## Installation

```bash
dotnet add package TransportR.Serialization.Arrow
```

## Usage

```csharp
var client = new TransportRClientBuilder<RecordBatch>()
    .WithUrl("http://localhost:5252")
    .WithArrowSerialization()
    .Build();
```

`WithArrowSerialization` exists only on a builder carrying `RecordBatch`: on another item type, the client does not
compile.

## Two things this package decides for you

**Every chunk is a complete IPC stream** — schema, batches, end-of-stream marker. The protocol
decodes chunks independently and replays them after a cut, so a chunk depending on a schema sent
once at the start would decode into nothing after the first cut. Repeating the schema is the price
of that independence, and it is small next to a batch.

**This serializer takes ownership of each `RecordBatch` you send, and disposes it once written.** A resend replays the
serialized bytes, never the batch. Batches you receive are yours, allocated through the `MemoryAllocator` you
pass in.

## Sizing the receive buffer — read this one

`WithReceiveCapacity` is unbounded by default, and the item dimension is close to meaningless
here: ten thousand record batches is not a memory figure, it is somewhere between a few megabytes
and a few hundred gigabytes. **Bound the bytes.**

```csharp
.WithReceiveCapacity(maxBytes: 256 * 1024 * 1024)
```

Two things to know about that number:

- It counts **wire** bytes — the IPC payload as it crossed the network, not the decoded buffers.
  With the default LZ4 compression the decoded batches are larger than the budget by whatever the
  codec saved; construct the serializer with `compress: false` and the two are close.
- A chunk larger than the whole budget is still admitted into an empty buffer, so a batch bigger
  than you planned for throttles the transfer rather than stalling it.

Bound the sender too, with `WithMaxInFlightBatches` (in batches, unbounded by default): with the receiver bounded
alone, serialized batches pile up in the sender ("Backpressure" in `ResilienceArchitecture.md`).
