# TransportR - MessagePack Serialization

Carries batches of any POCO over [TransportR](https://github.com/nicopon/TransportR) as MessagePack,
LZ4-compressed. The recommended default serializer.

## Installation

```bash
dotnet add package TransportR.Serialization.MessagePack
```

## Usage

```csharp
var client = new TransportRClientBuilder<Order>()
    .WithUrl("http://localhost:5252")
    .WithMessagePackSerialization()
    .Build();
```

By default the serializer is contractless: public properties are serialized by name, and the type
needs no attribute. Pass `useAnnotations: true` for types annotated with `[MessagePackObject]` and
`[Key(n)]`, which put integer keys on the wire instead of property names (about 11 % smaller on the
benchmark's orders, after compression).

Both ends of a transfer must use the same serializer and settings: a batch the receiver cannot
decode fails the transfer.
