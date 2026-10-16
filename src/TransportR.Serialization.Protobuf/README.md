# TransportR - Protobuf Serialization

Carries batches over [TransportR](https://github.com/nicopon/TransportR) as Protocol Buffers
(protobuf-net), Zstd-compressed. The most compact payload of the TransportR serializers.

## Installation

```bash
dotnet add package TransportR.Serialization.Protobuf
```

## Usage

```csharp
var client = new TransportRClientBuilder<Order>()
    .WithUrl("http://localhost:5252")
    .WithProtobufSerialization()   // compressionLevel: Zstd level, -1 by default
    .Build();
```

The item type needs a protobuf-net contract (`[ProtoContract]`, `[ProtoMember(n)]`, or a configured
`RuntimeTypeModel`): protobuf-net refuses a plain POCO. For types without a contract, use
`TransportR.Serialization.MessagePack`.

Both ends of a transfer must use the same serializer and settings: a batch the receiver cannot
decode fails the transfer.
