# Shared

The `Order` model (`Models/Order.cs`) the samples carry. It holds the MessagePack contract (`[MessagePackObject]`,
`[Key(n)]`); JSON needs none, so the samples switch serializer (`SERIALIZER_TYPE`) on the same type.
