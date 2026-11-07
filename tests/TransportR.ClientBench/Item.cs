namespace TransportR.ClientBench;

/// <summary>The item of the items mode. Public, because the serializer cannot build a formatter for a type it cannot see.</summary>
public sealed class Item
{
    public int Seq { get; set; }
    public string Label { get; set; } = "";
    public byte[] Payload { get; set; } = [];
}
