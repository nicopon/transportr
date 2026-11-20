namespace TransportR.Client.Configuration;

/// <summary>
/// How a client seals its transfers end to end: always, and with a pre-shared key mixed into the keys
/// when a provider gives one. Both ends of a transfer are set alike.
/// </summary>
/// <param name="preSharedKey">
/// Called once per transfer, synchronously, with the peer's client id, for the 32 bytes both ends hold;
/// <c>null</c> for encryption alone. A provider that returns <c>null</c> or another length, or throws,
/// refuses the transfer: there is no falling back to encryption without the key. The array it returns
/// stays its own, neither kept nor erased.
/// </param>
internal sealed class EndToEndEncryption(Func<Guid, byte[]?>? preSharedKey = null)
{
    public Func<Guid, byte[]?>? PreSharedKey { get; } = preSharedKey;
}
