namespace TransportR.Client;

/// <summary>How a transfer's batches cross the hub.</summary>
public enum TransferProtection
{
    /// <summary>In clear: the hub, and whoever terminates TLS on the way, can read them.</summary>
    None,

    /// <summary>
    /// Sealed end to end, without a pre-shared key: protected against whoever reads the path, not against a hub that
    /// rewrites it.
    /// </summary>
    Encrypted,

    /// <summary>Sealed end to end, with a pre-shared key: a hub that rewrites the streams makes the transfer fail.</summary>
    EncryptedWithPreSharedKey,
}
