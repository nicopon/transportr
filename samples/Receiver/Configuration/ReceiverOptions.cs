using TransportR.Client;

namespace Receiver.Configuration;

public class ReceiverOptions
{
    public string HubUrl { get; set; } = "http://localhost:5252";
    public string OutputDirectory { get; set; } = "output";
    public bool EnableAuth { get; set; } = false;
    public int BatchSize { get; set; } = 5000;
    public string Group { get; set; } = "receivers";
    public string? AccessToken { get; set; } = null;

    /// <summary>
    /// Overrides the receive buffer's item bound. Null keeps the client default.
    /// </summary>
    public int? ReceiveMaxItems { get; set; }

    /// <summary>
    /// Selects the byte-stream mode: the transfer is written to a file as it arrives, with no serializer
    /// and no item. The figure is the sender's batch budget, which a receiver only needs to enter the
    /// mode. Null keeps the item mode.
    /// </summary>
    public int? ByteStreamBatchBytes { get; set; }

    /// <summary>
    /// Overrides the receive buffer's wire-byte bound. Null keeps the client default.
    /// </summary>
    public long? ReceiveMaxBytes { get; set; }

    /// <summary>
    /// What may carry the write stream (the acknowledgements). Null keeps the client default.
    /// </summary>
    public WriteTransportPolicy? WriteTransport { get; set; }

    /// <summary>
    /// Caps how fast items are consumed, standing in for a consumer that cannot keep up. Zero
    /// (the default) consumes as fast as it can.
    /// </summary>
    /// <remarks>
    /// Throttling the consumer rather than the source is the whole point: a slow source proves
    /// nothing about what the receive buffer holds, because the buffer never fills.
    /// </remarks>
    public int ConsumeItemsPerSecond { get; set; } = 0;

    /// <summary>
    /// Seals every batch end to end. Both ends of a transfer have to set it alike.
    /// </summary>
    public bool EndToEndEncryption { get; set; }

    /// <summary>
    /// The pre-shared key of the end-to-end encryption, 32 bytes in base64: one key for every peer. Null seals
    /// without a key. It comes from the node's own configuration and is never printed.
    /// </summary>
    public string? EndToEndPreSharedKey { get; set; }

    /// <summary>
    /// The decoded <see cref="EndToEndPreSharedKey"/>, or null when none is set. Throws, without quoting the
    /// key, for one that does not decode to 32 bytes, and for a key set with the encryption off.
    /// </summary>
    public byte[]? ResolveEndToEndPreSharedKey()
    {
        if (string.IsNullOrEmpty(EndToEndPreSharedKey)) return null;

        if (!EndToEndEncryption)
            throw new InvalidOperationException("EndToEndPreSharedKey is set while EndToEndEncryption is off.");

        byte[] key;
        try
        {
            key = Convert.FromBase64String(EndToEndPreSharedKey);
        }
        catch (FormatException)
        {
            throw new InvalidOperationException("EndToEndPreSharedKey is not valid base64.");
        }

        if (key.Length != 32)
            throw new InvalidOperationException("EndToEndPreSharedKey must decode to 32 bytes.");
        return key;
    }

    /// <summary>
    /// Auth configuration for automatic token acquisition.
    /// </summary>
    public AuthOptions? Auth { get; set; }
}

public class AuthOptions
{
    public string TokenEndpoint { get; set; } = string.Empty;
    public string ClientId { get; set; } = string.Empty;
    public string ClientSecret { get; set; } = string.Empty;
    public string? Scope { get; set; }

    public bool IsConfigured =>
        !string.IsNullOrEmpty(TokenEndpoint) &&
        !string.IsNullOrEmpty(ClientId) &&
        !string.IsNullOrEmpty(ClientSecret);
}
