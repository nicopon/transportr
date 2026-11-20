using System.Buffers.Binary;
using System.Security.Cryptography;

namespace TransportR.Client.Protocol;

/// <summary>
/// One end's share of a transfer's end-to-end encryption: its ephemeral key pair, the keys it derives
/// once it has the peer's share, and what it seals, opens and signs with them. It reads and writes no
/// stream: the wire form it produces is the one "End-to-End Encryption" in
/// <c>TransportRArchitecture.md</c> states, and the session carries it.
/// </summary>
/// <remarks>
/// <para>
/// One per session, kept across its reconnections: every Ping carries the same share, and a peer whose
/// share changes in the course of a transfer is refused.
/// </para>
/// <para>
/// <b>A refusal is final.</b> Once a check fails, every <c>Try</c> and <c>Verify</c> method answers
/// <c>false</c> and every method that produces bytes throws, so a caller that let one refusal pass
/// cannot go on with keys the peer does not hold.
/// </para>
/// <para>
/// <b>Threading.</b> The peer's share is accepted on the read loop. The keys are published once, whole,
/// and from then on sealing (the send worker), receipts (the caller of <c>CompleteAsync</c>) and checks
/// (the read loop) may run at the same time. <see cref="Dispose"/> erases the keys they use. Receipts,
/// written and checked once per transfer, take the lock it takes, so that a release crossing one leaves
/// it refused rather than signed or checked under erased keys; sealing and opening do not, and rest on
/// <see cref="AesGcm"/>, which holds its key behind its handle until the call in flight returns.
/// </para>
/// </remarks>
internal sealed class EndToEndCipher : IDisposable
{
    /// <summary>An uncompressed P-256 point: <c>0x04</c>, then X and Y, 32 bytes each.</summary>
    public const int ShareSize = 1 + 2 * CoordinateSize;

    /// <summary>A full HMAC-SHA256 output, for the confirmation and for both receipts.</summary>
    public const int MacSize = 32;

    /// <summary>The AES-GCM tag that follows the ciphertext of a sealed batch.</summary>
    public const int TagSize = 16;

    public const int PreSharedKeySize = 32;
    public const int PingPayloadSize = ShareSize;
    public const int PongPayloadSize = ShareSize + MacSize;
    public const int ReceiptSize = MacSize;

    private const int CoordinateSize = 32;
    private const int KeySize = 32;
    private const int NonceSize = 12;
    private const byte UncompressedPoint = 0x04;
    private const byte WithoutPreSharedKey = 0x00;
    private const byte WithPreSharedKey = 0x01;
    private const int IdSize = 16;
    private const int ReceiptInputSize = IdSize + sizeof(int);

    // Length-prefixed in the transcript, so that no identifier of another length yields the same
    // bytes. Another suite, or another layout of the transcript, takes another identifier.
    private static ReadOnlySpan<byte> ProtocolId => "transportr e2e v1 P-256 HKDF-SHA256 AES-256-GCM"u8;

    private static int TranscriptSize => 1 + ProtocolId.Length + 1 + 3 * IdSize + 2 * ShareSize;

    private static ReadOnlySpan<byte> BatchLabel => "transportr e2e batch"u8;
    private static ReadOnlySpan<byte> ConfirmationLabel => "transportr e2e confirmation"u8;
    private static ReadOnlySpan<byte> SenderReceiptLabel => "transportr e2e sender receipt"u8;
    private static ReadOnlySpan<byte> ReceiverReceiptLabel => "transportr e2e receiver receipt"u8;

    private readonly object _gate = new();
    private readonly bool _isSender;
    private readonly Guid _transferId;
    private readonly Guid _senderId;
    private readonly Guid _receiverId;
    private readonly bool _usesPreSharedKey;
    private readonly byte[] _localShare;

    // Both go once the keys are derived: what is left in memory then cannot rebuild them.
    private ECDiffieHellman? _ephemeral;
    private byte[]? _preSharedKey;

    private byte[]? _peerShare;
    private Keys? _keys;

    // The sender's: what a Pong has to carry. The receiver's: the Pong it answers every Ping with.
    private byte[]? _expectedConfirmation;
    private byte[]? _pong;

    private volatile bool _failed;
    private volatile bool _disposed;

    /// <param name="ephemeral">
    /// This end's P-256 key pair, the cipher's from here on, even when it throws. Private, and reached
    /// from outside by the known-answer test alone, through <c>UnsafeAccessor</c>, so that no assembly
    /// the client opens its internals to can inject one: a pair that is not fresh loses forward
    /// secrecy, and the same pairs on two sessions of one transfer repeat nonces under one key.
    /// </param>
    private EndToEndCipher(
        bool isSender, Guid transferId, Guid senderId, Guid receiverId, byte[]? preSharedKey, ECDiffieHellman ephemeral)
    {
        try
        {
            if (preSharedKey is not null && preSharedKey.Length != PreSharedKeySize)
            {
                throw new ArgumentException($"A pre-shared key is {PreSharedKeySize} bytes.", nameof(preSharedKey));
            }

            var point = ephemeral.ExportParameters(includePrivateParameters: false).Q;
            if (point.X is not { Length: CoordinateSize } || point.Y is not { Length: CoordinateSize })
            {
                throw new CryptographicException("The platform exported a P-256 public key of an unexpected size.");
            }

            _localShare = new byte[ShareSize];
            _localShare[0] = UncompressedPoint;
            point.X.CopyTo(_localShare, 1);
            point.Y.CopyTo(_localShare, 1 + CoordinateSize);
        }
        catch
        {
            ephemeral.Dispose();
            throw;
        }

        _ephemeral = ephemeral;
        _isSender = isSender;
        _transferId = transferId;
        _senderId = senderId;
        _receiverId = receiverId;
        _usesPreSharedKey = preSharedKey is not null;

        // Last, so that no failure above leaves a copy nobody erases. Copied, and the caller's array
        // left alone: a provider may hand the same array to every transfer. Pinned, so that no
        // compaction leaves a copy of it behind.
        if (preSharedKey is not null)
        {
            _preSharedKey = GC.AllocateArray<byte>(PreSharedKeySize, pinned: true);
            preSharedKey.CopyTo(_preSharedKey, 0);
        }
    }

    /// <param name="senderId">
    /// The sending client's id as this end knows it: its own at the sender, the one the hub announces
    /// at the receiver.
    /// </param>
    /// <param name="receiverId">
    /// The receiving client's id as this end knows it: the target named or announced at the sender,
    /// its own at the receiver. Both ids enter every key, so a hub that names another peer to either
    /// end fails the confirmation.
    /// </param>
    /// <param name="preSharedKey">
    /// <c>null</c> for encryption alone, or exactly <see cref="PreSharedKeySize"/> bytes. Copied; the
    /// array passed is neither kept nor erased.
    /// </param>
    public static EndToEndCipher ForSender(Guid transferId, Guid senderId, Guid receiverId, byte[]? preSharedKey)
        => new(isSender: true, transferId, senderId, receiverId, preSharedKey, NewEphemeral());

    /// <inheritdoc cref="ForSender"/>
    public static EndToEndCipher ForReceiver(Guid transferId, Guid senderId, Guid receiverId, byte[]? preSharedKey)
        => new(isSender: false, transferId, senderId, receiverId, preSharedKey, NewEphemeral());

    public bool UsesPreSharedKey => _usesPreSharedKey;

    /// <summary>
    /// Whether batches can be sealed or opened: the receiver has accepted a Ping, the sender a Pong
    /// whose confirmation holds, and no check has failed since.
    /// </summary>
    public bool HasKeys => !_disposed && !_failed && Volatile.Read(ref _keys) is not null;

    /// <summary>Writes the sender's share, the payload of every Ping.</summary>
    public void WritePing(Span<byte> payload)
    {
        EnsureProducing(sender: true);
        RequireLength(payload.Length, PingPayloadSize, nameof(payload));
        _localShare.CopyTo(payload);
    }

    /// <summary>
    /// Takes the sender's share from a Ping. The first derives the keys; each one after has to carry
    /// the same share.
    /// </summary>
    public bool TryAcceptPing(ReadOnlySpan<byte> payload)
    {
        lock (_gate)
        {
            EnsureChecking(sender: false);
            if (_failed) return false;
            if (payload.Length != PingPayloadSize) return Refuse();

            if (_peerShare is { } accepted)
            {
                return CryptographicOperations.FixedTimeEquals(accepted, payload) || Refuse();
            }

            if (Derive(payload) is not { } derived) return Refuse();
            var (keys, confirmation) = derived;

            _peerShare = payload.ToArray();
            _pong = [.. _localShare, .. confirmation];
            Volatile.Write(ref _keys, keys);
            return true;
        }
    }

    /// <summary>Writes the receiver's share and the key confirmation, the payload of every Pong.</summary>
    public void WritePong(Span<byte> payload)
    {
        EnsureProducing(sender: false);
        RequireLength(payload.Length, PongPayloadSize, nameof(payload));
        var pong = _pong ?? throw new InvalidOperationException("No Ping has been accepted: there is no Pong to write.");
        pong.CopyTo(payload);
    }

    /// <summary>
    /// Takes the receiver's share and the key confirmation from a Pong. The first derives the keys and
    /// publishes them only if the confirmation holds; each one after has to carry the same share and
    /// the same confirmation.
    /// </summary>
    public bool TryAcceptPong(ReadOnlySpan<byte> payload)
    {
        lock (_gate)
        {
            EnsureChecking(sender: true);
            if (_failed) return false;
            if (payload.Length != PongPayloadSize) return Refuse();

            var share = payload[..ShareSize];
            var confirmation = payload[ShareSize..];

            if (_peerShare is { } accepted)
            {
                return (CryptographicOperations.FixedTimeEquals(accepted, share)
                        && CryptographicOperations.FixedTimeEquals(_expectedConfirmation, confirmation))
                       || Refuse();
            }

            if (Derive(share) is not { } derived) return Refuse();
            var (keys, expected) = derived;

            if (!CryptographicOperations.FixedTimeEquals(expected, confirmation))
            {
                keys.Dispose();
                return Refuse();
            }

            _peerShare = share.ToArray();
            _expectedConfirmation = expected;
            Volatile.Write(ref _keys, keys);
            return true;
        }
    }

    /// <summary>
    /// Seals a batch under <paramref name="header"/>, the header it goes out with, which the AES-GCM
    /// tag covers byte for byte.
    /// </summary>
    /// <param name="header">
    /// Its <see cref="ChunkHeader.PayloadLength"/> is the sealed length, <paramref name="plaintext"/>'s
    /// plus <see cref="TagSize"/>.
    /// </param>
    /// <param name="sealedPayload">
    /// Exactly that length: the ciphertext, then the tag. It may start where
    /// <paramref name="plaintext"/> does, which seals in place.
    /// </param>
    public void Seal(in ChunkHeader header, ReadOnlySpan<byte> plaintext, Span<byte> sealedPayload)
    {
        var keys = EnsureProducing(sender: true)
                   ?? throw new InvalidOperationException("No confirmed Pong yet: there is no key to seal with.");

        // Batch ids count up from 0: a negative one has wrapped, and the ids after it would repeat
        // nonces under this key.
        ArgumentOutOfRangeException.ThrowIfNegative(header.BatchId, nameof(header));

        if (header.PayloadLength != plaintext.Length + TagSize)
        {
            throw new ArgumentException("The header does not announce the sealed length of the plaintext.", nameof(header));
        }

        RequireLength(sealedPayload.Length, header.PayloadLength, nameof(sealedPayload));
        RequireInPlaceOrApart(plaintext, sealedPayload, nameof(sealedPayload));

        Span<byte> nonce = stackalloc byte[NonceSize];
        WriteNonce(header.BatchId, nonce);
        Span<byte> associatedData = stackalloc byte[ChunkHeader.Size];
        header.WriteTo(associatedData);

        keys.Batch.Encrypt(nonce, plaintext, sealedPayload[..plaintext.Length], sealedPayload[plaintext.Length..], associatedData);
    }

    /// <summary>
    /// Opens a sealed batch read under <paramref name="header"/>. Refused when the tag does not hold
    /// for this header and payload, when the payload is shorter than a tag, or when no Ping has given
    /// the keys yet.
    /// </summary>
    /// <param name="sealedPayload">The payload as read, <see cref="ChunkHeader.PayloadLength"/> bytes.</param>
    /// <param name="plaintext">
    /// <see cref="TagSize"/> bytes shorter. It may start where <paramref name="sealedPayload"/> does,
    /// which opens in place.
    /// </param>
    public bool TryOpen(in ChunkHeader header, ReadOnlySpan<byte> sealedPayload, Span<byte> plaintext)
    {
        EnsureChecking(sender: false);
        RequireLength(sealedPayload.Length, header.PayloadLength, nameof(sealedPayload));
        if (_failed) return false;

        if (Volatile.Read(ref _keys) is not { } keys || sealedPayload.Length < TagSize) return Refuse();

        var length = sealedPayload.Length - TagSize;
        RequireLength(plaintext.Length, length, nameof(plaintext));
        RequireInPlaceOrApart(sealedPayload, plaintext, nameof(plaintext));

        Span<byte> nonce = stackalloc byte[NonceSize];
        WriteNonce(header.BatchId, nonce);
        Span<byte> associatedData = stackalloc byte[ChunkHeader.Size];
        header.WriteTo(associatedData);

        try
        {
            keys.Batch.Decrypt(nonce, sealedPayload[..length], sealedPayload[length..], plaintext, associatedData);
            return true;
        }
        catch (AuthenticationTagMismatchException)
        {
            return Refuse();
        }
    }

    /// <summary>
    /// Writes this end's receipt for <paramref name="lastBatchId"/>: the sender's goes in Complete, the
    /// receiver's in CompleteAck. Each end's is keyed apart, so neither passes for the other's.
    /// </summary>
    public void WriteReceipt(int lastBatchId, Span<byte> receipt)
    {
        lock (_gate)
        {
            var keys = EnsureProducing(sender: null)
                       ?? throw new InvalidOperationException("No keys yet: there is nothing to sign with.");
            RequireLength(receipt.Length, ReceiptSize, nameof(receipt));
            ComputeReceipt(_isSender ? keys.SenderReceipt : keys.ReceiverReceipt, lastBatchId, receipt);
        }
    }

    /// <summary>Checks the peer's receipt for <paramref name="lastBatchId"/>.</summary>
    public bool VerifyPeerReceipt(int lastBatchId, ReadOnlySpan<byte> receipt)
    {
        Span<byte> expected = stackalloc byte[ReceiptSize];
        lock (_gate)
        {
            EnsureChecking(sender: null);
            if (_failed) return false;
            if (Volatile.Read(ref _keys) is not { } keys || receipt.Length != ReceiptSize) return Refuse();

            ComputeReceipt(_isSender ? keys.ReceiverReceipt : keys.SenderReceipt, lastBatchId, expected);
            return CryptographicOperations.FixedTimeEquals(expected, receipt) || Refuse();
        }
    }

    public override string ToString()
    {
        var state = _disposed ? "disposed"
            : _failed ? "refused"
            : Volatile.Read(ref _keys) is not null ? "keys derived"
            : "awaiting the peer's share";
        return $"{nameof(EndToEndCipher)}({(_isSender ? "sender" : "receiver")}, "
               + $"{(_usesPreSharedKey ? "pre-shared key" : "no pre-shared key")}, {state})";
    }

    public void Dispose()
    {
        lock (_gate)
        {
            if (_disposed) return;
            _disposed = true;

            _ephemeral?.Dispose();
            _ephemeral = null;
            if (_preSharedKey is { } preSharedKey) CryptographicOperations.ZeroMemory(preSharedKey);
            _preSharedKey = null;
            Interlocked.Exchange(ref _keys, null)?.Dispose();
        }
    }

    /// <summary>
    /// ECDH with the peer's share, then HKDF: the PSK, or 32 zero bytes, as the salt; each key expanded
    /// from its label followed by the transcript. Erases the ephemeral private key and the PSK on the
    /// way out, whether or not the share was good: a refusal ends the transfer.
    /// </summary>
    /// <returns><c>null</c> when the share is not a point of the curve.</returns>
    private (Keys Keys, byte[] Confirmation)? Derive(ReadOnlySpan<byte> peerShare)
    {
        byte[]? secret = null;
        Span<byte> pseudoRandomKey = stackalloc byte[KeySize];
        Span<byte> batchKey = stackalloc byte[KeySize];
        Span<byte> confirmationKey = stackalloc byte[KeySize];
        try
        {
            using (var peer = ImportShare(peerShare))
            {
                if (peer is null) return null;
                try
                {
                    secret = _ephemeral!.DeriveRawSecretAgreement(peer.PublicKey);
                }
                catch (CryptographicException)
                {
                    return null;
                }
            }

            Span<byte> transcript = stackalloc byte[TranscriptSize];
            WriteTranscript(peerShare, transcript);

            Span<byte> noPreSharedKey = stackalloc byte[PreSharedKeySize];
            HKDF.Extract(HashAlgorithmName.SHA256, secret, _preSharedKey ?? noPreSharedKey, pseudoRandomKey);

            Expand(pseudoRandomKey, BatchLabel, transcript, batchKey);
            Expand(pseudoRandomKey, ConfirmationLabel, transcript, confirmationKey);
            var senderReceipt = GC.AllocateArray<byte>(KeySize, pinned: true);
            Expand(pseudoRandomKey, SenderReceiptLabel, transcript, senderReceipt);
            var receiverReceipt = GC.AllocateArray<byte>(KeySize, pinned: true);
            Expand(pseudoRandomKey, ReceiverReceiptLabel, transcript, receiverReceipt);

            var confirmation = new byte[MacSize];
            HMACSHA256.HashData(confirmationKey, transcript, confirmation);

            return (new Keys(new AesGcm(batchKey, TagSize), senderReceipt, receiverReceipt), confirmation);
        }
        finally
        {
            if (secret is not null) CryptographicOperations.ZeroMemory(secret);
            CryptographicOperations.ZeroMemory(pseudoRandomKey);
            CryptographicOperations.ZeroMemory(batchKey);
            CryptographicOperations.ZeroMemory(confirmationKey);
            _ephemeral?.Dispose();
            _ephemeral = null;
            if (_preSharedKey is { } preSharedKey) CryptographicOperations.ZeroMemory(preSharedKey);
            _preSharedKey = null;
        }
    }

    private void WriteTranscript(ReadOnlySpan<byte> peerShare, Span<byte> transcript)
    {
        ReadOnlySpan<byte> senderShare = _isSender ? _localShare : peerShare;
        ReadOnlySpan<byte> receiverShare = _isSender ? peerShare : _localShare;

        transcript[0] = (byte)ProtocolId.Length;
        ProtocolId.CopyTo(transcript[1..]);
        var rest = transcript[(1 + ProtocolId.Length)..];
        rest[0] = _usesPreSharedKey ? WithPreSharedKey : WithoutPreSharedKey;
        _transferId.TryWriteBytes(rest.Slice(1, IdSize));
        _senderId.TryWriteBytes(rest.Slice(1 + IdSize, IdSize));
        _receiverId.TryWriteBytes(rest.Slice(1 + 2 * IdSize, IdSize));
        senderShare.CopyTo(rest.Slice(1 + 3 * IdSize, ShareSize));
        receiverShare.CopyTo(rest.Slice(1 + 3 * IdSize + ShareSize, ShareSize));
    }

    private static ECDiffieHellman NewEphemeral() => ECDiffieHellman.Create(ECCurve.NamedCurves.nistP256);

    private static ECDiffieHellman? ImportShare(ReadOnlySpan<byte> share)
    {
        if (share.Length != ShareSize || share[0] != UncompressedPoint) return null;
        try
        {
            // The import checks the point lies on the curve: a share off it would otherwise leak bits
            // of the private key through the secret derived from it.
            return ECDiffieHellman.Create(new ECParameters
            {
                Curve = ECCurve.NamedCurves.nistP256,
                Q = new ECPoint
                {
                    X = share.Slice(1, CoordinateSize).ToArray(),
                    Y = share.Slice(1 + CoordinateSize, CoordinateSize).ToArray(),
                },
            });
        }
        catch (CryptographicException)
        {
            return null;
        }
    }

    private static void Expand(ReadOnlySpan<byte> pseudoRandomKey, ReadOnlySpan<byte> label, ReadOnlySpan<byte> transcript, Span<byte> key)
    {
        Span<byte> info = stackalloc byte[label.Length + transcript.Length];
        label.CopyTo(info);
        transcript.CopyTo(info[label.Length..]);
        HKDF.Expand(HashAlgorithmName.SHA256, pseudoRandomKey, key, info);
    }

    private void ComputeReceipt(byte[] key, int lastBatchId, Span<byte> receipt)
    {
        Span<byte> input = stackalloc byte[ReceiptInputSize];
        _transferId.TryWriteBytes(input[..IdSize]);
        BinaryPrimitives.WriteInt32LittleEndian(input[IdSize..], lastBatchId);
        HMACSHA256.HashData(key, input, receipt);
    }

    /// <summary>
    /// The header's <see cref="ChunkHeader.BatchId"/> as it is written there, little-endian, then eight
    /// zero bytes. A batch id is spent once per session and a resend replays the sealed bytes, so under
    /// one key a nonce never covers two plaintexts.
    /// </summary>
    private static void WriteNonce(int batchId, Span<byte> nonce)
    {
        nonce.Clear();
        BinaryPrimitives.WriteInt32LittleEndian(nonce, batchId);
    }

    private bool Refuse()
    {
        _failed = true;
        return false;
    }

    /// <summary>
    /// For a method whose input comes off the wire: misuse throws, a bad input is the caller's to
    /// refuse with the answer. <paramref name="sender"/> is the role the method belongs to, or
    /// <c>null</c> for both.
    /// </summary>
    private void EnsureChecking(bool? sender)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        RequireRole(sender);
    }

    /// <summary>
    /// For a method that produces bytes: after a refusal there is nothing it may produce.
    /// </summary>
    /// <returns>The keys, or <c>null</c> before they are derived.</returns>
    private Keys? EnsureProducing(bool? sender)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        RequireRole(sender);
        if (_failed) throw new InvalidOperationException("A check failed: this end's encryption is refused.");
        return Volatile.Read(ref _keys);
    }

    private void RequireRole(bool? sender)
    {
        if (sender is { } role && role != _isSender)
        {
            throw new InvalidOperationException($"Not a step of the {(_isSender ? "sender" : "receiver")}.");
        }
    }

    private static void RequireLength(int actual, int expected, string parameter)
    {
        if (actual != expected)
        {
            throw new ArgumentException($"Expected {expected} bytes, got {actual}.", parameter);
        }
    }

    private static void RequireInPlaceOrApart(ReadOnlySpan<byte> source, ReadOnlySpan<byte> destination, string parameter)
    {
        if (source.Overlaps(destination, out var offset) && offset != 0)
        {
            throw new ArgumentException("The buffers overlap other than at their start.", parameter);
        }
    }

    private sealed class Keys(AesGcm batch, byte[] senderReceipt, byte[] receiverReceipt) : IDisposable
    {
        public AesGcm Batch { get; } = batch;
        public byte[] SenderReceipt { get; } = senderReceipt;
        public byte[] ReceiverReceipt { get; } = receiverReceipt;

        public void Dispose()
        {
            Batch.Dispose();
            CryptographicOperations.ZeroMemory(SenderReceipt);
            CryptographicOperations.ZeroMemory(ReceiverReceipt);
        }
    }
}
