using System.Buffers;
using System.Buffers.Binary;
using System.Security.Cryptography;
using Microsoft.Extensions.Logging;
using TransportR.Client.Configuration;
using TransportR.Client.Protocol;

namespace TransportR.Client.Sessions;

/// <summary>
/// The end-to-end encryption of one session, over its <see cref="EndToEndCipher"/>: what each chunk it
/// reads may carry, the share of its Pings, the Pong that answers them, and the keys a sender waits for
/// before it seals anything. The rules are those of "End-to-End Encryption" in
/// <c>TransportRArchitecture.md</c>.
/// </summary>
/// <remarks>
/// A refusal is raised as an <see cref="EndToEndEncryptionException"/>, which tells the peer, or as a
/// <see cref="WireProtocolViolationException"/> for a control chunk of another length. The session
/// disposes it once its loops have stopped, as the cipher requires.
/// </remarks>
internal sealed class SessionEncryption : IDisposable
{
    /// <summary>The largest payload an encrypted sender's control stream carries: a Pong.</summary>
    public const int MaxControlPayloadBytes = EndToEndCipher.PongPayloadSize;

    private const string PeerInClearMessage = "The peer does not encrypt end to end, and this end does.";
    private const string NotVerified =
        "End-to-end encryption could not be verified: the streams were altered on their way, or the two ends do not hold the same pre-shared key.";

    /// <summary>What the end that has no key fails with. The log says why: no key from the provider, or no P-256.</summary>
    public const string NoKey = "This end could not set up end-to-end encryption for its peer: the transfer is refused.";

    private readonly Guid _transferId;
    private readonly EndToEndCipher? _cipher;
    private readonly byte[] _ping = [];
    private readonly TaskCompletionSource _keys = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private byte[]? _pong;
    private volatile bool _hasRefused;

    // The sender's: the BatchId its Complete signs, the only one a CompleteAck may acknowledge. Signed
    // on the caller of CompleteAsync, read on the read loop: the id is written before the flag.
    private int _signedBatchId;
    private volatile bool _signed;

    private SessionEncryption(bool isSender, Guid transferId, EndToEndCipher? cipher)
    {
        _transferId = transferId;
        _cipher = cipher;
        if (isSender && cipher is not null)
        {
            _ping = new byte[EndToEndCipher.PingPayloadSize];
            cipher.WritePing(_ping);
        }
    }

    /// <summary>
    /// The encryption of a session the options encrypt, or <c>null</c> for a session in clear. Calls the
    /// pre-shared key provider, once, with the peer's id: one that gives no 32-byte key, or throws, leaves
    /// the session <see cref="Refused"/>.
    /// </summary>
    public static SessionEncryption? Open(
        ClientOptions options, bool isSender, string transferId, TransferParties parties, ILogger logger)
    {
        if (options.EndToEndEncryption is not { } settings) return null;

        var id = Guid.Parse(transferId);
        var peer = isSender ? parties.ReceiverId : parties.SenderId;
        byte[]? preSharedKey = null;
        if (settings.PreSharedKey is { } provider)
        {
            try
            {
                preSharedKey = provider(peer);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "The pre-shared key provider failed for peer {PeerId}: Transfer={TransferId} is refused", peer, transferId);
                return new SessionEncryption(isSender, id, cipher: null);
            }

            if (preSharedKey is not { Length: EndToEndCipher.PreSharedKeySize })
            {
                logger.LogError(
                    "The pre-shared key provider gave {Given} for peer {PeerId}, where {Expected} bytes are needed: Transfer={TransferId} is refused",
                    preSharedKey is null ? "no key" : $"{preSharedKey.Length} bytes", peer, EndToEndCipher.PreSharedKeySize, transferId);
                return new SessionEncryption(isSender, id, cipher: null);
            }
        }

        try
        {
            return new SessionEncryption(isSender, id, isSender
                ? EndToEndCipher.ForSender(id, parties.SenderId, parties.ReceiverId, preSharedKey)
                : EndToEndCipher.ForReceiver(id, parties.SenderId, parties.ReceiverId, preSharedKey));
        }
        catch (Exception ex) when (ex is CryptographicException or PlatformNotSupportedException)
        {
            logger.LogError(ex, "End-to-end encryption is not available here: Transfer={TransferId} is refused", transferId);
            return new SessionEncryption(isSender, id, cipher: null);
        }
    }

    /// <summary>
    /// The largest payload an encrypted receiver's data stream carries: a sealed batch at
    /// <paramref name="maxBatchBytes"/> and its tag, or a Ping, whichever is longer.
    /// </summary>
    public static int MaxDataPayloadBytes(int maxBatchBytes) =>
        (int)Math.Max(Math.Min((long)maxBatchBytes + EndToEndCipher.TagSize, int.MaxValue), EndToEndCipher.PingPayloadSize);

    /// <summary>
    /// This end has no key to run the transfer with, and refuses it at its first stream: its provider
    /// gave none for the peer, or the platform has no P-256.
    /// </summary>
    public bool Refused => _cipher is null;

    /// <summary>The payload of every Ping the sender writes: its share.</summary>
    public ReadOnlyMemory<byte> PingPayload => _ping;

    /// <summary>
    /// Whether this end has refused something it read: from then on a sender writes nothing but its
    /// Abort, which a resend would only hold back.
    /// </summary>
    public bool HasRefused => _hasRefused;

    /// <summary>The type a batch of <paramref name="clear"/>'s kind is written with, sealed.</summary>
    public static ChunkType SealedTypeOf(ChunkType clear) => clear switch
    {
        ChunkType.Batch => ChunkType.SealedBatch,
        ChunkType.StreamBatch => ChunkType.SealedStreamBatch,
        _ => throw new ArgumentOutOfRangeException(nameof(clear), clear, "Not a batch chunk type."),
    };

    /// <summary>
    /// Refuses a control chunk of another length than end-to-end encryption admits. A Ping or Pong
    /// without a payload is a peer in clear. The session has refused a type its stream does not carry
    /// before this, in both modes; a batch is bounded apart.
    /// </summary>
    public void CheckControlChunk(in ChunkHeader header)
    {
        int? admitted = header.Type switch
        {
            ChunkType.BatchAck or ChunkType.Abort => 0,
            ChunkType.Ping => EndToEndCipher.PingPayloadSize,
            ChunkType.Pong => EndToEndCipher.PongPayloadSize,
            ChunkType.Complete or ChunkType.CompleteAck => EndToEndCipher.ReceiptSize,
            _ => null,
        };

        if (admitted is not { } length || length == header.PayloadLength) return;

        if (header.PayloadLength == 0 && header.Type is ChunkType.Ping or ChunkType.Pong)
        {
            throw PeerInClear();
        }

        throw new WireProtocolViolationException(
            $"A {header.Type} chunk carried {header.PayloadLength} bytes, where end-to-end encryption admits {length}.");
    }

    /// <summary>
    /// Takes the sender's share from a Ping, and returns the payload of the Pong that answers it: the
    /// same for every Ping of the session.
    /// </summary>
    public byte[] AnswerPing(ReadOnlySpan<byte> payload)
    {
        var cipher = Keyed();
        if (!cipher.TryAcceptPing(payload)) throw Unverified();

        if (_pong is null)
        {
            var pong = new byte[EndToEndCipher.PongPayloadSize];
            cipher.WritePong(pong);
            _pong = pong;
        }

        return _pong;
    }

    /// <summary>
    /// Takes the receiver's share and the key confirmation from a Pong. The first that holds releases
    /// whoever waits for the keys.
    /// </summary>
    public void AcceptPong(ReadOnlySpan<byte> payload)
    {
        if (!Keyed().TryAcceptPong(payload)) throw Unverified();
        _keys.TrySetResult();
    }

    /// <summary>
    /// Seals a batch body, its 4-byte count then its payload, into a body of the same layout whose
    /// payload is the ciphertext and the tag, sealed under the header it is written with. Once: a resend
    /// writes the sealed body again, so no nonce covers two plaintexts.
    /// </summary>
    /// <returns>The sealed body, the caller's to dispose.</returns>
    public PooledPayload Seal(ChunkType sealedType, int batchId, ReadOnlySequence<byte> body)
    {
        var cipher = Keyed();
        var plaintext = body.Slice(sizeof(int));
        var plaintextLength = checked((int)plaintext.Length);
        var sealedLength = checked(plaintextLength + EndToEndCipher.TagSize);

        var sealedBody = new PooledPayload(sizeof(int) + sealedLength);
        try
        {
            var span = sealedBody.Memory.Span;
            body.Slice(0, sizeof(int)).CopyTo(span);
            plaintext.CopyTo(span[sizeof(int)..]);

            // The header ChunkIO writes for this body, byte for byte: the tag covers it.
            var header = ChunkHeader.CreateBatch(_transferId, batchId, BinaryPrimitives.ReadInt32LittleEndian(span), sealedLength)
                with { Type = sealedType };
            cipher.Seal(header, span.Slice(sizeof(int), plaintextLength), span.Slice(sizeof(int), sealedLength));
            return sealedBody;
        }
        catch
        {
            sealedBody.Dispose();
            throw;
        }
    }

    /// <summary>
    /// Opens a sealed batch in place, under the header it was read with.
    /// </summary>
    /// <returns>The plaintext, at the start of <paramref name="sealedPayload"/>.</returns>
    public Memory<byte> Open(in ChunkHeader header, Memory<byte> sealedPayload)
    {
        var cipher = Keyed();
        var span = sealedPayload.Span;
        var plaintextLength = Math.Max(0, span.Length - EndToEndCipher.TagSize);
        if (!cipher.TryOpen(header, span, span[..plaintextLength])) throw Unverified();
        return sealedPayload[..plaintextLength];
    }

    /// <summary>
    /// The sender's receipt for <paramref name="lastBatchId"/>, which Complete carries: called once every
    /// batch is registered, so that no Complete for fewer batches exists. <c>false</c> when the keys
    /// were refused meanwhile: the session is failing, and there is nothing to sign.
    /// </summary>
    public bool TrySignComplete(int lastBatchId, out byte[] receipt)
    {
        receipt = new byte[EndToEndCipher.ReceiptSize];
        if (_cipher is not { HasKeys: true } cipher) return false;

        try
        {
            cipher.WriteReceipt(lastBatchId, receipt);
        }
        catch (InvalidOperationException)
        {
            return false;
        }

        _signedBatchId = lastBatchId;
        _signed = true;
        return true;
    }

    /// <summary>
    /// Checks the sender's receipt in a Complete for <paramref name="lastBatchId"/>, before anything
    /// else is judged of it.
    /// </summary>
    public void VerifyComplete(int lastBatchId, ReadOnlySpan<byte> receipt)
    {
        if (!Keyed().VerifyPeerReceipt(lastBatchId, receipt)) throw Unverified();
    }

    /// <summary>
    /// The receiver's receipt for the Complete it accepted, which CompleteAck carries, and carries again
    /// unchanged on a reopened stream.
    /// </summary>
    public byte[] SignCompleteAck(int lastBatchId)
    {
        var receipt = new byte[EndToEndCipher.ReceiptSize];
        Keyed().WriteReceipt(lastBatchId, receipt);
        return receipt;
    }

    /// <summary>
    /// Checks the receiver's receipt in a CompleteAck for the BatchId the sender's Complete signed. One
    /// that comes before any Complete was signed acknowledges nothing, and is refused.
    /// </summary>
    public void VerifyCompleteAck(ReadOnlySpan<byte> receipt)
    {
        if (!_signed || !Keyed().VerifyPeerReceipt(_signedBatchId, receipt)) throw Unverified();
    }

    /// <summary>What an encrypted end raises on a batch in clear, or a Ping or Pong without a share.</summary>
    public EndToEndEncryptionException PeerInClear()
    {
        _hasRefused = true;
        return new EndToEndEncryptionException(AbortReason.EncryptionModeMismatch, PeerInClearMessage);
    }

    /// <summary>
    /// Completes once a Pong whose confirmation holds has given the sender its keys. Cancelled when the
    /// session fails or is released.
    /// </summary>
    public Task WaitForKeysAsync(CancellationToken ct) => _keys.Task.WaitAsync(ct);

    /// <summary>Lets go of whoever waits for keys that will not come: the session has failed or is released.</summary>
    public void Abandon() => _keys.TrySetCanceled();

    public void Dispose()
    {
        Abandon();
        _cipher?.Dispose();
    }

    private EndToEndCipher Keyed()
    {
        if (_cipher is { } cipher) return cipher;
        _hasRefused = true;
        throw new EndToEndEncryptionException(AbortReason.VerificationFailed, NoKey);
    }

    private EndToEndEncryptionException Unverified()
    {
        _hasRefused = true;
        return new EndToEndEncryptionException(AbortReason.VerificationFailed, NotVerified);
    }
}
