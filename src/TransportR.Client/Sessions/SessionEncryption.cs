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
/// <see cref="WireProtocolViolationException"/> for a length or a chunk the stream does not carry. The
/// session disposes it once its loops have stopped, as the cipher requires.
/// </remarks>
internal sealed class SessionEncryption : IDisposable
{
    /// <summary>The largest payload an encrypted sender's control stream carries: a Pong.</summary>
    public const int MaxControlPayloadBytes = EndToEndCipher.PongPayloadSize;

    private const string PeerInClear = "The peer does not encrypt end to end, and this end does.";
    private const string NotVerified =
        "End-to-end encryption could not be verified: the streams were altered on their way, or the two ends do not hold the same pre-shared key.";
    /// <summary>What the end that has no key fails with. The log says why: no key from the provider, or no P-256.</summary>
    public const string NoKey = "This end could not set up end-to-end encryption for its peer: the transfer is refused.";

    private readonly bool _isSender;
    private readonly EndToEndCipher? _cipher;
    private readonly byte[] _ping = [];
    private readonly TaskCompletionSource _keys = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private byte[]? _pong;

    private SessionEncryption(bool isSender, EndToEndCipher? cipher)
    {
        _isSender = isSender;
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
                return new SessionEncryption(isSender, cipher: null);
            }

            if (preSharedKey is not { Length: EndToEndCipher.PreSharedKeySize })
            {
                logger.LogError(
                    "The pre-shared key provider gave {Given} for peer {PeerId}, where {Expected} bytes are needed: Transfer={TransferId} is refused",
                    preSharedKey is null ? "no key" : $"{preSharedKey.Length} bytes", peer, EndToEndCipher.PreSharedKeySize, transferId);
                return new SessionEncryption(isSender, cipher: null);
            }
        }

        try
        {
            var id = Guid.Parse(transferId);
            return new SessionEncryption(isSender, isSender
                ? EndToEndCipher.ForSender(id, parties.SenderId, parties.ReceiverId, preSharedKey)
                : EndToEndCipher.ForReceiver(id, parties.SenderId, parties.ReceiverId, preSharedKey));
        }
        catch (Exception ex) when (ex is CryptographicException or PlatformNotSupportedException)
        {
            logger.LogError(ex, "End-to-end encryption is not available here: Transfer={TransferId} is refused", transferId);
            return new SessionEncryption(isSender, cipher: null);
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
    /// Refuses a control chunk this end's stream does not carry when it is encrypted, or carries at
    /// another length. A sender's control stream carries BatchAck, Pong, CompleteAck and Abort; a
    /// receiver's data stream Ping, Complete and Abort. A Ping or Pong without a payload is a peer in
    /// clear. An unknown type is left to the dispatch, which refuses it.
    /// </summary>
    public void CheckControlChunk(in ChunkHeader header)
    {
        int? admitted = (_isSender, header.Type) switch
        {
            (true, ChunkType.BatchAck) or (_, ChunkType.Abort) => 0,
            (true, ChunkType.Pong) => EndToEndCipher.PongPayloadSize,
            (false, ChunkType.Ping) => EndToEndCipher.PingPayloadSize,
            (true, ChunkType.CompleteAck) or (false, ChunkType.Complete) => header.PayloadLength,
            _ when !Enum.IsDefined(header.Type) => header.PayloadLength,
            _ => null,
        };

        if (admitted == header.PayloadLength) return;

        if (header.PayloadLength == 0 && header.Type is ChunkType.Ping or ChunkType.Pong)
        {
            throw new EndToEndEncryptionException(AbortReason.EncryptionModeMismatch, PeerInClear);
        }

        throw admitted is { } length
            ? new WireProtocolViolationException(
                $"A {header.Type} chunk carried {header.PayloadLength} bytes, where end-to-end encryption admits {length}.")
            : new WireProtocolViolationException(
                $"A {header.Type} chunk came on the stream an encrypted {(_isSender ? "sender" : "receiver")} reads, which does not carry one.");
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

    private EndToEndCipher Keyed() =>
        _cipher ?? throw new EndToEndEncryptionException(AbortReason.VerificationFailed, NoKey);

    private static EndToEndEncryptionException Unverified() => new(AbortReason.VerificationFailed, NotVerified);
}
