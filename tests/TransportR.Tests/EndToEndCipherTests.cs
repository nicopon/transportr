using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using TransportR.Client.Protocol;

namespace TransportR.Tests;

/// <summary>
/// <see cref="EndToEndCipher"/> on its own, with no stream behind it: the handshake carried by Ping and
/// Pong, a sealed batch, the receipts of Complete and CompleteAck, and what each refuses. The tests
/// that act as the hub alter the bytes a hub forwards; the ones that act as the specification derive
/// every value from the BCL primitives, as "End-to-End Encryption" in <c>TransportRArchitecture.md</c>
/// states it.
/// </summary>
public class EndToEndCipherTests
{
    // The values "End-to-End Encryption" gives the two sealed chunk types.
    private const ChunkType SealedBatch = (ChunkType)0x09;
    private const ChunkType SealedStreamBatch = (ChunkType)0x0A;

    private readonly Guid _transferId = Guid.NewGuid();
    private readonly Guid _senderId = Guid.NewGuid();
    private readonly Guid _receiverId = Guid.NewGuid();

    // ---- The handshake and a sealed batch ---------------------------------------------------------

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void A_batch_sealed_by_the_sender_opens_at_the_receiver(bool withPreSharedKey)
    {
        var psk = withPreSharedKey ? RandomNumberGenerator.GetBytes(32) : null;
        using var sender = EndToEndCipher.ForSender(_transferId, _senderId, _receiverId, psk);
        using var receiver = EndToEndCipher.ForReceiver(_transferId, _senderId, _receiverId, psk);

        Assert.True(Handshake(sender, receiver));
        Assert.True(sender.HasKeys);
        Assert.True(receiver.HasKeys);
        Assert.Equal(withPreSharedKey, sender.UsesPreSharedKey);

        var plaintext = RandomNumberGenerator.GetBytes(1000);
        var header = Header(batchId: 7, count: 12, plaintext.Length);
        var sealedPayload = new byte[header.PayloadLength];
        sender.Seal(header, plaintext, sealedPayload);

        Assert.False(sealedPayload.AsSpan(0, plaintext.Length).SequenceEqual(plaintext));

        var opened = new byte[plaintext.Length];
        Assert.True(receiver.TryOpen(header, sealedPayload, opened));
        Assert.Equal(plaintext, opened);
    }

    [Fact]
    public void A_batch_seals_and_opens_in_place()
    {
        var (sender, receiver) = Connected();
        using (sender)
        using (receiver)
        {
            var plaintext = RandomNumberGenerator.GetBytes(300);
            var header = Header(batchId: 0, count: 3, plaintext.Length);

            var elsewhere = new byte[header.PayloadLength];
            sender.Seal(header, plaintext, elsewhere);

            var buffer = new byte[header.PayloadLength];
            plaintext.CopyTo(buffer, 0);
            sender.Seal(header, buffer.AsSpan(0, plaintext.Length), buffer);

            // A resend replays the sealed bytes: the same header seals the same plaintext the same way.
            Assert.Equal(elsewhere, buffer);

            Assert.True(receiver.TryOpen(header, buffer, buffer.AsSpan(0, plaintext.Length)));
            Assert.Equal(plaintext, buffer.AsSpan(0, plaintext.Length).ToArray());
        }
    }

    [Fact]
    public void An_empty_batch_seals_to_its_tag_alone()
    {
        var (sender, receiver) = Connected();
        using (sender)
        using (receiver)
        {
            var header = Header(batchId: 0, count: 0, plaintextLength: 0);
            var sealedPayload = new byte[EndToEndCipher.TagSize];
            sender.Seal(header, ReadOnlySpan<byte>.Empty, sealedPayload);

            Assert.True(receiver.TryOpen(header, sealedPayload, Span<byte>.Empty));
        }
    }

    // ---- What a hub that alters a batch meets -----------------------------------------------------

    [Theory]
    [InlineData(0)]
    [InlineData(500)]
    [InlineData(999)]
    public void A_byte_altered_in_the_sealed_payload_is_refused(int offset)
    {
        var (sender, receiver) = Connected();
        using (sender)
        using (receiver)
        {
            var (header, sealedPayload) = SealOne(sender, plaintextLength: 1000);
            sealedPayload[offset] ^= 0x01;

            Assert.False(receiver.TryOpen(header, sealedPayload, new byte[1000]));
        }
    }

    [Theory]
    [InlineData(0)]
    [InlineData(15)]
    public void A_byte_altered_in_the_tag_is_refused(int offsetInTag)
    {
        var (sender, receiver) = Connected();
        using (sender)
        using (receiver)
        {
            var (header, sealedPayload) = SealOne(sender, plaintextLength: 100);
            sealedPayload[100 + offsetInTag] ^= 0x80;

            Assert.False(receiver.TryOpen(header, sealedPayload, new byte[100]));
        }
    }

    /// <summary>
    /// The header is the associated data, byte for byte: whatever field a hub changes, the batch does
    /// not open. The payload's length is its own case, below, since the reader takes as many bytes as
    /// the header announces.
    /// </summary>
    [Theory]
    [InlineData(0)]  // Version
    [InlineData(1)]  // Type
    [InlineData(6)]  // BatchId, low byte
    [InlineData(9)]  // BatchId, high byte
    [InlineData(10)] // Count, low byte
    [InlineData(13)] // Count, high byte
    [InlineData(14)] // TransferId, first byte
    [InlineData(29)] // TransferId, last byte
    public void A_byte_altered_in_a_header_field_is_refused(int offsetInHeader)
    {
        var (sender, receiver) = Connected();
        using (sender)
        using (receiver)
        {
            var (header, sealedPayload) = SealOne(sender, plaintextLength: 64);

            var wire = new byte[ChunkHeader.Size];
            header.WriteTo(wire);
            wire[offsetInHeader] ^= 0x01;
            var altered = ChunkHeader.ReadFrom(wire);

            Assert.False(receiver.TryOpen(altered, sealedPayload, new byte[64]));
        }
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(1)]
    public void A_payload_length_altered_with_the_payload_is_refused(int delta)
    {
        var (sender, receiver) = Connected();
        using (sender)
        using (receiver)
        {
            var (header, sealedPayload) = SealOne(sender, plaintextLength: 64);

            var altered = header with { PayloadLength = header.PayloadLength + delta };
            var payload = new byte[altered.PayloadLength];
            sealedPayload.AsSpan(0, Math.Min(sealedPayload.Length, payload.Length)).CopyTo(payload);

            Assert.False(receiver.TryOpen(altered, payload, new byte[payload.Length - EndToEndCipher.TagSize]));
        }
    }

    [Fact]
    public void A_payload_shorter_than_a_tag_is_refused()
    {
        var (sender, receiver) = Connected();
        using (sender)
        using (receiver)
        {
            var header = Header(batchId: 0, count: 0, plaintextLength: 0) with { PayloadLength = EndToEndCipher.TagSize - 1 };

            Assert.False(receiver.TryOpen(header, new byte[EndToEndCipher.TagSize - 1], Span<byte>.Empty));
        }
    }

    [Fact]
    public void A_batch_of_one_type_does_not_open_as_the_other()
    {
        var (sender, receiver) = Connected();
        using (sender)
        using (receiver)
        {
            var (header, sealedPayload) = SealOne(sender, plaintextLength: 64);

            Assert.False(receiver.TryOpen(header with { Type = SealedStreamBatch }, sealedPayload, new byte[64]));
        }
    }

    [Fact]
    public void A_receiver_that_holds_no_keys_refuses_a_sealed_batch()
    {
        var (sender, receiver) = Connected();
        using (sender)
        using (receiver)
        using (var early = EndToEndCipher.ForReceiver(_transferId, _senderId, _receiverId, null))
        {
            var (header, sealedPayload) = SealOne(sender, plaintextLength: 64);

            Assert.False(early.TryOpen(header, sealedPayload, new byte[64]));
        }
    }

    // ---- What a hub that substitutes a share, or a peer with another key, meets -------------------

    [Fact]
    public void A_different_pre_shared_key_fails_the_confirmation()
    {
        using var sender = EndToEndCipher.ForSender(_transferId, _senderId, _receiverId, RandomNumberGenerator.GetBytes(32));
        using var receiver = EndToEndCipher.ForReceiver(_transferId, _senderId, _receiverId, RandomNumberGenerator.GetBytes(32));

        Assert.False(Handshake(sender, receiver));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void A_pre_shared_key_on_one_end_only_fails_the_confirmation(bool senderHasIt)
    {
        var psk = RandomNumberGenerator.GetBytes(32);
        using var sender = EndToEndCipher.ForSender(_transferId, _senderId, _receiverId, senderHasIt ? psk : null);
        using var receiver = EndToEndCipher.ForReceiver(_transferId, _senderId, _receiverId, senderHasIt ? null : psk);

        Assert.False(Handshake(sender, receiver));
    }

    [Fact]
    public void An_all_zero_pre_shared_key_does_not_pass_for_none()
    {
        using var sender = EndToEndCipher.ForSender(_transferId, _senderId, _receiverId, new byte[32]);
        using var receiver = EndToEndCipher.ForReceiver(_transferId, _senderId, _receiverId, null);

        Assert.False(Handshake(sender, receiver));
    }

    /// <summary>
    /// The hub puts a share of its own in the Pong and forwards the receiver's confirmation: the
    /// confirmation covers the shares, so the sender refuses it whether or not a key is pre-shared.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void A_substituted_receiver_share_fails_the_confirmation(bool withPreSharedKey)
    {
        var psk = withPreSharedKey ? RandomNumberGenerator.GetBytes(32) : null;
        using var sender = EndToEndCipher.ForSender(_transferId, _senderId, _receiverId, psk);
        using var receiver = EndToEndCipher.ForReceiver(_transferId, _senderId, _receiverId, psk);
        using var hub = EndToEndCipher.ForReceiver(_transferId, _senderId, _receiverId, null);

        var ping = Ping(sender);
        Assert.True(receiver.TryAcceptPing(ping));
        Assert.True(hub.TryAcceptPing(ping));

        var pong = Pong(receiver);
        Pong(hub).AsSpan(0, EndToEndCipher.ShareSize).CopyTo(pong);

        Assert.False(sender.TryAcceptPong(pong));
    }

    /// <summary>
    /// The hub answers the sender's Ping itself. Without a pre-shared key it can, and that is the
    /// limit of encryption alone; with one, it has no way to compute the confirmation.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void A_pong_forged_without_the_pre_shared_key_fails_the_confirmation(bool hubGuessesAKey)
    {
        using var sender = EndToEndCipher.ForSender(_transferId, _senderId, _receiverId, RandomNumberGenerator.GetBytes(32));
        using var hub = EndToEndCipher.ForReceiver(_transferId, _senderId, _receiverId, hubGuessesAKey ? RandomNumberGenerator.GetBytes(32) : null);

        Assert.True(hub.TryAcceptPing(Ping(sender)));

        Assert.False(sender.TryAcceptPong(Pong(hub)));
    }

    /// <summary>
    /// The hub puts a share of its own in the Ping: the receiver cannot tell, but the confirmation it
    /// sends back covers the share it saw, and the sender refuses it.
    /// </summary>
    [Fact]
    public void A_substituted_sender_share_fails_the_confirmation()
    {
        var psk = RandomNumberGenerator.GetBytes(32);
        using var sender = EndToEndCipher.ForSender(_transferId, _senderId, _receiverId, psk);
        using var receiver = EndToEndCipher.ForReceiver(_transferId, _senderId, _receiverId, psk);
        using var hub = EndToEndCipher.ForSender(_transferId, _senderId, _receiverId, null);

        Assert.True(receiver.TryAcceptPing(Ping(hub)));

        Assert.False(sender.TryAcceptPong(Pong(receiver)));
    }

    [Fact]
    public void Two_sessions_on_one_transfer_derive_different_keys()
    {
        var psk = RandomNumberGenerator.GetBytes(32);
        var (sender1, receiver1) = Connected(psk);
        var (sender2, receiver2) = Connected(psk);
        using (sender1)
        using (receiver1)
        using (sender2)
        using (receiver2)
        {
            Assert.NotEqual(Pong(receiver1)[EndToEndCipher.ShareSize..], Pong(receiver2)[EndToEndCipher.ShareSize..]);

            var plaintext = RandomNumberGenerator.GetBytes(64);
            var header = Header(batchId: 0, count: 1, plaintext.Length);
            var sealed1 = new byte[header.PayloadLength];
            var sealed2 = new byte[header.PayloadLength];
            sender1.Seal(header, plaintext, sealed1);
            sender2.Seal(header, plaintext, sealed2);

            Assert.NotEqual(sealed1, sealed2);
            Assert.False(receiver2.TryOpen(header, sealed1, new byte[64]));
        }
    }

    // ---- What a hub that hands a transfer to another node, or names another sender, meets ---------

    /// <summary>
    /// A, B and C hold one pre-shared key, their trust domain's. A asks for B, and the hub hands the
    /// transfer to C, which takes it as A's transfer to C: had the confirmation passed, A would take
    /// C's receipt for B's.
    /// </summary>
    [Fact]
    public void A_transfer_handed_to_another_node_of_the_domain_fails_the_confirmation()
    {
        var domainKey = RandomNumberGenerator.GetBytes(32);
        var (a, b, c) = (Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid());
        using var sender = EndToEndCipher.ForSender(_transferId, senderId: a, receiverId: b, domainKey);
        using var receiver = EndToEndCipher.ForReceiver(_transferId, senderId: a, receiverId: c, domainKey);

        Assert.False(Handshake(sender, receiver));
    }

    /// <summary>A sends to C, and the hub tells C that D sends.</summary>
    [Fact]
    public void A_sender_misnamed_to_the_receiver_fails_the_confirmation()
    {
        var domainKey = RandomNumberGenerator.GetBytes(32);
        var (a, c, d) = (Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid());
        using var sender = EndToEndCipher.ForSender(_transferId, senderId: a, receiverId: c, domainKey);
        using var receiver = EndToEndCipher.ForReceiver(_transferId, senderId: d, receiverId: c, domainKey);

        Assert.False(Handshake(sender, receiver));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Ids_swapped_between_the_roles_fail_the_confirmation(bool withPreSharedKey)
    {
        var psk = withPreSharedKey ? RandomNumberGenerator.GetBytes(32) : null;
        using var sender = EndToEndCipher.ForSender(_transferId, _senderId, _receiverId, psk);
        using var receiver = EndToEndCipher.ForReceiver(_transferId, senderId: _receiverId, receiverId: _senderId, psk);

        Assert.False(Handshake(sender, receiver));
    }

    /// <summary>
    /// The two ends agree on every input but one id, which one of them sees otherwise.
    /// </summary>
    [Theory]
    [InlineData(true, true, false)]
    [InlineData(true, false, false)]
    [InlineData(false, true, false)]
    [InlineData(false, false, false)]
    [InlineData(true, true, true)]
    [InlineData(true, false, true)]
    [InlineData(false, true, true)]
    [InlineData(false, false, true)]
    public void One_id_the_two_ends_see_differently_fails_the_confirmation(bool senderIdDiffers, bool atTheSender, bool withPreSharedKey)
    {
        var psk = withPreSharedKey ? RandomNumberGenerator.GetBytes(32) : null;
        var other = Guid.NewGuid();
        var senderId = senderIdDiffers ? other : _senderId;
        var receiverId = senderIdDiffers ? _receiverId : other;

        using var sender = atTheSender
            ? EndToEndCipher.ForSender(_transferId, senderId, receiverId, psk)
            : EndToEndCipher.ForSender(_transferId, _senderId, _receiverId, psk);
        using var receiver = atTheSender
            ? EndToEndCipher.ForReceiver(_transferId, _senderId, _receiverId, psk)
            : EndToEndCipher.ForReceiver(_transferId, senderId, receiverId, psk);

        Assert.False(Handshake(sender, receiver));
    }

    // ---- The share, once per session --------------------------------------------------------------

    [Fact]
    public void The_same_share_is_accepted_again_and_another_is_refused_at_the_receiver()
    {
        using var sender = EndToEndCipher.ForSender(_transferId, _senderId, _receiverId, null);
        using var other = EndToEndCipher.ForSender(_transferId, _senderId, _receiverId, null);
        using var receiver = EndToEndCipher.ForReceiver(_transferId, _senderId, _receiverId, null);

        Assert.True(receiver.TryAcceptPing(Ping(sender)));
        var pong = Pong(receiver);

        Assert.True(receiver.TryAcceptPing(Ping(sender)));
        Assert.Equal(pong, Pong(receiver));

        Assert.False(receiver.TryAcceptPing(Ping(other)));
    }

    [Fact]
    public void The_same_pong_is_accepted_again_and_another_is_refused_at_the_sender()
    {
        using var sender = EndToEndCipher.ForSender(_transferId, _senderId, _receiverId, null);
        using var receiver = EndToEndCipher.ForReceiver(_transferId, _senderId, _receiverId, null);
        using var other = EndToEndCipher.ForReceiver(_transferId, _senderId, _receiverId, null);

        var ping = Ping(sender);
        Assert.True(receiver.TryAcceptPing(ping));
        Assert.True(other.TryAcceptPing(ping));

        Assert.True(sender.TryAcceptPong(Pong(receiver)));
        Assert.True(sender.TryAcceptPong(Pong(receiver)));

        // A whole, valid Pong of another receiver: its share differs from the one accepted.
        Assert.False(sender.TryAcceptPong(Pong(other)));
    }

    [Fact]
    public void Each_ping_carries_the_same_share()
    {
        using var sender = EndToEndCipher.ForSender(_transferId, _senderId, _receiverId, null);

        Assert.Equal(Ping(sender), Ping(sender));
    }

    [Fact]
    public void A_share_off_the_curve_is_refused()
    {
        using var sender = EndToEndCipher.ForSender(_transferId, _senderId, _receiverId, null);
        using var receiver = EndToEndCipher.ForReceiver(_transferId, _senderId, _receiverId, null);

        var ping = Ping(sender);
        ping[^1] ^= 0x01;

        Assert.False(receiver.TryAcceptPing(ping));
    }

    [Theory]
    [InlineData(0x02)] // A compressed point's prefix
    [InlineData(0x00)] // The point at infinity's
    public void A_share_not_in_uncompressed_form_is_refused(byte prefix)
    {
        using var sender = EndToEndCipher.ForSender(_transferId, _senderId, _receiverId, null);
        using var receiver = EndToEndCipher.ForReceiver(_transferId, _senderId, _receiverId, null);

        var ping = Ping(sender);
        ping[0] = prefix;

        Assert.False(receiver.TryAcceptPing(ping));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(EndToEndCipher.PingPayloadSize - 1)]
    [InlineData(EndToEndCipher.PingPayloadSize + 1)]
    public void A_ping_of_another_length_is_refused(int length)
    {
        using var receiver = EndToEndCipher.ForReceiver(_transferId, _senderId, _receiverId, null);

        Assert.False(receiver.TryAcceptPing(new byte[length]));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(EndToEndCipher.PongPayloadSize - 1)]
    [InlineData(EndToEndCipher.PongPayloadSize + 1)]
    public void A_pong_of_another_length_is_refused(int length)
    {
        using var sender = EndToEndCipher.ForSender(_transferId, _senderId, _receiverId, null);

        Assert.False(sender.TryAcceptPong(new byte[length]));
    }

    // ---- Receipts ---------------------------------------------------------------------------------

    [Theory]
    [InlineData(false, -1)]
    [InlineData(true, -1)]
    [InlineData(false, 41)]
    [InlineData(true, 41)]
    public void Each_receipt_verifies_at_the_peer(bool withPreSharedKey, int lastBatchId)
    {
        var (sender, receiver) = Connected(withPreSharedKey ? RandomNumberGenerator.GetBytes(32) : null);
        using (sender)
        using (receiver)
        {
            Assert.True(receiver.VerifyPeerReceipt(lastBatchId, Receipt(sender, lastBatchId)));
            Assert.True(sender.VerifyPeerReceipt(lastBatchId, Receipt(receiver, lastBatchId)));
        }
    }

    [Fact]
    public void A_receipt_for_another_batch_id_is_refused()
    {
        var (sender, receiver) = Connected();
        using (sender)
        using (receiver)
        {
            var senderReceipt = Receipt(sender, 41);
            var receiverReceipt = Receipt(receiver, 40);

            Assert.False(receiver.VerifyPeerReceipt(40, senderReceipt));
            Assert.False(sender.VerifyPeerReceipt(41, receiverReceipt));
        }
    }

    [Fact]
    public void A_receipt_of_another_transfer_is_refused()
    {
        var psk = RandomNumberGenerator.GetBytes(32);
        var otherTransferId = Guid.NewGuid();
        var (sender, receiver) = Connected(psk);
        using (sender)
        using (receiver)
        using (var otherSender = EndToEndCipher.ForSender(otherTransferId, _senderId, _receiverId, psk))
        using (var otherReceiver = EndToEndCipher.ForReceiver(otherTransferId, _senderId, _receiverId, psk))
        {
            Assert.True(Handshake(otherSender, otherReceiver));

            Assert.False(receiver.VerifyPeerReceipt(41, Receipt(otherSender, 41)));
            Assert.False(sender.VerifyPeerReceipt(41, Receipt(otherReceiver, 41)));
        }
    }

    /// <summary>
    /// A hub that sends the sender's Complete back as a CompleteAck, or the reverse: each end's receipt
    /// is keyed apart, so neither passes for the other's.
    /// </summary>
    [Fact]
    public void A_receipt_does_not_pass_for_the_peer_s()
    {
        var (sender, receiver) = Connected();
        using (sender)
        using (receiver)
        {
            Assert.False(sender.VerifyPeerReceipt(41, Receipt(sender, 41)));
            Assert.False(receiver.VerifyPeerReceipt(41, Receipt(receiver, 41)));
        }
    }

    [Theory]
    [InlineData(0)]
    [InlineData(31)]
    public void A_byte_altered_in_a_receipt_is_refused(int offset)
    {
        var (sender, receiver) = Connected();
        using (sender)
        using (receiver)
        {
            var receipt = Receipt(sender, 41);
            receipt[offset] ^= 0x01;

            Assert.False(receiver.VerifyPeerReceipt(41, receipt));
        }
    }

    [Theory]
    [InlineData(0)]
    [InlineData(EndToEndCipher.ReceiptSize - 1)]
    [InlineData(EndToEndCipher.ReceiptSize + 1)]
    public void A_receipt_of_another_length_is_refused(int length)
    {
        var (sender, receiver) = Connected();
        using (sender)
        using (receiver)
        {
            var receipt = new byte[length];
            Receipt(sender, 41).AsSpan(0, Math.Min(length, EndToEndCipher.ReceiptSize)).CopyTo(receipt);

            Assert.False(receiver.VerifyPeerReceipt(41, receipt));
        }
    }

    [Fact]
    public void A_receipt_is_refused_before_the_keys()
    {
        using var receiver = EndToEndCipher.ForReceiver(_transferId, _senderId, _receiverId, null);

        Assert.False(receiver.VerifyPeerReceipt(0, new byte[EndToEndCipher.ReceiptSize]));
    }

    // ---- A failed check, the roles, the order -----------------------------------------------------

    [Fact]
    public void After_a_refused_batch_the_receiver_refuses_everything()
    {
        var (sender, receiver) = Connected();
        using (sender)
        using (receiver)
        {
            var (header, sealedPayload) = SealOne(sender, plaintextLength: 64);
            var altered = sealedPayload.ToArray();
            altered[0] ^= 0x01;
            Assert.False(receiver.TryOpen(header, altered, new byte[64]));

            Assert.False(receiver.TryOpen(header, sealedPayload, new byte[64]));
            Assert.False(receiver.VerifyPeerReceipt(0, Receipt(sender, 0)));
            Assert.Throws<InvalidOperationException>(() => receiver.WritePong(new byte[EndToEndCipher.PongPayloadSize]));
            Assert.Throws<InvalidOperationException>(() => receiver.WriteReceipt(0, new byte[EndToEndCipher.ReceiptSize]));
        }
    }

    [Fact]
    public void After_a_refused_confirmation_the_sender_seals_nothing()
    {
        using var sender = EndToEndCipher.ForSender(_transferId, _senderId, _receiverId, RandomNumberGenerator.GetBytes(32));
        using var receiver = EndToEndCipher.ForReceiver(_transferId, _senderId, _receiverId, RandomNumberGenerator.GetBytes(32));
        Assert.False(Handshake(sender, receiver));

        var header = Header(batchId: 0, count: 1, plaintextLength: 8);
        Assert.Throws<InvalidOperationException>(() => sender.Seal(header, new byte[8], new byte[header.PayloadLength]));
        Assert.Throws<InvalidOperationException>(() => sender.WriteReceipt(0, new byte[EndToEndCipher.ReceiptSize]));
        Assert.False(sender.TryAcceptPong(Pong(receiver)));
    }

    [Fact]
    public void Nothing_is_sealed_or_signed_before_the_keys()
    {
        using var sender = EndToEndCipher.ForSender(_transferId, _senderId, _receiverId, null);
        using var receiver = EndToEndCipher.ForReceiver(_transferId, _senderId, _receiverId, null);

        var header = Header(batchId: 0, count: 1, plaintextLength: 8);
        Assert.Throws<InvalidOperationException>(() => sender.Seal(header, new byte[8], new byte[header.PayloadLength]));
        Assert.Throws<InvalidOperationException>(() => sender.WriteReceipt(0, new byte[EndToEndCipher.ReceiptSize]));
        Assert.Throws<InvalidOperationException>(() => receiver.WritePong(new byte[EndToEndCipher.PongPayloadSize]));
    }

    [Fact]
    public void Each_end_does_only_its_own_part()
    {
        var (sender, receiver) = Connected();
        using (sender)
        using (receiver)
        {
            var header = Header(batchId: 0, count: 1, plaintextLength: 8);

            Assert.Throws<InvalidOperationException>(() => receiver.WritePing(new byte[EndToEndCipher.PingPayloadSize]));
            Assert.Throws<InvalidOperationException>(() => receiver.TryAcceptPong(new byte[EndToEndCipher.PongPayloadSize]));
            Assert.Throws<InvalidOperationException>(() => receiver.Seal(header, new byte[8], new byte[header.PayloadLength]));
            Assert.Throws<InvalidOperationException>(() => sender.TryAcceptPing(new byte[EndToEndCipher.PingPayloadSize]));
            Assert.Throws<InvalidOperationException>(() => sender.WritePong(new byte[EndToEndCipher.PongPayloadSize]));
            Assert.Throws<InvalidOperationException>(() => sender.TryOpen(header, new byte[header.PayloadLength], new byte[8]));
        }
    }

    [Fact]
    public void A_seal_whose_header_does_not_announce_the_sealed_length_is_refused()
    {
        var (sender, receiver) = Connected();
        using (sender)
        using (receiver)
        {
            var header = Header(batchId: 0, count: 1, plaintextLength: 8) with { PayloadLength = 8 };

            Assert.Throws<ArgumentException>(() => sender.Seal(header, new byte[8], new byte[8]));
        }
    }

    /// <summary>
    /// Batch ids count up from 0, so a negative one has wrapped: the next would repeat a nonce under
    /// the same key.
    /// </summary>
    [Theory]
    [InlineData(-1)]
    [InlineData(int.MinValue)]
    public void A_batch_id_that_has_wrapped_is_not_sealed(int batchId)
    {
        var (sender, receiver) = Connected();
        using (sender)
        using (receiver)
        {
            var header = Header(batchId, count: 1, plaintextLength: 8);

            Assert.Throws<ArgumentOutOfRangeException>(() => sender.Seal(header, new byte[8], new byte[header.PayloadLength]));
        }
    }

    // ---- Secrets ----------------------------------------------------------------------------------

    [Theory]
    [InlineData(0)]
    [InlineData(16)]
    [InlineData(31)]
    [InlineData(33)]
    public void A_pre_shared_key_of_another_length_is_refused_without_its_bytes(int length)
    {
        var psk = RandomNumberGenerator.GetBytes(length);

        var refusal = Assert.Throws<ArgumentException>(() => EndToEndCipher.ForSender(_transferId, _senderId, _receiverId, psk));

        if (length > 0) AssertNotIn(refusal.ToString(), psk);
    }

    [Fact]
    public void The_caller_s_pre_shared_key_is_left_as_it_was()
    {
        var psk = RandomNumberGenerator.GetBytes(32);
        var copy = psk.ToArray();

        var (sender, receiver) = Connected(psk);
        sender.Dispose();
        receiver.Dispose();

        Assert.Equal(copy, psk);
    }

    [Fact]
    public void ToString_carries_no_secret()
    {
        var psk = RandomNumberGenerator.GetBytes(32);
        using var sender = EndToEndCipher.ForSender(_transferId, _senderId, _receiverId, psk);
        using var specReceiver = ECDiffieHellman.Create(ECCurve.NamedCurves.nistP256);

        var senderShare = Ping(sender);
        var receiverShare = Share(specReceiver);
        var keys = DeriveAsSpecified(specReceiver, senderShare, psk, senderShare, receiverShare);
        Assert.True(sender.TryAcceptPong([.. receiverShare, .. keys.Confirmation]));

        var text = sender.ToString();
        foreach (var secret in new[] { psk, keys.Batch, keys.ConfirmationKey, keys.SenderReceipt, keys.ReceiverReceipt })
        {
            AssertNotIn(text, secret);
        }
    }

    [Fact]
    public void After_dispose_no_operation_is_possible()
    {
        var (sender, receiver) = Connected();
        var header = Header(batchId: 0, count: 1, plaintextLength: 8);
        sender.Dispose();
        receiver.Dispose();
        sender.Dispose();

        Assert.Throws<ObjectDisposedException>(() => sender.WritePing(new byte[EndToEndCipher.PingPayloadSize]));
        Assert.Throws<ObjectDisposedException>(() => sender.TryAcceptPong(new byte[EndToEndCipher.PongPayloadSize]));
        Assert.Throws<ObjectDisposedException>(() => sender.Seal(header, new byte[8], new byte[header.PayloadLength]));
        Assert.Throws<ObjectDisposedException>(() => sender.WriteReceipt(0, new byte[EndToEndCipher.ReceiptSize]));
        Assert.Throws<ObjectDisposedException>(() => sender.VerifyPeerReceipt(0, new byte[EndToEndCipher.ReceiptSize]));
        Assert.Throws<ObjectDisposedException>(() => receiver.TryAcceptPing(new byte[EndToEndCipher.PingPayloadSize]));
        Assert.Throws<ObjectDisposedException>(() => receiver.WritePong(new byte[EndToEndCipher.PongPayloadSize]));
        Assert.Throws<ObjectDisposedException>(() => receiver.TryOpen(header, new byte[header.PayloadLength], new byte[8]));
        Assert.False(sender.HasKeys);
    }

    // ---- The specification, from the BCL primitives -----------------------------------------------

    /// <summary>
    /// The test plays the receiver with an ECDH key of its own and derives every value from the
    /// specification's text: the sender accepts the confirmation it computes, its batch opens with the
    /// key, nonce and associated data it states, and the receipts are the MACs it names.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void The_sender_derives_what_the_specification_states(bool withPreSharedKey)
    {
        var psk = withPreSharedKey ? RandomNumberGenerator.GetBytes(32) : null;
        using var sender = EndToEndCipher.ForSender(_transferId, _senderId, _receiverId, psk);
        using var specReceiver = ECDiffieHellman.Create(ECCurve.NamedCurves.nistP256);

        var senderShare = Ping(sender);
        Assert.Equal(EndToEndCipher.PingPayloadSize, senderShare.Length);
        Assert.Equal(0x04, senderShare[0]);

        var receiverShare = Share(specReceiver);
        var keys = DeriveAsSpecified(specReceiver, senderShare, psk, senderShare, receiverShare);
        Assert.True(sender.TryAcceptPong([.. receiverShare, .. keys.Confirmation]));

        var plaintext = RandomNumberGenerator.GetBytes(100);
        var header = Header(batchId: 0x01020304, count: 5, plaintext.Length);
        var sealedPayload = new byte[header.PayloadLength];
        sender.Seal(header, plaintext, sealedPayload);

        var nonce = new byte[12];
        BinaryPrimitives.WriteInt32LittleEndian(nonce, header.BatchId);
        var aad = new byte[ChunkHeader.Size];
        header.WriteTo(aad);
        var opened = new byte[plaintext.Length];
        using (var aes = new AesGcm(keys.Batch, EndToEndCipher.TagSize))
        {
            aes.Decrypt(nonce, sealedPayload.AsSpan(0, plaintext.Length), sealedPayload.AsSpan(plaintext.Length), opened, aad);
        }
        Assert.Equal(plaintext, opened);

        Assert.Equal(HMACSHA256.HashData(keys.SenderReceipt, ReceiptInput(_transferId, 41)), Receipt(sender, 41));
        Assert.True(sender.VerifyPeerReceipt(41, HMACSHA256.HashData(keys.ReceiverReceipt, ReceiptInput(_transferId, 41))));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void The_receiver_derives_what_the_specification_states(bool withPreSharedKey)
    {
        var psk = withPreSharedKey ? RandomNumberGenerator.GetBytes(32) : null;
        using var receiver = EndToEndCipher.ForReceiver(_transferId, _senderId, _receiverId, psk);
        using var specSender = ECDiffieHellman.Create(ECCurve.NamedCurves.nistP256);

        var senderShare = Share(specSender);
        Assert.True(receiver.TryAcceptPing(senderShare));
        var pong = Pong(receiver);
        var receiverShare = pong[..EndToEndCipher.ShareSize];

        var keys = DeriveAsSpecified(specSender, receiverShare, psk, senderShare, receiverShare);
        Assert.Equal(keys.Confirmation, pong[EndToEndCipher.ShareSize..]);

        var plaintext = RandomNumberGenerator.GetBytes(100);
        var header = Header(batchId: 3, count: 5, plaintext.Length) with { Type = SealedStreamBatch };
        var nonce = new byte[12];
        BinaryPrimitives.WriteInt32LittleEndian(nonce, header.BatchId);
        var aad = new byte[ChunkHeader.Size];
        header.WriteTo(aad);
        var sealedPayload = new byte[header.PayloadLength];
        using (var aes = new AesGcm(keys.Batch, EndToEndCipher.TagSize))
        {
            aes.Encrypt(nonce, plaintext, sealedPayload.AsSpan(0, plaintext.Length), sealedPayload.AsSpan(plaintext.Length), aad);
        }

        var opened = new byte[plaintext.Length];
        Assert.True(receiver.TryOpen(header, sealedPayload, opened));
        Assert.Equal(plaintext, opened);

        Assert.True(receiver.VerifyPeerReceipt(-1, HMACSHA256.HashData(keys.SenderReceipt, ReceiptInput(_transferId, -1))));
        Assert.Equal(HMACSHA256.HashData(keys.ReceiverReceipt, ReceiptInput(_transferId, -1)), Receipt(receiver, -1));
    }

    // ---- The known-answer vector ------------------------------------------------------------------

    /// <summary>
    /// Every byte of the handshake, of one sealed batch and of both receipts, for fixed inputs, as a
    /// script written from the specification alone computed them with other implementations of each
    /// primitive (Python's <c>cryptography</c>). Whatever changes one of them changes the wire.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void The_wire_matches_the_known_answer_vector(bool withPreSharedKey)
    {
        var expected = withPreSharedKey ? KnownAnswer.WithPreSharedKey : KnownAnswer.WithoutPreSharedKey;
        var psk = withPreSharedKey ? Convert.FromHexString(KnownAnswer.PreSharedKey) : null;
        var transferId = Guid.Parse(KnownAnswer.TransferId);
        var senderId = Guid.Parse(KnownAnswer.SenderId);
        var receiverId = Guid.Parse(KnownAnswer.ReceiverId);

        using var sender = NewCipher(isSender: true, transferId, senderId, receiverId, psk, FixedEphemeral(KnownAnswer.SenderPrivateKey));
        using var receiver = NewCipher(isSender: false, transferId, senderId, receiverId, psk, FixedEphemeral(KnownAnswer.ReceiverPrivateKey));

        var ping = Ping(sender);
        Assert.Equal(expected.Ping, Convert.ToHexString(ping));
        Assert.True(receiver.TryAcceptPing(ping));
        var pong = Pong(receiver);
        Assert.Equal(expected.Pong, Convert.ToHexString(pong));
        Assert.True(sender.TryAcceptPong(pong));

        var plaintext = Encoding.ASCII.GetBytes(KnownAnswer.Plaintext);
        var header = new ChunkHeader
        {
            Version = ChunkHeader.CurrentVersion,
            Type = SealedBatch,
            PayloadLength = plaintext.Length + EndToEndCipher.TagSize,
            BatchId = KnownAnswer.BatchId,
            Count = KnownAnswer.Count,
            TransferId = transferId,
        };
        var wireHeader = new byte[ChunkHeader.Size];
        header.WriteTo(wireHeader);
        Assert.Equal(expected.SealedHeader, Convert.ToHexString(wireHeader));

        var sealedPayload = new byte[header.PayloadLength];
        sender.Seal(header, plaintext, sealedPayload);
        Assert.Equal(expected.SealedPayload, Convert.ToHexString(sealedPayload));
        var opened = new byte[plaintext.Length];
        Assert.True(receiver.TryOpen(header, sealedPayload, opened));
        Assert.Equal(plaintext, opened);

        var senderReceipt = Receipt(sender, KnownAnswer.BatchId);
        var receiverReceipt = Receipt(receiver, KnownAnswer.BatchId);
        Assert.Equal(expected.SenderReceipt, Convert.ToHexString(senderReceipt));
        Assert.Equal(expected.ReceiverReceipt, Convert.ToHexString(receiverReceipt));
        Assert.True(receiver.VerifyPeerReceipt(KnownAnswer.BatchId, senderReceipt));
        Assert.True(sender.VerifyPeerReceipt(KnownAnswer.BatchId, receiverReceipt));
    }

    /// <summary>The inputs and outputs of the script; its values, copied as it prints them.</summary>
    private static class KnownAnswer
    {
        public const string SenderPrivateKey = "6C0B4B6A2D1F3E5A7C9E8D7B6A5F4E3D2C1B0A99887766554433221100FFEEDD";
        public const string ReceiverPrivateKey = "1D2C3B4A59687786950A1B2C3D4E5F60718293A4B5C6D7E8F901122334455667";
        public const string TransferId = "0f1e2d3c-4b5a-6978-8796-a5b4c3d2e1f0";
        public const string SenderId = "a0a1a2a3-b0b1-c0c1-d0d1-d2d3d4d5d6d7";
        public const string ReceiverId = "e0e1e2e3-f0f1-0010-1112-131415161718";
        public const string PreSharedKey = "000102030405060708090A0B0C0D0E0F101112131415161718191A1B1C1D1E1F";
        public const string Plaintext = "TransportR known-answer batch";
        public const int BatchId = 0x01020304;
        public const int Count = 3;

        public static readonly Vector WithoutPreSharedKey = new(
            Ping: "046B46386A43E4DE2AE1F28625599E677DEA82E97BB03CC10E6A4B37F6F934A1AD001956A10AD25986BFAE42E0A9A8941151A9736F60FE747F65EB4B5054AB0341",
            Pong: "04CB41CC669DAD5137068331728B34806AC10A52E5EBFB536F5520D09B69007A4EB29FA1F77DFE236EF991B1835BE2409BEC73A65A1DE55287323EDD829B48054DE74E1FD757F26FCF63BB35CA7B9C3CE2D4A012869BA973A92BA7A42B9717072C",
            SealedHeader: "02092D00000004030201030000003C2D1E0F5A4B78698796A5B4C3D2E1F0",
            SealedPayload: "A1EA925ECCB87C39C9B0ECB7B2C2129226FFC866647705657EC077239A4C8FE912EEFBDFAAE1A88E366767080E",
            SenderReceipt: "158CF27775C1F08694F82839871666A04DF8342244213E2CE87E88E2815156E0",
            ReceiverReceipt: "12707A719408FC08DAD803DE5889FDE96D5DD3DC539E9D434DE8DC2D366A5882");

        public static readonly Vector WithPreSharedKey = new(
            Ping: "046B46386A43E4DE2AE1F28625599E677DEA82E97BB03CC10E6A4B37F6F934A1AD001956A10AD25986BFAE42E0A9A8941151A9736F60FE747F65EB4B5054AB0341",
            Pong: "04CB41CC669DAD5137068331728B34806AC10A52E5EBFB536F5520D09B69007A4EB29FA1F77DFE236EF991B1835BE2409BEC73A65A1DE55287323EDD829B48054D1DA163C8A3152D684BBD3730C3975D192A9846A72AA80880D04CB83965E6C695",
            SealedHeader: "02092D00000004030201030000003C2D1E0F5A4B78698796A5B4C3D2E1F0",
            SealedPayload: "1F3BA61B5AEEB032E77416B4D8CA7063F78451DDE79F80B56E1AE283BFD62CC747DCBA2B61C1D1A9D47FA3C853",
            SenderReceipt: "BA9DB56107E542C7696542ADE61CD8A490982781E4D642EB12BE6C70E502AD47",
            ReceiverReceipt: "C9E276DF219ACFF6B2860AABF6B17D6589FC34AFCC270DD22644B109FFF1B429");

        public sealed record Vector(
            string Ping, string Pong, string SealedHeader, string SealedPayload, string SenderReceipt, string ReceiverReceipt);
    }

    /// <summary>
    /// The cipher's private constructor, the only one that takes a key pair instead of drawing it:
    /// nothing the client opens its internals to can reach it.
    /// </summary>
    [UnsafeAccessor(UnsafeAccessorKind.Constructor)]
    private static extern EndToEndCipher NewCipher(
        bool isSender, Guid transferId, Guid senderId, Guid receiverId, byte[]? preSharedKey, ECDiffieHellman ephemeral);

    /// <summary>
    /// The private key alone, so that the platform computes the public point and the Ping compared
    /// with the vector is not the point the test gave it.
    /// </summary>
    private static ECDiffieHellman FixedEphemeral(string privateKey) => ECDiffieHellman.Create(new ECParameters
    {
        Curve = ECCurve.NamedCurves.nistP256,
        D = Convert.FromHexString(privateKey),
    });

    // ---- Helpers ----------------------------------------------------------------------------------

    private sealed record SpecifiedKeys(
        byte[] Batch, byte[] ConfirmationKey, byte[] SenderReceipt, byte[] ReceiverReceipt, byte[] Confirmation);

    private SpecifiedKeys DeriveAsSpecified(
        ECDiffieHellman ownKey, byte[] peerShare, byte[]? psk, byte[] senderShare, byte[] receiverShare)
    {
        using var peer = ECDiffieHellman.Create(new ECParameters
        {
            Curve = ECCurve.NamedCurves.nistP256,
            Q = new ECPoint { X = peerShare[1..33], Y = peerShare[33..65] },
        });
        var secret = ownKey.DeriveRawSecretAgreement(peer.PublicKey);

        var protocol = Encoding.ASCII.GetBytes("transportr e2e v1 P-256 HKDF-SHA256 AES-256-GCM");
        byte[] transcript =
        [
            (byte)protocol.Length, .. protocol,
            psk is null ? (byte)0x00 : (byte)0x01,
            .. _transferId.ToByteArray(),
            .. _senderId.ToByteArray(),
            .. _receiverId.ToByteArray(),
            .. senderShare,
            .. receiverShare,
        ];

        var prk = HKDF.Extract(HashAlgorithmName.SHA256, secret, psk ?? new byte[32]);
        byte[] Expand(string label) =>
            HKDF.Expand(HashAlgorithmName.SHA256, prk, 32, [.. Encoding.ASCII.GetBytes(label), .. transcript]);

        var confirmationKey = Expand("transportr e2e confirmation");
        return new SpecifiedKeys(
            Batch: Expand("transportr e2e batch"),
            ConfirmationKey: confirmationKey,
            SenderReceipt: Expand("transportr e2e sender receipt"),
            ReceiverReceipt: Expand("transportr e2e receiver receipt"),
            Confirmation: HMACSHA256.HashData(confirmationKey, transcript));
    }

    private static byte[] ReceiptInput(Guid transferId, int lastBatchId)
    {
        var input = new byte[20];
        transferId.TryWriteBytes(input);
        BinaryPrimitives.WriteInt32LittleEndian(input.AsSpan(16), lastBatchId);
        return input;
    }

    private static byte[] Share(ECDiffieHellman key)
    {
        var q = key.ExportParameters(false).Q;
        return [0x04, .. q.X!, .. q.Y!];
    }

    private (EndToEndCipher Sender, EndToEndCipher Receiver) Connected(byte[]? psk = null)
    {
        var sender = EndToEndCipher.ForSender(_transferId, _senderId, _receiverId, psk);
        var receiver = EndToEndCipher.ForReceiver(_transferId, _senderId, _receiverId, psk);
        Assert.True(Handshake(sender, receiver));
        return (sender, receiver);
    }

    private static bool Handshake(EndToEndCipher sender, EndToEndCipher receiver)
    {
        Assert.True(receiver.TryAcceptPing(Ping(sender)));
        return sender.TryAcceptPong(Pong(receiver));
    }

    private static byte[] Ping(EndToEndCipher sender)
    {
        var payload = new byte[EndToEndCipher.PingPayloadSize];
        sender.WritePing(payload);
        return payload;
    }

    private static byte[] Pong(EndToEndCipher receiver)
    {
        var payload = new byte[EndToEndCipher.PongPayloadSize];
        receiver.WritePong(payload);
        return payload;
    }

    private static byte[] Receipt(EndToEndCipher cipher, int lastBatchId)
    {
        var receipt = new byte[EndToEndCipher.ReceiptSize];
        cipher.WriteReceipt(lastBatchId, receipt);
        return receipt;
    }

    private ChunkHeader Header(int batchId, int count, int plaintextLength) => new()
    {
        Version = ChunkHeader.CurrentVersion,
        Type = SealedBatch,
        PayloadLength = plaintextLength + EndToEndCipher.TagSize,
        BatchId = batchId,
        Count = count,
        TransferId = _transferId,
    };

    private (ChunkHeader Header, byte[] SealedPayload) SealOne(EndToEndCipher sender, int plaintextLength)
    {
        var header = Header(batchId: 0, count: 1, plaintextLength);
        var sealedPayload = new byte[header.PayloadLength];
        sender.Seal(header, RandomNumberGenerator.GetBytes(plaintextLength), sealedPayload);
        return (header, sealedPayload);
    }

    private static void AssertNotIn(string text, byte[] secret)
    {
        Assert.DoesNotContain(Convert.ToHexString(secret), text, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain(Convert.ToBase64String(secret), text, StringComparison.Ordinal);
    }
}
