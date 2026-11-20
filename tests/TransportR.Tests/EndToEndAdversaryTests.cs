using System.Diagnostics;
using System.Security.Cryptography;
using System.Text;
using Microsoft.Extensions.Logging;
using TransportR.Client;
using TransportR.Client.Configuration;
using TransportR.Client.Protocol;
using TransportR.Client.Sessions;
using TransportR.Tests.TestHelpers;
using static TransportR.Tests.EndToEndSessionTests;

namespace TransportR.Tests;

/// <summary>
/// End-to-end encryption under attack: a hub that reads, rewrites, holds back, replays, reorders and
/// redirects what two sessions write without holding the pre-shared key, an end that has no key, and a
/// release racing the cipher. Each test shows an attack failing.
/// </summary>
public class EndToEndAdversaryTests
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(15);

    private static readonly byte[] DomainKey = Enumerable.Range(7, 32).Select(i => (byte)i).ToArray();

    // ---- Receipts carried over from another transfer ----

    [Fact]
    public async Task ACompleteTakenFromAnotherTransfer_UnderTheSameDomainKey_IsRefusedByTheReceiver()
    {
        var earlier = await CompletedTransferAsync();
        var complete = earlier.SenderWrote.First(chunk => chunk.Header.Type == ChunkType.Complete);

        await using var pair = new SessionPair(sender: Keyed(), receiver: Keyed());
        pair.Hub.Rewrite = chunk => chunk.Header.Type == ChunkType.Complete
            ? [WithTransferId(complete.Bytes, Guid.Parse(pair.TransferId))]
            : [chunk.Bytes];
        var outcome = await pair.RunAsync(items: 2);

        Assert.Equal(TransferFailureCategory.EndToEndEncryptionFailed, outcome.Receiver?.Category);
        Assert.Equal(TransferFailureCategory.AbandonedByPeer, outcome.Sender?.Category);
        Assert.DoesNotContain(pair.Hub.ReceiverWrote, chunk => chunk.Header.Type == ChunkType.CompleteAck);
    }

    [Fact]
    public async Task ACompleteAckTakenFromAnotherTransfer_UnderTheSameDomainKey_IsRefusedByTheSender()
    {
        var earlier = await CompletedTransferAsync();
        var acknowledgement = earlier.ReceiverWrote.First(chunk => chunk.Header.Type == ChunkType.CompleteAck);

        await using var pair = new SessionPair(sender: Keyed(), receiver: Keyed());
        pair.Hub.Rewrite = chunk =>
        {
            if (chunk.Header.Type != ChunkType.Complete) return [chunk.Bytes];
            pair.Hub.Inject(toSender: true, WithTransferId(acknowledgement.Bytes, Guid.Parse(pair.TransferId)));
            return [];
        };
        var outcome = await pair.RunAsync(items: 2, waitForBoth: false, reader: End.Sender);

        Assert.Equal(TransferFailureCategory.EndToEndEncryptionFailed, outcome.Sender?.Category);
    }

    // ---- Identities and redirection ----

    public static TheoryData<string> Misdirections => new() { "another sender announced", "another receiver reached", "the streams of two transfers crossed" };

    [Theory]
    [MemberData(nameof(Misdirections))]
    public async Task AHubThatMisdirectsATransfer_FailsItAtTheConfirmation_UnderOneKeyPerDomain(string misdirection)
    {
        var parties = new TransferParties(Guid.NewGuid(), Guid.NewGuid());
        var receiverParties = misdirection switch
        {
            "another sender announced" => parties with { SenderId = Guid.NewGuid() },
            "another receiver reached" => parties with { ReceiverId = Guid.NewGuid() },
            _ => parties,
        };
        var receiverTransferId = misdirection == "the streams of two transfers crossed" ? Guid.NewGuid().ToString() : null;

        await using var duo = new Duo(Keyed(), Keyed(), parties, receiverParties, receiverTransferId: receiverTransferId);
        var outcome = await duo.RunAsync(items: 2);

        Assert.Equal(TransferFailureCategory.EndToEndEncryptionFailed, outcome.Sender?.Category);
        Assert.Equal(TransferFailureCategory.AbandonedByPeer, outcome.Receiver?.Category);
        Assert.Equal(AbortReason.VerificationFailed.Describe(), outcome.Receiver?.Message);
        Assert.DoesNotContain(duo.Hub.SenderWrote, IsBatch);
        Assert.Equal(0, outcome.Received);
    }

    // ---- Downgrade ----

    [Theory]
    [InlineData((byte)ChunkType.Ping)]
    [InlineData((byte)ChunkType.Pong)]
    public async Task AShareTheHubStrips_IsTakenForAPeerInClear_AndNothingIsSealed(byte carryingType)
    {
        var carrying = (ChunkType)carryingType;
        await using var pair = new SessionPair(sender: Keyed(), receiver: Keyed());
        pair.Hub.Rewrite = chunk => chunk.Header.Type == carrying ? [Resized(chunk.Bytes, 0)] : [chunk.Bytes];
        var outcome = await pair.RunAsync(items: 2);

        var (refusing, abandoned) = carrying == ChunkType.Ping ? (outcome.Receiver, outcome.Sender) : (outcome.Sender, outcome.Receiver);
        Assert.Equal(TransferFailureCategory.EndToEndEncryptionFailed, refusing?.Category);
        Assert.Equal(TransferFailureCategory.AbandonedByPeer, abandoned?.Category);
        Assert.Equal(AbortReason.EncryptionModeMismatch.Describe(), abandoned?.Message);
        Assert.DoesNotContain(pair.Hub.SenderWrote, IsBatch);
        Assert.Equal(0, outcome.Received);
    }

    // ---- Bounds and order ----

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(15)]
    [InlineData(16)]
    public async Task ASealedBatchCutShorterThanItsTag_IsRefusedAsUnverified_NeverAcknowledged(int length)
    {
        await using var pair = new SessionPair(sender: Keyed(), receiver: Keyed());
        pair.Hub.Rewrite = chunk => chunk.Header.Type == ChunkType.SealedBatch && chunk.Header.BatchId == 0
            ? [Resized(chunk.Bytes, length)]
            : [chunk.Bytes];
        var outcome = await pair.RunAsync(items: 2);

        Assert.Equal(TransferFailureCategory.EndToEndEncryptionFailed, outcome.Receiver?.Category);
        Assert.Equal(TransferFailureCategory.AbandonedByPeer, outcome.Sender?.Category);
        Assert.Equal(AbortReason.VerificationFailed.Describe(), outcome.Sender?.Message);
        Assert.DoesNotContain(pair.Hub.ReceiverWrote, chunk => chunk.Header.Type == ChunkType.BatchAck);
        Assert.Equal(0, outcome.Received);
    }

    [Fact]
    public async Task ASealedBatchTheHubPutsAheadOfThePing_IsRefused_BeforeAnyKeyIsDerived()
    {
        await using var pair = new SessionPair(sender: Keyed(), receiver: Keyed());
        var forged = SealedChunk(Guid.Parse(pair.TransferId), batchId: 0, RandomNumberGenerator.GetBytes(5 + EndToEndCipher.TagSize));
        var first = 0;
        pair.Hub.Rewrite = chunk => chunk.Header.Type == ChunkType.Ping && Interlocked.Exchange(ref first, 1) == 0
            ? [forged, chunk.Bytes]
            : [chunk.Bytes];
        var outcome = await pair.RunAsync(items: 1);

        Assert.Equal(TransferFailureCategory.EndToEndEncryptionFailed, outcome.Receiver?.Category);
        Assert.Equal(TransferFailureCategory.AbandonedByPeer, outcome.Sender?.Category);
        Assert.Equal(AbortReason.VerificationFailed.Describe(), outcome.Sender?.Message);
        Assert.DoesNotContain(pair.Hub.ReceiverWrote, chunk => chunk.Header.Type is ChunkType.Pong or ChunkType.BatchAck);
    }

    public static TheoryData<string> MisplacedControlChunks => new()
    {
        "a BatchAck carrying a byte, to the sender",
        "an Abort carrying bytes, to the sender",
        "the sender's Ping reflected to it",
        "the sender's Complete reflected to it",
        "the receiver's Pong reflected to it",
    };

    [Theory]
    [MemberData(nameof(MisplacedControlChunks))]
    public async Task AControlChunkAStreamDoesNotCarry_OrCarriesWithAPayload_IsAProtocolViolation(string misplaced)
    {
        await using var pair = new SessionPair(sender: Keyed(), receiver: Keyed());
        var transferId = Guid.Parse(pair.TransferId);
        var readBySender = !misplaced.StartsWith("the receiver's");
        pair.Hub.Rewrite = chunk =>
        {
            switch (misplaced)
            {
                case "a BatchAck carrying a byte, to the sender" when chunk.Header.Type == ChunkType.BatchAck:
                    return [Resized(chunk.Bytes, 1)];
                case "an Abort carrying bytes, to the sender" when chunk.Header.Type == ChunkType.Pong:
                    pair.Hub.Inject(toSender: true, Control(transferId, ChunkType.Abort, (int)AbortReason.VerificationFailed, new byte[4]));
                    return [];
                case "the sender's Ping reflected to it" when chunk.Header.Type == ChunkType.Ping:
                case "the sender's Complete reflected to it" when chunk.Header.Type == ChunkType.Complete:
                    pair.Hub.Inject(toSender: true, chunk.Bytes);
                    return [];
                case "the receiver's Pong reflected to it" when chunk.Header.Type == ChunkType.Pong:
                    pair.Hub.Inject(toSender: false, chunk.Bytes);
                    return [];
                default:
                    return [chunk.Bytes];
            }
        };
        var outcome = await pair.RunAsync(items: 1, waitForBoth: false, reader: readBySender ? End.Sender : End.Receiver);

        Assert.Equal(TransferFailureCategory.ProtocolViolation, (readBySender ? outcome.Sender : outcome.Receiver)?.Category);
    }

    [Fact]
    public async Task ASealedBatchOnTheSendersControlStream_IsAProtocolViolation()
    {
        await using var pair = new SessionPair(sender: Keyed(), receiver: Keyed());
        var injected = 0;
        pair.Hub.Rewrite = chunk =>
        {
            if (chunk.Header.Type == ChunkType.SealedBatch && Interlocked.Exchange(ref injected, 1) == 0) pair.Hub.Inject(toSender: true, chunk.Bytes);
            return [chunk.Bytes];
        };
        var outcome = await pair.RunAsync(items: 2, waitForBoth: false, reader: End.Sender);

        Assert.Equal(TransferFailureCategory.ProtocolViolation, outcome.Sender?.Category);
    }

    public static TheoryData<string, bool> ChunksOfTheOtherDirection
    {
        get
        {
            var data = new TheoryData<string, bool>();
            foreach (var misplaced in new[]
            {
                "a Complete, to the sender", "a Ping, to the sender", "a batch, to the sender",
                "a Pong, to the receiver", "a BatchAck, to the receiver", "a CompleteAck, to the receiver",
            })
            {
                data.Add(misplaced, false);
                data.Add(misplaced, true);
            }

            return data;
        }
    }

    /// <summary>
    /// A chunk only the other end reads, without a payload, forged as the sender's first batch passes: the
    /// Complete's BatchId -1 is one a sender with no receiver state of its own would take as complete.
    /// </summary>
    [Theory]
    [MemberData(nameof(ChunksOfTheOtherDirection))]
    public async Task AChunkOfATypeTheStreamDoesNotCarry_IsAProtocolViolation_InClearAsEncrypted(string misplaced, bool encrypted)
    {
        await using var pair = new SessionPair(sender: encrypted ? Keyed() : null, receiver: encrypted ? Keyed() : null);
        var transferId = Guid.Parse(pair.TransferId);
        var toSender = misplaced.EndsWith("to the sender");
        var forged = misplaced switch
        {
            "a Complete, to the sender" => Control(transferId, ChunkType.Complete, -1, []),
            "a Ping, to the sender" => Control(transferId, ChunkType.Ping, 0, []),
            "a batch, to the sender" => encrypted ? SealedChunk(transferId, 0, []) : Retyped(SealedChunk(transferId, 0, []), ChunkType.Batch),
            "a Pong, to the receiver" => Control(transferId, ChunkType.Pong, 0, []),
            "a BatchAck, to the receiver" => Control(transferId, ChunkType.BatchAck, 0, []),
            _ => Control(transferId, ChunkType.CompleteAck, 0, []),
        };
        var injected = 0;
        pair.Hub.Rewrite = chunk =>
        {
            if (IsBatch(chunk) && Interlocked.Exchange(ref injected, 1) == 0) pair.Hub.Inject(toSender, forged);
            return [chunk.Bytes];
        };
        var outcome = await pair.RunAsync(items: 2, waitForBoth: false, reader: toSender ? End.Sender : End.Receiver);

        Assert.Equal(TransferFailureCategory.ProtocolViolation, (toSender ? outcome.Sender : outcome.Receiver)?.Category);
    }

    [Fact]
    public async Task AKeepalivePongWhoseConfirmationTheHubAlters_IsRefused_ByTheSender()
    {
        await using var pair = new SessionPair(sender: Keyed(), receiver: Keyed(), configureSender: options =>
            options.PresenceHeartbeatInterval = TimeSpan.FromMilliseconds(200));
        var pongs = 0;
        pair.Hub.Rewrite = chunk => chunk.Header.Type == ChunkType.Pong && Interlocked.Increment(ref pongs) > 1
            ? [Flipped(chunk.Bytes, chunk.Bytes.Length - 1)]
            : [chunk.Bytes];

        await pair.StartAsync();
        var receiving = pair.ReceiveAsync();
        await pair.Sender.SendAsync("item", CancellationToken.None);
        var refused = await Assert.ThrowsAsync<TransferFailedException>(async () =>
        {
            await WaitUntilAsync(() => pair.Sender.State == Client.Models.SessionState.Failed);
            await pair.Sender.CompleteAsync().WaitAsync(Bound);
        });
        var abandoned = await Assert.ThrowsAsync<TransferFailedException>(() => receiving.WaitAsync(Bound));

        Assert.Equal(TransferFailureCategory.EndToEndEncryptionFailed, refused.Category);
        Assert.Equal(TransferFailureCategory.AbandonedByPeer, abandoned.Category);
        Assert.Equal(AbortReason.VerificationFailed.Describe(), abandoned.Message);
    }

    [Fact]
    public async Task HeaderFieldsOfControlChunksThatNoReceiptCovers_AlteredByTheHub_ChangeNothingDelivered()
    {
        await using var pair = new SessionPair(sender: Keyed(), receiver: Keyed());
        pair.Hub.Rewrite = chunk =>
        {
            if (IsBatch(chunk) || chunk.Header.Type == ChunkType.BatchAck) return [chunk.Bytes];
            var header = chunk.Header with { Count = 0x7F, TransferId = Guid.NewGuid() };
            if (chunk.Header.Type is ChunkType.Ping or ChunkType.CompleteAck) header = header with { BatchId = 12345 };
            var copy = (byte[])chunk.Bytes.Clone();
            header.WriteTo(copy);
            return [copy];
        };
        var outcome = await pair.RunAsync(items: 3);

        Assert.Null(outcome.Sender);
        Assert.Null(outcome.Receiver);
        Assert.Equal(3, outcome.Received);
    }

    // ---- Refusals and their timing ----

    [Fact]
    public async Task APongRefusedWhileTheSenderWaitsForTheHandshake_LetsItsAbortReachTheReceiver_WithinTwoSeconds()
    {
        await using var duo = new Duo(Keyed(), Keyed());
        var dropped = DropFirstStreamBatchesFrom(duo.Hub, batchId: 1, count: 1);
        var relay = duo.Hub.Rewrite!;
        var pongs = 0;
        duo.Hub.Rewrite = chunk => chunk.Header.Type == ChunkType.Pong && Interlocked.Increment(ref pongs) > 1
            ? [WithShare(chunk.Bytes, NewShare())]
            : relay(chunk);

        await duo.StartAsync();
        var receiving = duo.ReceiveAsync();
        for (var i = 0; i < 2; i++) await duo.Sender.SendAsync($"item-{i}", CancellationToken.None);
        await dropped.WaitAsync(Bound);
        var cutAt = Stopwatch.StartNew();
        duo.Sender.TriggerReconnect();

        var abandoned = await Assert.ThrowsAsync<TransferFailedException>(() => receiving.WaitAsync(Bound));
        var abandonedAfter = cutAt.Elapsed;
        var refused = await Assert.ThrowsAsync<TransferFailedException>(() => duo.Sender.CompleteAsync().WaitAsync(Bound));

        Assert.Equal(TransferFailureCategory.EndToEndEncryptionFailed, refused.Category);
        Assert.Equal(TransferFailureCategory.AbandonedByPeer, abandoned.Category);
        Assert.Equal(AbortReason.VerificationFailed.Describe(), abandoned.Message);
        Assert.True(abandonedAfter < TimeSpan.FromSeconds(2), $"The receiver learnt of the refusal after {abandonedAfter}.");
    }

    [Fact]
    public async Task APongRefusedWhileTheSenderSeals_FailsTheSender_ForTheEncryption()
    {
        await using var pair = new SessionPair(sender: Keyed(), receiver: Keyed());
        var pongs = 0;
        pair.Hub.Rewrite = chunk => chunk.Header.Type == ChunkType.Pong && Interlocked.Increment(ref pongs) > 1
            ? [WithShare(chunk.Bytes, NewShare())]
            : [chunk.Bytes];
        await pair.StartAsync();
        var receiving = pair.ReceiveAsync();
        var sent = 0;
        var producing = Task.Run(async () =>
        {
            while (true)
            {
                await pair.Sender.SendAsync("item", CancellationToken.None);
                Interlocked.Increment(ref sent);
            }
        });
        await WaitUntilAsync(() => Volatile.Read(ref sent) >= 200);
        pair.Sender.TriggerReconnect();

        await Assert.ThrowsAsync<TransferFailedException>(() => producing.WaitAsync(Bound));
        await Assert.ThrowsAsync<TransferFailedException>(() => receiving.WaitAsync(Bound));
        var refused = await Assert.ThrowsAsync<TransferFailedException>(() => pair.Sender.CompleteAsync().WaitAsync(Bound));
        Assert.Equal(TransferFailureCategory.EndToEndEncryptionFailed, refused.Category);
    }

    [Fact]
    public async Task AHubThatSuppressesEveryPong_FailsTheSenderOnTheAcknowledgementTimeout_WithNothingSealed()
    {
        await using var pair = new SessionPair(sender: Keyed(), receiver: Keyed(), configureSender: options =>
            options.CompleteStallTimeout = TimeSpan.FromSeconds(1.5));
        pair.Hub.Rewrite = chunk => chunk.Header.Type == ChunkType.Pong ? [] : [chunk.Bytes];
        var outcome = await pair.RunAsync(items: 2);

        Assert.Equal(TransferFailureCategory.AcknowledgementTimedOut, outcome.Sender?.Category);
        Assert.Equal(TransferFailureCategory.AbandonedByPeer, outcome.Receiver?.Category);
        Assert.DoesNotContain(pair.Hub.SenderWrote, IsBatch);
        Assert.Equal(0, outcome.Received);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task AnEndWithoutAKey_WritesOneAbort_OfReasonSix_AndNothingElse_WhicheverStreamRefusesFirst(bool atTheSender)
    {
        for (var run = 0; run < 10; run++)
        {
            var refusing = new EndToEndEncryption(_ => null);
            await using var pair = new SessionPair(sender: atTheSender ? refusing : Keyed(), receiver: atTheSender ? Keyed() : refusing);
            var outcome = await pair.RunAsync(items: 2);

            var (refused, peer) = atTheSender ? (outcome.Sender, outcome.Receiver) : (outcome.Receiver, outcome.Sender);
            var wrote = atTheSender ? pair.Hub.SenderWrote : pair.Hub.ReceiverWrote;
            Assert.Equal(TransferFailureCategory.EndToEndEncryptionFailed, refused?.Category);
            Assert.Equal(TransferFailureCategory.AbandonedByPeer, peer?.Category);
            Assert.Equal(AbortReason.VerificationFailed.Describe(), peer?.Message);
            Assert.Equal([(ChunkType.Abort, 6, 0)], wrote.Select(chunk => (chunk.Header.Type, chunk.Header.BatchId, chunk.Header.PayloadLength)));
        }
    }

    // ---- Nonces ----

    [Fact]
    public async Task AcrossThreeReconnections_EachBatchIdIsSealedOnce_EveryWriteOfItTheSameBytes()
    {
        const int perStream = 60;
        const int items = 4 * perStream;
        await using var pair = new SessionPair(sender: Keyed(), receiver: Keyed(), configureSender: options =>
            options.MaxInFlightBatches = 8);

        // From a point in each of the first three streams the hub keeps the batches, as a cut would,
        // so that the stream after it resends them.
        var cutEpoch = -1;
        var kept = 0;
        pair.Hub.Rewrite = chunk =>
        {
            if (!(chunk.FromSender && IsBatch(chunk) && chunk.Epoch == Volatile.Read(ref cutEpoch))) return [chunk.Bytes];
            Interlocked.Increment(ref kept);
            return [];
        };

        await pair.StartAsync();
        var receiving = pair.ReceiveAsync();
        var next = 0;
        for (var epoch = 0; epoch < 3; epoch++)
        {
            while (next < (epoch + 1) * perStream - 5) await pair.Sender.SendAsync($"item-{next++}", CancellationToken.None);
            await WaitUntilAsync(() => pair.Received >= next);

            var keptBefore = Volatile.Read(ref kept);
            Volatile.Write(ref cutEpoch, epoch);
            while (next < (epoch + 1) * perStream) await pair.Sender.SendAsync($"item-{next++}", CancellationToken.None);
            await WaitUntilAsync(() => Volatile.Read(ref kept) >= keptBefore + 5);

            // Sent on at once: the worker seals the next batches while the stream is reopened.
            pair.Sender.TriggerReconnect();
        }

        while (next < items) await pair.Sender.SendAsync($"item-{next++}", CancellationToken.None);
        await pair.Sender.CompleteAsync().WaitAsync(Bound);
        await receiving.WaitAsync(Bound);

        var sealedBatches = pair.Hub.SenderWrote.Where(IsBatch).ToList();
        Assert.All(sealedBatches, batch => Assert.Equal(ChunkType.SealedBatch, batch.Header.Type));
        var byId = sealedBatches.GroupBy(batch => batch.Header.BatchId).ToList();
        Assert.Equal(Enumerable.Range(0, items), byId.Select(group => group.Key).Order());
        Assert.All(byId, group => Assert.Single(group.Select(batch => Convert.ToHexString(batch.Bytes)).Distinct()));
        Assert.True(sealedBatches.Count >= items + 15, $"{sealedBatches.Count - items} batches were written again, where 15 were kept.");
        Assert.Equal(items, pair.Received);
    }

    // ---- Secrets ----

    [Fact]
    public async Task NoLogLine_NorFailureMessage_OfAnEncryptedTransfer_CarriesThePreSharedKey()
    {
        await using (var succeeding = new Duo(Keyed(), Keyed()))
        {
            var outcome = await succeeding.RunAsync(items: 3);
            Assert.Null(outcome.Sender);
            Assert.Null(outcome.Receiver);
            AssertCarriesNoKey(succeeding.Logged, DomainKey);
        }

        var otherKey = DomainKey.Select(b => (byte)(b ^ 0x5A)).ToArray();
        await using (var refused = new Duo(Keyed(), new EndToEndEncryption(_ => otherKey)))
        {
            var outcome = await refused.RunAsync(items: 3);
            Assert.Equal(TransferFailureCategory.EndToEndEncryptionFailed, outcome.Sender?.Category);
            AssertCarriesNoKey([.. refused.Logged, outcome.Sender!.ToString(), outcome.Receiver!.ToString()], DomainKey);
            AssertCarriesNoKey([.. refused.Logged, outcome.Sender!.ToString(), outcome.Receiver!.ToString()], otherKey);
        }
    }

    [Fact]
    public async Task AProviderThatThrowsWithItsKeyInItsMessage_FailsTheTransfer_WithAMessageThatCarriesNoKey()
    {
        var provider = new EndToEndEncryption(_ =>
            throw new InvalidOperationException($"The vault refused key {Convert.ToHexString(DomainKey)} for this peer."));
        await using var duo = new Duo(provider, Keyed());
        var outcome = await duo.RunAsync(items: 1);

        Assert.Equal(TransferFailureCategory.EndToEndEncryptionFailed, outcome.Sender?.Category);
        Assert.Equal(TransferFailureCategory.AbandonedByPeer, outcome.Receiver?.Category);
        AssertCarriesNoKey([outcome.Sender!.ToString(), outcome.Receiver!.ToString()], DomainKey);
    }

    // ---- The cipher, released under a seal ----

    [Fact]
    public void ACipherReleasedWhileItSeals_EitherRefusesOrSealsUnderItsOwnKey_NeverUnderAnErasedOne()
    {
        var transferId = Guid.NewGuid();
        var plaintext = RandomNumberGenerator.GetBytes(4096);
        var header = ChunkHeader.CreateBatch(transferId, 0, 1, plaintext.Length + EndToEndCipher.TagSize) with { Type = ChunkType.SealedBatch };
        var (sealedWhole, refused) = (0, 0);

        for (var trial = 0; trial < 5_000; trial++)
        {
            using var receiver = EndToEndCipher.ForReceiver(transferId, Guid.Empty, Guid.Empty, null);
            var sender = EndToEndCipher.ForSender(transferId, Guid.Empty, Guid.Empty, null);
            var ping = new byte[EndToEndCipher.PingPayloadSize];
            sender.WritePing(ping);
            Assert.True(receiver.TryAcceptPing(ping));
            var pong = new byte[EndToEndCipher.PongPayloadSize];
            receiver.WritePong(pong);
            Assert.True(sender.TryAcceptPong(pong));

            var sealedPayload = new byte[header.PayloadLength];
            var sealedThisTrial = false;
            using var go = new Barrier(2);
            var sealing = new Thread(() =>
            {
                go.SignalAndWait();
                try
                {
                    sender.Seal(header, plaintext, sealedPayload);
                    sealedThisTrial = true;
                }
                catch (InvalidOperationException)
                {
                    // Disposed, or its keys already taken by the release: refused either way.
                }
            });
            sealing.Start();
            go.SignalAndWait();
            Thread.SpinWait(trial % 64);
            sender.Dispose();
            sealing.Join();

            if (!sealedThisTrial)
            {
                refused++;
                continue;
            }

            sealedWhole++;
            Assert.True(receiver.TryOpen(header, sealedPayload, new byte[plaintext.Length]), $"Trial {trial} sealed under another key.");
        }

        Assert.True(sealedWhole > 0 && refused > 0, $"{sealedWhole} sealed, {refused} refused: the release never met the seal.");
    }

    [Fact]
    public void ACipherReleasedWhileItChecksAReceipt_NeverTakesOneSignedUnderAZeroKey()
    {
        var transferId = Guid.NewGuid();
        var input = new byte[20];
        transferId.TryWriteBytes(input);
        var forged = HMACSHA256.HashData(new byte[32], input); // BatchId 0, little-endian zeros
        var (accepted, trials) = (0, 0);
        for (var trial = 0; trial < 5_000; trial++, trials++)
        {
            using var sender = EndToEndCipher.ForSender(transferId, Guid.Empty, Guid.Empty, null);
            var receiver = EndToEndCipher.ForReceiver(transferId, Guid.Empty, Guid.Empty, null);
            var ping = new byte[EndToEndCipher.PingPayloadSize];
            sender.WritePing(ping);
            Assert.True(receiver.TryAcceptPing(ping));
            using var go = new Barrier(2);
            var checking = new Thread(() =>
            {
                go.SignalAndWait();
                try
                {
                    if (receiver.VerifyPeerReceipt(0, forged)) Interlocked.Increment(ref accepted);
                }
                catch (ObjectDisposedException)
                {
                    // Released before the check began: refused.
                }
            });
            checking.Start();
            go.SignalAndWait();
            Thread.SpinWait(trial % 64);
            receiver.Dispose();
            checking.Join();
        }

        Assert.True(accepted == 0, $"{accepted} of {trials} trials took the receipt.");
    }

    [Fact]
    public void ACipherReleasedWhileItSignsAReceipt_EitherRefusesOrSignsUnderItsOwnKey_NeverUnderAZeroOne()
    {
        var transferId = Guid.NewGuid();
        var (signedWhole, refused) = (0, 0);
        for (var trial = 0; trial < 5_000; trial++)
        {
            var receiver = EndToEndCipher.ForReceiver(transferId, Guid.Empty, Guid.Empty, null);
            using var sender = EndToEndCipher.ForSender(transferId, Guid.Empty, Guid.Empty, null);
            var ping = new byte[EndToEndCipher.PingPayloadSize];
            sender.WritePing(ping);
            Assert.True(receiver.TryAcceptPing(ping));
            var pong = new byte[EndToEndCipher.PongPayloadSize];
            receiver.WritePong(pong);
            Assert.True(sender.TryAcceptPong(pong));

            var receipt = new byte[EndToEndCipher.ReceiptSize];
            var signedThisTrial = false;
            using var go = new Barrier(2);
            var signing = new Thread(() =>
            {
                go.SignalAndWait();
                try
                {
                    receiver.WriteReceipt(0, receipt);
                    signedThisTrial = true;
                }
                catch (Exception ex) when (ex is ObjectDisposedException or InvalidOperationException)
                {
                    // Released before, or its keys already taken: refused either way.
                }
            });
            signing.Start();
            go.SignalAndWait();
            Thread.SpinWait(trial % 64);
            receiver.Dispose();
            signing.Join();

            if (!signedThisTrial)
            {
                refused++;
                continue;
            }

            signedWhole++;
            Assert.True(sender.VerifyPeerReceipt(0, receipt), $"Trial {trial} signed under another key.");
        }

        Assert.True(signedWhole > 0 && refused > 0, $"{signedWhole} signed, {refused} refused: the release never met the signature.");
    }

    // ---- In clear ----

    [Fact]
    public async Task InClear_APingThatCarriesAShare_IsAnsweredAsAlways_AndTheTransferSucceeds()
    {
        await using var pair = new SessionPair(sender: null, receiver: null);
        pair.Hub.Rewrite = chunk => chunk.Header.Type == ChunkType.Ping ? [Resized(chunk.Bytes, EndToEndCipher.PingPayloadSize)] : [chunk.Bytes];
        var outcome = await pair.RunAsync(items: 2);

        Assert.Null(outcome.Sender);
        Assert.Null(outcome.Receiver);
        Assert.Equal(2, outcome.Received);
        Assert.All(pair.Hub.ReceiverWrote, chunk => Assert.Equal(0, chunk.Header.PayloadLength));
    }

    // ---- Harness ----

    private static EndToEndEncryption Keyed() => new(_ => DomainKey);

    private static async Task<RelayHub> CompletedTransferAsync()
    {
        await using var pair = new SessionPair(sender: Keyed(), receiver: Keyed());
        var outcome = await pair.RunAsync(items: 2);
        Assert.Null(outcome.Sender);
        Assert.Null(outcome.Receiver);
        return pair.Hub;
    }

    private static void AssertCarriesNoKey(IEnumerable<string> texts, byte[] key)
    {
        var forms = new[] { Convert.ToHexString(key), Convert.ToBase64String(key), BitConverter.ToString(key) };
        foreach (var text in texts)
        {
            foreach (var form in forms)
            {
                Assert.DoesNotContain(form, text, StringComparison.OrdinalIgnoreCase);
            }
        }
    }

    private static bool IsBatch(WireChunk chunk) => ChunkIO.IsBatch(chunk.Header.Type);

    private static byte[] Control(Guid transferId, ChunkType type, int batchId, byte[] payload)
    {
        using var chunk = new MemoryStream();
        ChunkIO.WriteControlAsync(chunk, transferId, batchId, type, payload).GetAwaiter().GetResult();
        return chunk.ToArray();
    }

    private static byte[] SealedChunk(Guid transferId, int batchId, byte[] payload)
    {
        var header = ChunkHeader.CreateBatch(transferId, batchId, 1, payload.Length) with { Type = ChunkType.SealedBatch };
        var chunk = new byte[ChunkHeader.Size + payload.Length];
        header.WriteTo(chunk);
        payload.CopyTo(chunk, ChunkHeader.Size);
        return chunk;
    }

    private static byte[] WithTransferId(byte[] chunk, Guid transferId)
    {
        var copy = (byte[])chunk.Clone();
        transferId.TryWriteBytes(copy.AsSpan(14, 16));
        return copy;
    }

    private static byte[] Resized(byte[] chunk, int length)
    {
        var header = ChunkHeader.ReadFrom(chunk) with { PayloadLength = length };
        var copy = new byte[ChunkHeader.Size + length];
        header.WriteTo(copy);
        chunk.AsSpan(ChunkHeader.Size, Math.Min(length, chunk.Length - ChunkHeader.Size)).CopyTo(copy.AsSpan(ChunkHeader.Size));
        return copy;
    }

    private static byte[] Flipped(byte[] chunk, int at)
    {
        var copy = (byte[])chunk.Clone();
        copy[at] ^= 0x01;
        return copy;
    }

    private static byte[] Retyped(byte[] chunk, ChunkType type)
    {
        var copy = (byte[])chunk.Clone();
        copy[1] = (byte)type;
        return copy;
    }

    private static byte[] WithShare(byte[] chunk, byte[] share)
    {
        var copy = (byte[])chunk.Clone();
        share.CopyTo(copy, ChunkHeader.Size);
        return copy;
    }

    private static byte[] NewShare()
    {
        using var key = ECDiffieHellman.Create(ECCurve.NamedCurves.nistP256);
        var point = key.ExportParameters(includePrivateParameters: false).Q;
        return [0x04, .. point.X!, .. point.Y!];
    }

    private static Task DropFirstStreamBatchesFrom(RelayHub hub, int batchId, int count)
    {
        var dropped = 0;
        var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        hub.Rewrite = chunk =>
        {
            if (!(chunk.FromSender && chunk.Epoch == 0 && IsBatch(chunk) && chunk.Header.BatchId >= batchId)) return [chunk.Bytes];
            if (Interlocked.Increment(ref dropped) == count) done.TrySetResult();
            return [];
        };
        return done.Task;
    }

    private static async Task WaitUntilAsync(Func<bool> condition)
    {
        var deadline = DateTime.UtcNow + Bound;
        while (!condition())
        {
            Assert.True(DateTime.UtcNow < deadline, "The condition was not met in time.");
            await Task.Delay(10);
        }
    }

    /// <summary>Every line a session logs, its exception written out as a sink would.</summary>
    private sealed class CapturingLogger : ILogger
    {
        private readonly List<string> _lines = [];

        public IReadOnlyList<string> Lines
        {
            get { lock (_lines) return [.. _lines]; }
        }

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            var line = new StringBuilder(formatter(state, exception));
            if (exception is not null) line.AppendLine().Append(exception);
            lock (_lines) _lines.Add(line.ToString());
        }

        public bool IsEnabled(LogLevel logLevel) => true;

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
    }

    /// <summary>
    /// A sending and a receiving session of strings over one <see cref="RelayHub"/>, each holding its own
    /// view of the transfer's two clients and id, each logging apart.
    /// </summary>
    private sealed class Duo : IAsyncDisposable
    {
        private readonly CapturingLogger _senderLog = new();
        private readonly CapturingLogger _receiverLog = new();
        private int _received;

        public RelayHub Hub { get; } = new();
        public TestSession Sender { get; }
        public TestSession Receiver { get; }

        public IEnumerable<string> Logged => _senderLog.Lines.Concat(_receiverLog.Lines);

        public Duo(
            EndToEndEncryption? sender, EndToEndEncryption? receiver,
            TransferParties? senderParties = null, TransferParties? receiverParties = null,
            string? receiverTransferId = null)
        {
            var parties = senderParties ?? new TransferParties(Guid.NewGuid(), Guid.NewGuid());
            var transferId = Guid.NewGuid().ToString();
            Sender = new TestSession(Context(sender, _senderLog), transferId, batchSize: 1, "http://test/data", affinity: null, parties);
            Receiver = new TestSession(Context(receiver, _receiverLog), receiverTransferId ?? transferId, batchSize: 1, "http://test/data",
                affinity: null, receiverParties ?? parties);
        }

        private StubSessionContext Context(EndToEndEncryption? encryption, ILogger logger)
        {
            var context = new StubSessionContext(new HttpClient(Hub), logger) { ProbeAnswers = true };
            context.Options.EndToEndEncryption = encryption;
            context.Options.CompleteStallTimeout = TimeSpan.FromSeconds(5);
            return context;
        }

        public async Task StartAsync()
        {
            await Receiver.StartReceiverAsync();
            await Sender.StartSenderAsync();
        }

        public Task ReceiveAsync() => Task.Run(async () =>
        {
            await foreach (var _ in Receiver.ReceiveAsync(CancellationToken.None)) Interlocked.Increment(ref _received);
        });

        public async Task<Outcome> RunAsync(int items)
        {
            await StartAsync();
            var receiving = Capture(ReceiveAsync());
            var sending = Capture(Task.Run(async () =>
            {
                for (var i = 0; i < items; i++) await Sender.SendAsync($"item-{i}", CancellationToken.None);
                await Sender.CompleteAsync();
            }));

            return new Outcome(await sending.WaitAsync(Bound), await receiving.WaitAsync(Bound), Volatile.Read(ref _received));
        }

        private static async Task<TransferFailedException?> Capture(Task task)
        {
            try
            {
                await task;
                return null;
            }
            catch (TransferFailedException failed)
            {
                return failed;
            }
        }

        public async ValueTask DisposeAsync()
        {
            await Sender.DisposeAsync();
            await Receiver.DisposeAsync();
        }
    }
}
