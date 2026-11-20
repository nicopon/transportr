using System.Security.Cryptography;
using TransportR.Client;
using TransportR.Client.Batching;
using TransportR.Client.Configuration;
using TransportR.Client.Internal;
using TransportR.Client.Protocol;
using TransportR.Client.Sessions;
using TransportR.Tests.TestHelpers;

namespace TransportR.Tests;

/// <summary>
/// End-to-end encryption as two sessions run it through a hub that relays, records and, where a test
/// says so, rewrites what they write.
/// </summary>
public class EndToEndSessionTests
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(15);

    private static readonly byte[] KeyA = Enumerable.Range(1, 32).Select(i => (byte)i).ToArray();
    private static readonly byte[] KeyB = Enumerable.Range(2, 32).Select(i => (byte)i).ToArray();

    [Fact]
    public void TheAbortReasonsOfEncryption_HaveTheirWireValues_AndATextOfTheirOwn()
    {
        Assert.Equal(5, (int)AbortReason.EncryptionModeMismatch);
        Assert.Equal(6, (int)AbortReason.VerificationFailed);
        Assert.NotEqual("The peer aborted the transfer (reason 5).", AbortReason.EncryptionModeMismatch.Describe());
        Assert.NotEqual("The peer aborted the transfer (reason 6).", AbortReason.VerificationFailed.Describe());
    }

    [Fact]
    public void EndToEndEncryptionFailed_ComesAfterEveryCategoryThatWasThereBefore()
    {
        Assert.Equal(TransferFailureCategory.EndToEndEncryptionFailed, Enum.GetValues<TransferFailureCategory>().Max());
        Assert.Equal((int)TransferFailureCategory.Other + 1, (int)TransferFailureCategory.EndToEndEncryptionFailed);
    }

    // ---- The handshake ----

    [Fact]
    public async Task AnEncryptedSender_AndAReceiverInClear_FailBeforeAnyBatch_TheSenderOnTheMismatch()
    {
        await using var pair = new SessionPair(sender: Encrypted(), receiver: null);
        var outcome = await pair.RunAsync(items: 2);

        Assert.Equal(TransferFailureCategory.EndToEndEncryptionFailed, outcome.Sender?.Category);
        Assert.Equal(TransferFailureCategory.AbandonedByPeer, outcome.Receiver?.Category);
        Assert.Equal(AbortReason.EncryptionModeMismatch.Describe(), outcome.Receiver?.Message);
        Assert.DoesNotContain(pair.Hub.SenderWrote, IsBatch);
        Assert.Equal(0, outcome.Received);
    }

    [Fact]
    public async Task ASenderInClear_AndAnEncryptedReceiver_FailBeforeAnyBatchIsTaken_TheReceiverOnTheMismatch()
    {
        await using var pair = new SessionPair(sender: null, receiver: Encrypted());
        var outcome = await pair.RunAsync(items: 2);

        Assert.Equal(TransferFailureCategory.EndToEndEncryptionFailed, outcome.Receiver?.Category);
        Assert.Equal(TransferFailureCategory.AbandonedByPeer, outcome.Sender?.Category);
        Assert.Equal(AbortReason.EncryptionModeMismatch.Describe(), outcome.Sender?.Message);
        Assert.DoesNotContain(pair.Hub.ReceiverWrote, chunk => chunk.Header.Type is ChunkType.BatchAck or ChunkType.Pong);
        Assert.Equal(0, outcome.Received);
    }

    public static TheoryData<byte[]?, byte[]?> KeysThatDoNotMatch => new()
    {
        { KeyA, KeyB },
        { KeyA, null },
        { null, KeyB },
    };

    [Theory]
    [MemberData(nameof(KeysThatDoNotMatch))]
    public async Task PreSharedKeysThatDoNotMatch_FailAtTheConfirmation_BeforeAnyBatch(byte[]? senderKey, byte[]? receiverKey)
    {
        await using var pair = new SessionPair(sender: Encrypted(senderKey), receiver: Encrypted(receiverKey));
        var outcome = await pair.RunAsync(items: 2);

        Assert.Equal(TransferFailureCategory.EndToEndEncryptionFailed, outcome.Sender?.Category);
        Assert.Equal(TransferFailureCategory.AbandonedByPeer, outcome.Receiver?.Category);
        Assert.Equal(AbortReason.VerificationFailed.Describe(), outcome.Receiver?.Message);
        Assert.DoesNotContain(pair.Hub.SenderWrote, IsBatch);
        Assert.Equal(0, outcome.Received);
    }

    public static TheoryData<bool, int> KeysAProviderMayGive => new()
    {
        { true, -1 },
        { true, 31 },
        { true, 33 },
        { false, -1 },
        { false, 31 },
    };

    /// <param name="length">The key the provider returns, -1 for <c>null</c>.</param>
    [Theory]
    [MemberData(nameof(KeysAProviderMayGive))]
    public async Task AProviderThatGivesNo32ByteKey_RefusesTheTransfer_AtItsEnd(bool atTheSender, int length)
    {
        var asked = new List<Guid>();
        var refusing = new EndToEndEncryption(peer =>
        {
            lock (asked) asked.Add(peer);
            return length < 0 ? null : new byte[length];
        });
        var keeping = new EndToEndEncryption(_ => new byte[32]);
        await using var pair = new SessionPair(sender: atTheSender ? refusing : keeping, receiver: atTheSender ? keeping : refusing);
        var outcome = await pair.RunAsync(items: 2);

        var (refused, peer) = atTheSender ? (outcome.Sender, outcome.Receiver) : (outcome.Receiver, outcome.Sender);
        Assert.Equal(TransferFailureCategory.EndToEndEncryptionFailed, refused?.Category);
        Assert.Equal(TransferFailureCategory.AbandonedByPeer, peer?.Category);
        Assert.Equal([atTheSender ? pair.Parties.ReceiverId : pair.Parties.SenderId], asked);
        Assert.DoesNotContain(pair.Hub.SenderWrote, IsBatch);
        Assert.Equal(0, outcome.Received);
    }

    [Fact]
    public async Task AProviderThatThrows_RefusesTheTransfer_AsOneThatGivesNoKey()
    {
        await using var pair = new SessionPair(
            sender: new EndToEndEncryption(_ => throw new InvalidOperationException("no vault")),
            receiver: new EndToEndEncryption(_ => new byte[32]));
        var outcome = await pair.RunAsync(items: 1);

        Assert.Equal(TransferFailureCategory.EndToEndEncryptionFailed, outcome.Sender?.Category);
        Assert.Equal(TransferFailureCategory.AbandonedByPeer, outcome.Receiver?.Category);
    }

    [Fact]
    public async Task AKeepalivePingWhoseShareChanged_IsRefused_ByTheReceiver()
    {
        await using var pair = new SessionPair(sender: Encrypted(), receiver: Encrypted(), configureSender: options =>
            options.PresenceHeartbeatInterval = TimeSpan.FromMilliseconds(200));
        var pings = 0;
        pair.Hub.Rewrite = chunk =>
            chunk.Header.Type == ChunkType.Ping && Interlocked.Increment(ref pings) > 1 ? [Flipped(chunk.Bytes, ChunkHeader.Size + 10)] : [chunk.Bytes];

        await pair.StartAsync();
        var receiving = pair.ReceiveAsync();
        var refused = await Assert.ThrowsAsync<TransferFailedException>(() => receiving.WaitAsync(Bound));
        var abandoned = await Assert.ThrowsAsync<TransferFailedException>(() => pair.Sender.CompleteAsync().WaitAsync(Bound));

        Assert.Equal(TransferFailureCategory.EndToEndEncryptionFailed, refused.Category);
        Assert.Equal(TransferFailureCategory.AbandonedByPeer, abandoned.Category);
        Assert.Equal(AbortReason.VerificationFailed.Describe(), abandoned.Message);
    }

    [Theory]
    [InlineData((byte)ChunkType.Ping)]
    [InlineData((byte)ChunkType.Pong)]
    public async Task AShareTheHubSubstitutes_FailsTheTransfer_AtTheSender_BeforeAnyBatch(byte carryingType)
    {
        var carrying = (ChunkType)carryingType;
        await using var pair = new SessionPair(sender: Encrypted(), receiver: Encrypted());
        pair.Hub.Rewrite = chunk => chunk.Header.Type == carrying ? [WithShare(chunk.Bytes, NewShare())] : [chunk.Bytes];
        var outcome = await pair.RunAsync(items: 2);

        Assert.Equal(TransferFailureCategory.EndToEndEncryptionFailed, outcome.Sender?.Category);
        Assert.Equal(TransferFailureCategory.AbandonedByPeer, outcome.Receiver?.Category);
        Assert.DoesNotContain(pair.Hub.SenderWrote, IsBatch);
        Assert.Equal(0, outcome.Received);
    }

    [Theory]
    [InlineData((byte)ChunkType.Ping, 64)]
    [InlineData((byte)ChunkType.Ping, 66)]
    [InlineData((byte)ChunkType.Pong, 96)]
    [InlineData((byte)ChunkType.Pong, 98)]
    public async Task AHandshakeChunkOfAnotherLength_IsAProtocolViolation_AtTheEndThatReadsIt(byte wireType, int length)
    {
        var type = (ChunkType)wireType;
        await using var pair = new SessionPair(sender: Encrypted(), receiver: Encrypted());
        pair.Hub.Rewrite = chunk => chunk.Header.Type == type ? [Resized(chunk.Bytes, length)] : [chunk.Bytes];
        var outcome = await pair.RunAsync(items: 1, waitForBoth: false, reader: type == ChunkType.Ping ? End.Receiver : End.Sender);

        var reader = type == ChunkType.Ping ? outcome.Receiver : outcome.Sender;
        Assert.Equal(TransferFailureCategory.ProtocolViolation, reader?.Category);
        Assert.Equal(0, outcome.Received);
    }

    // ---- Sealed batches ----

    [Fact]
    public async Task ASealedTransferOfItems_GoesThroughAReconnection_TheResendReplayingTheSameBytes()
    {
        await using var pair = new SessionPair(sender: Encrypted(KeyA), receiver: Encrypted(KeyA));
        var dropped = DropFirstStreamBatchesFrom(pair.Hub, batchId: 1, count: 2);

        await pair.StartAsync();
        var receiving = pair.ReceiveAsync();
        for (var i = 0; i < 3; i++) await pair.Sender.SendAsync($"item-{i}", CancellationToken.None);
        await dropped.WaitAsync(Bound);

        pair.Sender.TriggerReconnect();
        await WaitUntilAsync(() => pair.Received == 3);
        await pair.Sender.CompleteAsync().WaitAsync(Bound);
        await receiving.WaitAsync(Bound);

        var batches = pair.Hub.SenderWrote.Where(IsBatch).ToList();
        Assert.All(batches, batch => Assert.Equal(ChunkType.SealedBatch, batch.Header.Type));
        foreach (var batchId in new[] { 1, 2 })
        {
            Assert.Equal(
                batches.Single(batch => batch.Epoch == 0 && batch.Header.BatchId == batchId).Bytes,
                batches.Single(batch => batch.Epoch == 1 && batch.Header.BatchId == batchId).Bytes);
        }

        var pings = pair.Hub.SenderWrote.Where(chunk => chunk.Header.Type == ChunkType.Ping).ToList();
        Assert.Equal([0, 1], pings.Select(ping => ping.Epoch));
        Assert.Equal(pings[0].Bytes, pings[1].Bytes);
    }

    [Fact]
    public async Task ASealedTransferOfBytes_GoesThroughAReconnection_AndCarriesNoPlaintext_UnderAPlaintextBound()
    {
        // WithMaxBatchBytes bounds the plaintext at both ends: the receiver admits the tag on top of it.
        await using var pair = new BytePair(Encrypted(), Encrypted(), batchBytes: 16, options => options.MaxBatchBytes = 16);
        var dropped = DropFirstStreamBatchesFrom(pair.Hub, batchId: 1, count: 2);
        var sent = Enumerable.Range(0, 48).Select(i => (byte)(0xA0 + i)).ToArray();

        await pair.StartAsync();
        var receiving = pair.ReceiveAsync();
        await pair.Send.WriteAsync(sent);
        await dropped.WaitAsync(Bound);

        pair.Sender.TriggerReconnect();
        await WaitUntilAsync(() => pair.ReceivedCount == sent.Length);
        await pair.Send.CompleteAsync().WaitAsync(Bound);
        Assert.Equal(sent, await receiving.WaitAsync(Bound));

        var batches = pair.Hub.SenderWrote.Where(IsBatch).ToList();
        Assert.All(batches, batch =>
        {
            Assert.Equal(ChunkType.SealedStreamBatch, batch.Header.Type);
            Assert.Equal(16, batch.Header.Count);
            Assert.Equal(16 + EndToEndCipher.TagSize, batch.Header.PayloadLength);
            Assert.Equal(-1, batch.Payload.IndexOf(sent.AsSpan(16 * batch.Header.BatchId, 4)));
        });
        Assert.Equal(
            batches.Single(batch => batch.Epoch == 0 && batch.Header.BatchId == 1).Bytes,
            batches.Single(batch => batch.Epoch == 1 && batch.Header.BatchId == 1).Bytes);
    }

    [Theory]
    [InlineData(6)]  // BatchId
    [InlineData(10)] // Count
    [InlineData(20)] // TransferId
    [InlineData(30)] // the ciphertext
    [InlineData(-1)] // the tag
    public async Task ASealedBatchAlteredOnItsWay_FailsTheTransfer_AndIsNeverAcknowledged(int at)
    {
        await using var pair = new SessionPair(sender: Encrypted(), receiver: Encrypted());
        pair.Hub.Rewrite = chunk => chunk.Header.Type == ChunkType.SealedBatch && chunk.Header.BatchId == 0
            ? [Flipped(chunk.Bytes, at < 0 ? chunk.Bytes.Length + at : at)]
            : [chunk.Bytes];
        var outcome = await pair.RunAsync(items: 2);

        Assert.Equal(TransferFailureCategory.EndToEndEncryptionFailed, outcome.Receiver?.Category);
        Assert.Equal(TransferFailureCategory.AbandonedByPeer, outcome.Sender?.Category);
        Assert.Equal(AbortReason.VerificationFailed.Describe(), outcome.Sender?.Message);
        Assert.DoesNotContain(pair.Hub.ReceiverWrote, chunk => chunk.Header.Type == ChunkType.BatchAck);
        Assert.Equal(0, outcome.Received);
    }

    [Fact]
    public async Task ABatchInClear_IsRefusedByAnEncryptedReceiver_AsAPeerInClear()
    {
        await using var pair = new SessionPair(sender: Encrypted(), receiver: Encrypted());
        pair.Hub.Rewrite = chunk => chunk.Header.Type == ChunkType.SealedBatch ? [Retyped(chunk.Bytes, ChunkType.Batch)] : [chunk.Bytes];
        var outcome = await pair.RunAsync(items: 1);

        Assert.Equal(TransferFailureCategory.EndToEndEncryptionFailed, outcome.Receiver?.Category);
        Assert.Equal(TransferFailureCategory.AbandonedByPeer, outcome.Sender?.Category);
        Assert.Equal(AbortReason.EncryptionModeMismatch.Describe(), outcome.Sender?.Message);
        Assert.DoesNotContain(pair.Hub.ReceiverWrote, chunk => chunk.Header.Type == ChunkType.BatchAck);
    }

    [Fact]
    public async Task ASealedBatch_IsRefusedByAReceiverInClear_ByItsType()
    {
        await using var pair = new SessionPair(sender: null, receiver: null);
        pair.Hub.Rewrite = chunk => chunk.Header.Type == ChunkType.Batch ? [Retyped(chunk.Bytes, ChunkType.SealedBatch)] : [chunk.Bytes];
        var outcome = await pair.RunAsync(items: 1);

        Assert.Equal(TransferFailureCategory.UndecodableOrUnserializableBatch, outcome.Receiver?.Category);
        Assert.Equal(TransferFailureCategory.AbandonedByPeer, outcome.Sender?.Category);
        Assert.DoesNotContain(pair.Hub.ReceiverWrote, chunk => chunk.Header.Type == ChunkType.BatchAck);
    }

    [Fact]
    public async Task AShareThatChangesOnAReopenedStream_IsRefused_AndNothingIsResentBeforeTheAbort()
    {
        await using var pair = new SessionPair(sender: Encrypted(), receiver: Encrypted());
        var dropped = DropFirstStreamBatchesFrom(pair.Hub, batchId: 1, count: 1);
        var pongs = 0;
        var relay = pair.Hub.Rewrite!;
        pair.Hub.Rewrite = chunk => chunk.Header.Type == ChunkType.Pong && Interlocked.Increment(ref pongs) > 1
            ? [WithShare(chunk.Bytes, NewShare())]
            : relay(chunk);

        await pair.StartAsync();
        var receiving = pair.ReceiveAsync();
        for (var i = 0; i < 2; i++) await pair.Sender.SendAsync($"item-{i}", CancellationToken.None);
        await dropped.WaitAsync(Bound);
        pair.Sender.TriggerReconnect();

        var abandoned = await Assert.ThrowsAsync<TransferFailedException>(() => receiving.WaitAsync(Bound));
        await WaitUntilAsync(() => pair.Sender.State == Client.Models.SessionState.Failed);
        var refused = await Assert.ThrowsAsync<TransferFailedException>(() => pair.Sender.CompleteAsync().WaitAsync(Bound));

        Assert.Equal(TransferFailureCategory.EndToEndEncryptionFailed, refused.Category);
        Assert.Equal(TransferFailureCategory.AbandonedByPeer, abandoned.Category);
        Assert.Equal(
            [ChunkType.Ping, ChunkType.Abort],
            pair.Hub.SenderWrote.Where(chunk => chunk.Epoch == 1).Select(chunk => chunk.Header.Type));
    }

    // ---- Harness ----

    private static EndToEndEncryption Encrypted(byte[]? preSharedKey = null) =>
        new(preSharedKey is null ? null : _ => preSharedKey);

    private static bool IsBatch(WireChunk chunk) =>
        chunk.Header.Type is ChunkType.Batch or ChunkType.StreamBatch or ChunkType.SealedBatch or ChunkType.SealedStreamBatch;

    /// <summary>
    /// Drops the batches of the sender's first stream from <paramref name="batchId"/> on, as a cut would;
    /// completes once <paramref name="count"/> of them have been dropped.
    /// </summary>
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
            await Task.Delay(20);
        }
    }

    private static byte[] Retyped(byte[] chunk, ChunkType type)
    {
        var copy = (byte[])chunk.Clone();
        copy[1] = (byte)type;
        return copy;
    }

    private static byte[] Flipped(byte[] chunk, int at)
    {
        var copy = (byte[])chunk.Clone();
        copy[at] ^= 0x01;
        return copy;
    }

    /// <summary>The chunk with its payload cut or padded to <paramref name="length"/>, and its header saying so.</summary>
    private static byte[] Resized(byte[] chunk, int length)
    {
        var header = ChunkHeader.ReadFrom(chunk) with { PayloadLength = length };
        var copy = new byte[ChunkHeader.Size + length];
        header.WriteTo(copy);
        chunk.AsSpan(ChunkHeader.Size, Math.Min(length, chunk.Length - ChunkHeader.Size)).CopyTo(copy.AsSpan(ChunkHeader.Size));
        return copy;
    }

    /// <summary>The chunk with the share at the head of its payload replaced.</summary>
    private static byte[] WithShare(byte[] chunk, byte[] share)
    {
        var copy = (byte[])chunk.Clone();
        share.CopyTo(copy, ChunkHeader.Size);
        return copy;
    }

    /// <summary>A share of a key pair of the hub's own: a point of the curve, uncompressed.</summary>
    private static byte[] NewShare()
    {
        using var key = ECDiffieHellman.Create(ECCurve.NamedCurves.nistP256);
        var point = key.ExportParameters(includePrivateParameters: false).Q;
        return [0x04, .. point.X!, .. point.Y!];
    }

    /// <summary>
    /// A sending and a receiving session of bytes over a <see cref="RelayHub"/>, through the handles a
    /// <see cref="ByteStreamClient"/> gives; a probe after a cut finds the hub answering.
    /// </summary>
    internal sealed class BytePair : IAsyncDisposable
    {
        private readonly List<byte> _received = [];

        public RelayHub Hub { get; } = new();
        public TransferParties Parties { get; } = new(Guid.NewGuid(), Guid.NewGuid());
        public string TransferId { get; } = Guid.NewGuid().ToString();
        public DataSession Sender { get; }
        public DataSession Receiver { get; }
        public HttpByteSend Send { get; }
        public HttpByteReceive Receive { get; }

        public int ReceivedCount
        {
            get { lock (_received) return _received.Count; }
        }

        public BytePair(EndToEndEncryption? sender, EndToEndEncryption? receiver, int batchBytes, Action<ClientOptions>? configure = null)
        {
            var senderContext = Context(sender, configure);
            var source = new ByteBatchSource(batchBytes, senderContext.Options.BatchFlushIntervalMs, senderContext.StreamManager);
            Sender = new DataSession(senderContext, TransferId, 1000, "http://test/data", null, Parties, source, new ByteReceivePort(null).OpenSink);
            Send = new HttpByteSend(Sender, source.SendBytesAsync);

            var receiverContext = Context(receiver, configure);
            var port = new ByteReceivePort(null);
            Receiver = new DataSession(receiverContext, TransferId, 1000, "http://test/data", null, Parties,
                new ByteBatchSource(batchBytes, receiverContext.Options.BatchFlushIntervalMs, receiverContext.StreamManager), port.OpenSink);
            Receive = new HttpByteReceive(Receiver, port.ReadAllAsync);
        }

        private StubSessionContext Context(EndToEndEncryption? encryption, Action<ClientOptions>? configure)
        {
            var context = new StubSessionContext(new HttpClient(Hub)) { ProbeAnswers = true };
            context.Options.EndToEndEncryption = encryption;
            context.Options.CompleteStallTimeout = TimeSpan.FromSeconds(5);
            configure?.Invoke(context.Options);
            return context;
        }

        public async Task StartAsync()
        {
            await Receiver.StartReceiverAsync();
            await Sender.StartSenderAsync();
        }

        /// <summary>Reads the bytes until the transfer ends, and returns them.</summary>
        public Task<byte[]> ReceiveAsync() => Task.Run(async () =>
        {
            var buffer = new byte[64];
            int read;
            while ((read = await Receive.ReadAsync(buffer)) > 0)
            {
                lock (_received) _received.AddRange(buffer.AsSpan(0, read));
            }

            lock (_received) return _received.ToArray();
        });

        public async ValueTask DisposeAsync()
        {
            await Send.DisposeAsync();
            await Receive.DisposeAsync();
        }
    }

    internal enum End { Sender, Receiver }

    /// <param name="Received">The items the receiver's consumer took.</param>
    internal sealed record Outcome(TransferFailedException? Sender, TransferFailedException? Receiver, int Received);

    /// <summary>
    /// A sending and a receiving session of strings, one item per batch, over a <see cref="RelayHub"/>,
    /// each with its own options; a probe after a cut finds the hub answering.
    /// </summary>
    internal sealed class SessionPair : IAsyncDisposable
    {
        private int _received;

        public RelayHub Hub { get; } = new();
        public TransferParties Parties { get; } = new(Guid.NewGuid(), Guid.NewGuid());
        public string TransferId { get; } = Guid.NewGuid().ToString();
        public TestSession Sender { get; }
        public TestSession Receiver { get; }
        public int Received => Volatile.Read(ref _received);

        public SessionPair(
            EndToEndEncryption? sender, EndToEndEncryption? receiver,
            Action<ClientOptions>? configureSender = null, Action<ClientOptions>? configureReceiver = null)
        {
            Sender = new TestSession(Context(sender, configureSender), TransferId, batchSize: 1, "http://test/data", affinity: null, Parties);
            Receiver = new TestSession(Context(receiver, configureReceiver), TransferId, batchSize: 1, "http://test/data", affinity: null, Parties);
        }

        private StubSessionContext Context(EndToEndEncryption? encryption, Action<ClientOptions>? configure)
        {
            var context = new StubSessionContext(new HttpClient(Hub)) { ProbeAnswers = true };
            context.Options.EndToEndEncryption = encryption;
            context.Options.CompleteStallTimeout = TimeSpan.FromSeconds(5);
            configure?.Invoke(context.Options);
            return context;
        }

        public async Task StartAsync()
        {
            await Receiver.StartReceiverAsync();
            await Sender.StartSenderAsync();
        }

        /// <summary>Takes every item the receiver hands over, then its outcome.</summary>
        public Task ReceiveAsync() => Task.Run(async () =>
        {
            await foreach (var _ in Receiver.ReceiveAsync(CancellationToken.None)) Interlocked.Increment(ref _received);
        });

        /// <summary>
        /// Starts both ends, sends <paramref name="items"/> and completes, and returns how each end
        /// ended: <c>null</c> for an end that succeeded. With <paramref name="waitForBoth"/> false, only
        /// <paramref name="reader"/>'s outcome is waited for.
        /// </summary>
        public async Task<Outcome> RunAsync(int items, bool waitForBoth = true, End reader = End.Receiver)
        {
            await StartAsync();
            var receiving = Capture(ReceiveAsync());
            var sending = Capture(Task.Run(async () =>
            {
                for (var i = 0; i < items; i++) await Sender.SendAsync($"item-{i}", CancellationToken.None);
                await Sender.CompleteAsync();
            }));

            if (waitForBoth)
            {
                return new Outcome(await sending.WaitAsync(Bound), await receiving.WaitAsync(Bound), Received);
            }

            return reader == End.Sender
                ? new Outcome(await sending.WaitAsync(Bound), null, Received)
                : new Outcome(null, await receiving.WaitAsync(Bound), Received);
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
