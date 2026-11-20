using TransportR.Client;
using TransportR.Serialization.MessagePack;
using TransportR.Tests.Infrastructure;

namespace TransportR.Tests;

/// <summary>
/// End-to-end encryption as a host reaches it: the two builder methods, over a hub on the loopback,
/// for a client of items and a client of bytes. What each end does with the keys is
/// <see cref="EndToEndSessionTests"/>'s.
/// </summary>
public class EndToEndEncryptionBuilderTests
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(30);

    private static readonly byte[] KeyA = Enumerable.Range(0, 32).Select(i => (byte)(i * 7 + 1)).ToArray();
    private static readonly byte[] KeyB = Enumerable.Range(0, 32).Select(i => (byte)(i * 11 + 3)).ToArray();

    public enum Kind { Items, Bytes }

    /// <summary>What an end of a transfer is set to: whether it seals, the key it gives if any, and the call that this one replaces.</summary>
    private sealed record Setting(bool Sealed, Func<Guid, byte[]?>? Key, Setting? Earlier = null)
    {
        public static readonly Setting Clear = new(false, null);
        public static readonly Setting Encrypted = new(true, null);
        public static Setting Keyed(byte[]? key) => new(true, _ => key);
    }

    private sealed record Outcome(
        TransferFailedException? Sender,
        TransferFailedException? Receiver,
        long Delivered,
        bool Identical,
        TransferProtection? SenderProtection,
        TransferProtection? ReceiverProtection);

    private static TransportRClientBuilder<string> ItemsBuilder(string url, Setting setting) =>
        Apply(new TransportRClientBuilder<string>().WithUrl(url).WithMessagePackSerialization().WithMaxInFlightBatches(4), setting);

    private static ByteStreamClientBuilder BytesBuilder(string url, Setting setting) =>
        Apply(new ByteStreamClientBuilder(1000).WithUrl(url).WithMaxInFlightBatches(4), setting);

    private static TransportRClientBuilder<string> Apply(TransportRClientBuilder<string> builder, Setting setting)
    {
        if (setting.Earlier is { } earlier) builder = Apply(builder, earlier);
        return setting switch
        {
            { Sealed: false } => builder,
            { Key: null } => builder.WithEndToEndEncryption(),
            _ => builder.WithEndToEndEncryption(setting.Key),
        };
    }

    private static ByteStreamClientBuilder Apply(ByteStreamClientBuilder builder, Setting setting)
    {
        if (setting.Earlier is { } earlier) builder = Apply(builder, earlier);
        return setting switch
        {
            { Sealed: false } => builder,
            { Key: null } => builder.WithEndToEndEncryption(),
            _ => builder.WithEndToEndEncryption(setting.Key),
        };
    }

    private static Task<Outcome> RunAsync(Kind kind, Setting sender, Setting receiver) =>
        kind == Kind.Items ? RunItemsAsync(sender, receiver) : RunBytesAsync(sender, receiver);

    private static TransferProtection? ProtectionOf(IDataClient client) => client.GetTransfers().SingleOrDefault()?.Protection;

    private static async Task<Outcome> RunItemsAsync(Setting senderSetting, Setting receiverSetting)
    {
        await using var hub = await TestHubServerFactory.StartLoopbackAsync();
        await using var sender = ItemsBuilder(hub.BaseUrl, senderSetting).Build();
        await using var receiver = ItemsBuilder(hub.BaseUrl, receiverSetting).Build();

        var receiveHandle = new TaskCompletionSource<Receive<string>>(TaskCreationOptions.RunContinuationsAsynchronously);
        receiver.OnTransferStarted += started => { receiveHandle.TrySetResult(started); return Task.CompletedTask; };
        await sender.ConnectAsync();
        await receiver.ConnectAsync();

        var expected = Enumerable.Range(0, 95).Select(i => $"item-{i}").ToList();
        TransferProtection? senderProtection = null;

        var sending = Task.Run(async () =>
        {
            try
            {
                await using var send = await sender.InitTransferAsync(receiver.ClientId, batchSize: 10);
                senderProtection = ProtectionOf(sender);
                await send.SendAsync(expected);
                await send.CompleteAsync();
                return null;
            }
            catch (TransferFailedException failure)
            {
                return failure;
            }
        });

        var delivered = new List<string>();
        TransferProtection? receiverProtection = null;
        var receiving = Task.Run(async () =>
        {
            try
            {
                await using var receive = await receiveHandle.Task.WaitAsync(Bound);
                receiverProtection = ProtectionOf(receiver);
                await foreach (var item in receive.ReceiveAsync()) delivered.Add(item);
                await receive.WaitForCompletionAsync();
                return null;
            }
            catch (TransferFailedException failure)
            {
                return failure;
            }
        });

        var senderFailure = await sending.WaitAsync(Bound);
        var receiverFailure = await receiving.WaitAsync(Bound);
        return new Outcome(senderFailure, receiverFailure, delivered.Count, delivered.SequenceEqual(expected),
            senderProtection, receiverProtection);
    }

    private static async Task<Outcome> RunBytesAsync(Setting senderSetting, Setting receiverSetting)
    {
        await using var hub = await TestHubServerFactory.StartLoopbackAsync();
        await using var sender = BytesBuilder(hub.BaseUrl, senderSetting).Build();
        await using var receiver = BytesBuilder(hub.BaseUrl, receiverSetting).Build();

        var receiveHandle = new TaskCompletionSource<ByteReceive>(TaskCreationOptions.RunContinuationsAsynchronously);
        receiver.OnTransferStarted += started => { receiveHandle.TrySetResult(started); return Task.CompletedTask; };
        await sender.ConnectAsync();
        await receiver.ConnectAsync();

        var expected = new byte[7321];
        new Random(20261120).NextBytes(expected);
        TransferProtection? senderProtection = null;

        var sending = Task.Run(async () =>
        {
            try
            {
                await using var send = await sender.InitTransferAsync(receiver.ClientId);
                senderProtection = ProtectionOf(sender);
                await send.CopyFromAsync(new MemoryStream(expected));
                await send.CompleteAsync();
                return null;
            }
            catch (TransferFailedException failure)
            {
                return failure;
            }
        });

        var delivered = new MemoryStream();
        TransferProtection? receiverProtection = null;
        var receiving = Task.Run(async () =>
        {
            try
            {
                await using var receive = await receiveHandle.Task.WaitAsync(Bound);
                receiverProtection = ProtectionOf(receiver);
                await receive.CopyToAsync(delivered);
                await receive.WaitForCompletionAsync();
                return null;
            }
            catch (TransferFailedException failure)
            {
                return failure;
            }
        });

        var senderFailure = await sending.WaitAsync(Bound);
        var receiverFailure = await receiving.WaitAsync(Bound);
        return new Outcome(senderFailure, receiverFailure, delivered.Length, delivered.ToArray().AsSpan().SequenceEqual(expected),
            senderProtection, receiverProtection);
    }

    private static long Expected(Kind kind) => kind == Kind.Items ? 95 : 7321;

    private static void AssertDelivered(Kind kind, Outcome outcome)
    {
        Assert.Null(outcome.Sender);
        Assert.Null(outcome.Receiver);
        Assert.Equal(Expected(kind), outcome.Delivered);
        Assert.True(outcome.Identical, "What arrived is not what was sent.");
    }

    private static void AssertFailedBeforeTheFirstBatch(Outcome outcome)
    {
        Assert.Equal(0, outcome.Delivered);
        Assert.False(outcome.Identical);
    }

    [Theory]
    [InlineData(Kind.Items)]
    [InlineData(Kind.Bytes)]
    public async Task WithoutAKey_ATransferIsDelivered_AndBothEndsReportEncrypted(Kind kind)
    {
        var outcome = await RunAsync(kind, Setting.Encrypted, Setting.Encrypted);

        AssertDelivered(kind, outcome);
        Assert.Equal(TransferProtection.Encrypted, outcome.SenderProtection);
        Assert.Equal(TransferProtection.Encrypted, outcome.ReceiverProtection);
    }

    [Theory]
    [InlineData(Kind.Items)]
    [InlineData(Kind.Bytes)]
    public async Task WithAKey_ATransferIsDelivered_AndBothEndsReportTheKey(Kind kind)
    {
        var outcome = await RunAsync(kind, Setting.Keyed(KeyA), Setting.Keyed(KeyA));

        AssertDelivered(kind, outcome);
        Assert.Equal(TransferProtection.EncryptedWithPreSharedKey, outcome.SenderProtection);
        Assert.Equal(TransferProtection.EncryptedWithPreSharedKey, outcome.ReceiverProtection);
    }

    [Theory]
    [InlineData(Kind.Items)]
    [InlineData(Kind.Bytes)]
    public async Task ATransferInClear_ReportsNone_AndIsDeliveredAsBefore(Kind kind)
    {
        var outcome = await RunAsync(kind, Setting.Clear, Setting.Clear);

        AssertDelivered(kind, outcome);
        Assert.Equal(TransferProtection.None, outcome.SenderProtection);
        Assert.Equal(TransferProtection.None, outcome.ReceiverProtection);
    }

    [Theory]
    [InlineData(Kind.Items)]
    [InlineData(Kind.Bytes)]
    public async Task KeysThatDiffer_FailBeforeTheFirstBatch_TheSenderOnTheKey_TheReceiverAbandoned(Kind kind)
    {
        var outcome = await RunAsync(kind, Setting.Keyed(KeyA), Setting.Keyed(KeyB));

        Assert.Equal(TransferFailureCategory.EndToEndEncryptionFailed, outcome.Sender?.Category);
        Assert.Equal(TransferFailureCategory.AbandonedByPeer, outcome.Receiver?.Category);
        AssertFailedBeforeTheFirstBatch(outcome);
    }

    [Theory]
    [InlineData(Kind.Items)]
    [InlineData(Kind.Bytes)]
    public async Task AnEncryptedSender_AndAReceiverInClear_Fail_TheSenderOnTheMismatch(Kind kind)
    {
        var outcome = await RunAsync(kind, Setting.Encrypted, Setting.Clear);

        Assert.Equal(TransferFailureCategory.EndToEndEncryptionFailed, outcome.Sender?.Category);
        Assert.Equal(TransferFailureCategory.AbandonedByPeer, outcome.Receiver?.Category);
        AssertFailedBeforeTheFirstBatch(outcome);
    }

    [Theory]
    [InlineData(Kind.Items)]
    [InlineData(Kind.Bytes)]
    public async Task ASenderInClear_AndAnEncryptedReceiver_Fail_TheReceiverOnTheMismatch(Kind kind)
    {
        var outcome = await RunAsync(kind, Setting.Clear, Setting.Encrypted);

        Assert.Equal(TransferFailureCategory.EndToEndEncryptionFailed, outcome.Receiver?.Category);
        Assert.Equal(TransferFailureCategory.AbandonedByPeer, outcome.Sender?.Category);
        AssertFailedBeforeTheFirstBatch(outcome);
    }

    [Theory]
    [InlineData(Kind.Items, true)]
    [InlineData(Kind.Items, false)]
    [InlineData(Kind.Bytes, true)]
    [InlineData(Kind.Bytes, false)]
    public async Task AProviderThatGivesNoKey_FailsTheTransfer_AtItsEnd_AndNeverFallsBackToNoKey(Kind kind, bool atTheSender)
    {
        var refusing = Setting.Keyed(null);
        var keeping = Setting.Keyed(KeyA);
        var outcome = await RunAsync(kind, atTheSender ? refusing : keeping, atTheSender ? keeping : refusing);

        var (refused, peer) = atTheSender ? (outcome.Sender, outcome.Receiver) : (outcome.Receiver, outcome.Sender);
        Assert.Equal(TransferFailureCategory.EndToEndEncryptionFailed, refused?.Category);
        Assert.Equal(TransferFailureCategory.AbandonedByPeer, peer?.Category);
        AssertFailedBeforeTheFirstBatch(outcome);
    }

    [Fact]
    public void ANullProvider_IsRefusedAtTheCall_OnBothBuilders()
    {
        Assert.Throws<ArgumentNullException>("preSharedKey", () => new TransportRClientBuilder<string>().WithEndToEndEncryption(null!));
        Assert.Throws<ArgumentNullException>("preSharedKey", () => new ByteStreamClientBuilder(1000).WithEndToEndEncryption(null!));
    }

    [Fact]
    public void BothMethods_ReturnTheirBuilder()
    {
        var items = new TransportRClientBuilder<string>();
        var bytes = new ByteStreamClientBuilder(1000);

        Assert.Same(items, items.WithEndToEndEncryption());
        Assert.Same(items, items.WithEndToEndEncryption(_ => KeyA));
        Assert.Same(bytes, bytes.WithEndToEndEncryption());
        Assert.Same(bytes, bytes.WithEndToEndEncryption(_ => KeyA));
    }

    [Theory]
    [InlineData(Kind.Items)]
    [InlineData(Kind.Bytes)]
    public async Task ALaterCall_ReplacesTheEarlierOne_ASetting_WithoutAKey_AfterOneWithIt(Kind kind)
    {
        // Were the first call kept, the sender would hold KeyA while the receiver holds none: a mismatch.
        var outcome = await RunAsync(kind, Replaced(Setting.Keyed(KeyA), Setting.Encrypted), Setting.Encrypted);

        AssertDelivered(kind, outcome);
        Assert.Equal(TransferProtection.Encrypted, outcome.SenderProtection);
        Assert.Equal(TransferProtection.Encrypted, outcome.ReceiverProtection);
    }

    [Theory]
    [InlineData(Kind.Items)]
    [InlineData(Kind.Bytes)]
    public async Task ALaterCall_ReplacesTheEarlierOne_ASetting_WithAKey_AfterOneWithout(Kind kind)
    {
        var outcome = await RunAsync(kind, Replaced(Setting.Encrypted, Setting.Keyed(KeyA)), Setting.Keyed(KeyA));

        AssertDelivered(kind, outcome);
        Assert.Equal(TransferProtection.EncryptedWithPreSharedKey, outcome.SenderProtection);
        Assert.Equal(TransferProtection.EncryptedWithPreSharedKey, outcome.ReceiverProtection);
    }

    /// <summary>A setting that is <paramref name="first"/> then, by a second call, <paramref name="second"/>.</summary>
    private static Setting Replaced(Setting first, Setting second) => second with { Earlier = first };
}
