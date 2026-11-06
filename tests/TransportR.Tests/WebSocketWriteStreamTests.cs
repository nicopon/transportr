using System.Net.WebSockets;
using System.Security.Cryptography;
using TransportR.Client.Internal;
using TransportR.Client.Sessions;
using TransportR.Protocol;
using TransportR.Tests.Infrastructure;

namespace TransportR.Tests;

public class WebSocketWriteStreamTests
{
    private const int BufferSize = 1024;
    private static readonly TimeSpan TestBound = TimeSpan.FromSeconds(10);
    private static readonly WebSocketCloseStatus EndOfStream = (WebSocketCloseStatus)DataPlane.EndOfStreamCloseStatus;
    private static readonly WebSocketCloseStatus StreamOver = (WebSocketCloseStatus)DataPlane.StreamOverCloseStatus;

    private static async Task<(WebSocketMessageType Type, byte[] Bytes)> ReceiveMessageAsync(WebSocket socket)
    {
        var buffer = new byte[BufferSize * 4];
        var count = 0;
        while (true)
        {
            var received = await socket.ReceiveAsync(buffer.AsMemory(count), CancellationToken.None).AsTask().WaitAsync(TestBound);
            count += received.Count;
            if (received.EndOfMessage)
            {
                return (received.MessageType, buffer.AsSpan(0, count).ToArray());
            }
        }
    }

    [Fact]
    public async Task NothingLeaves_BeforeAFlush_AndAFlushIsOneMessage()
    {
        await using var pair = await WebSocketPair.OpenAsync();
        await using var stream = new WebSocketWriteStream(pair.Client, BufferSize);

        await stream.WriteAsync(new byte[] { 1, 2, 3 });
        await stream.WriteAsync(new byte[] { 4 });
        var message = ReceiveMessageAsync(pair.Server);
        await Task.Delay(200);
        Assert.False(message.IsCompleted);

        await stream.FlushAsync();

        var (type, bytes) = await message;
        Assert.Equal(WebSocketMessageType.Binary, type);
        Assert.Equal(new byte[] { 1, 2, 3, 4 }, bytes);
    }

    [Fact]
    public async Task AFullBuffer_GoesOutWithoutAFlush()
    {
        await using var pair = await WebSocketPair.OpenAsync();
        await using var stream = new WebSocketWriteStream(pair.Client, BufferSize);

        await stream.WriteAsync(new byte[BufferSize + 1]);

        var (_, bytes) = await ReceiveMessageAsync(pair.Server);
        Assert.Equal(BufferSize, bytes.Length);
    }

    [Fact]
    public async Task Complete_SendsWhatIsLeft_AndReturnsOnlyOnceTheHubHasAnsweredTheEnd()
    {
        await using var pair = await WebSocketPair.OpenAsync();
        await using var stream = new WebSocketWriteStream(pair.Client, BufferSize);

        await stream.WriteAsync(new byte[] { 7 });
        var completing = stream.CompleteAsync(CancellationToken.None);

        Assert.Equal(new byte[] { 7 }, (await ReceiveMessageAsync(pair.Server)).Bytes);
        Assert.Equal(WebSocketMessageType.Close, (await ReceiveMessageAsync(pair.Server)).Type);
        Assert.Equal(EndOfStream, pair.Server.CloseStatus);

        await Task.Delay(200);
        Assert.False(completing.IsCompleted);

        await pair.Server.CloseOutputAsync(EndOfStream, null, CancellationToken.None);
        await completing.WaitAsync(TestBound);
        Assert.Null(stream.Failure);
    }

    [Fact]
    public async Task TheHubHoldingTheStreamNoLonger_IsNoticedWithNothingBeingWritten()
    {
        await using var pair = await WebSocketPair.OpenAsync();
        await using var stream = new WebSocketWriteStream(pair.Client, BufferSize);

        await pair.Server.CloseOutputAsync(StreamOver, null, CancellationToken.None);

        await Task.Delay(Timeout.InfiniteTimeSpan, stream.Lost).ContinueWith(_ => { }).WaitAsync(TestBound);
        var refused = Assert.IsType<StreamRefusedException>(stream.Failure);
        Assert.Equal(StreamOutcome.Retryable, refused.Outcome);
    }

    [Fact]
    public async Task ARefusal_ReleasesAWriteBlockedOnAFullSocket()
    {
        await using var pair = await WebSocketPair.OpenAsync();
        await using var stream = new WebSocketWriteStream(pair.Client, 1024 * 1024);

        // Nobody reads on the hub's side, so the socket fills.
        var writing = Task.Run(async () =>
        {
            while (true)
            {
                await stream.WriteAsync(new byte[1024 * 1024], stream.Lost);
                await stream.FlushAsync(stream.Lost);
            }
        });
        await Task.Delay(1000);
        Assert.False(writing.IsCompleted);

        await pair.Server.CloseOutputAsync(StreamOver, null, CancellationToken.None);

        await Assert.ThrowsAnyAsync<Exception>(() => writing.WaitAsync(TestBound));
        Assert.IsType<StreamRefusedException>(stream.Failure);
    }

    [Fact]
    public async Task ACut_IsNoticedWithNothingBeingWritten_AndIsNotARefusal()
    {
        await using var pair = await WebSocketPair.OpenAsync();
        await using var stream = new WebSocketWriteStream(pair.Client, BufferSize);

        pair.Server.Abort();

        await Task.Delay(Timeout.InfiniteTimeSpan, stream.Lost).ContinueWith(_ => { }).WaitAsync(TestBound);
        Assert.IsType<IOException>(stream.Failure);
    }

    [Theory]
    [InlineData(WebSocketCloseStatus.NormalClosure)]
    [InlineData(WebSocketCloseStatus.EndpointUnavailable)]
    public async Task ACloseOfAnotherStatus_IsACut(WebSocketCloseStatus status)
    {
        // A front end that cuts with a Close of its own must not read as the hub having closed the stream.
        await using var pair = await WebSocketPair.OpenAsync();
        await using var stream = new WebSocketWriteStream(pair.Client, BufferSize);

        var completing = stream.CompleteAsync(CancellationToken.None);
        await ReceiveMessageAsync(pair.Server);
        await pair.Server.CloseOutputAsync(status, null, CancellationToken.None);

        await Assert.ThrowsAsync<IOException>(() => completing.WaitAsync(TestBound));
    }

    [Fact]
    public async Task AnAnswerToAnEndNeverSent_IsACut()
    {
        await using var pair = await WebSocketPair.OpenAsync();
        await using var stream = new WebSocketWriteStream(pair.Client, BufferSize);

        await pair.Server.CloseOutputAsync(EndOfStream, null, CancellationToken.None);

        await Task.Delay(Timeout.InfiniteTimeSpan, stream.Lost).ContinueWith(_ => { }).WaitAsync(TestBound);
        Assert.IsType<IOException>(stream.Failure);
    }

    [Fact]
    public async Task DataFromTheHub_IsAViolation()
    {
        await using var pair = await WebSocketPair.OpenAsync();
        await using var stream = new WebSocketWriteStream(pair.Client, BufferSize);

        await pair.Server.SendAsync(new byte[] { 1 }, WebSocketMessageType.Binary, endOfMessage: true, CancellationToken.None);

        await Task.Delay(Timeout.InfiniteTimeSpan, stream.Lost).ContinueWith(_ => { }).WaitAsync(TestBound);
        Assert.IsType<WireProtocolViolationException>(stream.Failure);
    }

    [Fact]
    public async Task APeerThatStopsReading_BlocksTheWrite_AndNothingIsLostWhenItResumes()
    {
        await using var pair = await WebSocketPair.OpenAsync();
        await using var stream = new WebSocketWriteStream(pair.Client, 64 * 1024);

        const int total = 32 * 1024 * 1024;
        var bytes = RandomNumberGenerator.GetBytes(total);
        var writing = Task.Run(async () =>
        {
            await stream.WriteAsync(bytes);
            await stream.FlushAsync();
        });

        await Task.Delay(1000);
        Assert.False(writing.IsCompleted);

        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        var buffer = new byte[64 * 1024];
        var read = 0;
        while (read < total)
        {
            var received = await pair.Server.ReceiveAsync(buffer.AsMemory(), CancellationToken.None).AsTask().WaitAsync(TestBound);
            hash.AppendData(buffer, 0, received.Count);
            read += received.Count;
        }

        await writing.WaitAsync(TestBound);
        Assert.Equal(SHA256.HashData(bytes), hash.GetHashAndReset());
    }

    [Fact]
    public async Task Disposing_ReturnsAtOnce_WhateverTheHubDoes()
    {
        await using var pair = await WebSocketPair.OpenAsync();
        var stream = new WebSocketWriteStream(pair.Client, BufferSize);

        await stream.DisposeAsync().AsTask().WaitAsync(TestBound);
    }
}
