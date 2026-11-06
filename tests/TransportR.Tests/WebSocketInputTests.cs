using System.Net;
using System.Net.WebSockets;
using Microsoft.Extensions.Logging.Abstractions;
using TransportR.Hub.Streams;
using TransportR.Protocol;
using TransportR.Tests.Infrastructure;

namespace TransportR.Tests;

public class WebSocketInputTests
{
    private static readonly TimeSpan TestBound = TimeSpan.FromSeconds(10);
    private static readonly WebSocketCloseStatus EndOfStream = (WebSocketCloseStatus)DataPlane.EndOfStreamCloseStatus;

    private static WebSocketInput InputOn(WebSocketPair pair, TimeSpan? heartbeat = null) =>
        new(pair.Server, "transfer:Data", heartbeat, TimeProvider.System, NullLogger.Instance);

    private static Task SendAsync(WebSocket socket, params byte[] bytes) =>
        socket.SendAsync(bytes, WebSocketMessageType.Binary, endOfMessage: true, CancellationToken.None);

    private static async Task<List<byte>> ReadToEndAsync(Stream stream)
    {
        var all = new List<byte>();
        var buffer = new byte[4];
        int read;
        while ((read = await stream.ReadAsync(buffer).AsTask().WaitAsync(TestBound)) != 0)
        {
            all.AddRange(buffer.AsSpan(0, read));
        }

        return all;
    }

    [Fact]
    public async Task BinaryMessages_MakeUpTheStream_AndTheClientsEndIsItsEnd()
    {
        await using var pair = await WebSocketPair.OpenAsync();
        using var input = InputOn(pair);

        await SendAsync(pair.Client, 1, 2, 3, 4, 5, 6);
        await SendAsync(pair.Client, 7);
        await pair.Client.CloseOutputAsync(EndOfStream, null, CancellationToken.None);

        Assert.Equal(new byte[] { 1, 2, 3, 4, 5, 6, 7 }, await ReadToEndAsync(input));
        Assert.True(input.EndTaken);
    }

    [Fact]
    public async Task AnEmptyMessage_DoesNotEndTheStream()
    {
        await using var pair = await WebSocketPair.OpenAsync();
        using var input = InputOn(pair);

        await SendAsync(pair.Client);
        await SendAsync(pair.Client, 9);

        var buffer = new byte[4];
        Assert.Equal(1, await input.ReadAsync(buffer).AsTask().WaitAsync(TestBound));
        Assert.False(input.EndTaken);
    }

    [Theory]
    [InlineData(WebSocketCloseStatus.NormalClosure)]
    [InlineData(WebSocketCloseStatus.EndpointUnavailable)]
    public async Task ACloseOfAnotherStatus_IsACut_NotTheEndOfTheStream(WebSocketCloseStatus status)
    {
        // The hub retires a transfer on the end of its data stream: a front end that cuts a connection
        // with a Close of its own must not finish one.
        await using var pair = await WebSocketPair.OpenAsync();
        using var input = InputOn(pair);

        await pair.Client.CloseOutputAsync(status, null, CancellationToken.None);

        await Assert.ThrowsAsync<IOException>(() => input.ReadAsync(new byte[4]).AsTask().WaitAsync(TestBound));
        Assert.False(input.EndTaken);
    }

    [Fact]
    public async Task AClientThatGoesAway_ReadsAsACut()
    {
        await using var pair = await WebSocketPair.OpenAsync();
        using var input = InputOn(pair);

        var read = input.ReadAsync(new byte[4]).AsTask();
        pair.Client.Abort();

        await Assert.ThrowsAsync<IOException>(() => read.WaitAsync(TestBound));
    }

    [Fact]
    public async Task AMessageThatIsNotBinary_IsNotPartOfTheStream()
    {
        await using var pair = await WebSocketPair.OpenAsync();
        using var input = InputOn(pair);

        await pair.Client.SendAsync(ReadOnlyMemory<byte>.Empty, WebSocketMessageType.Text, endOfMessage: true, CancellationToken.None);

        await Assert.ThrowsAsync<IOException>(() => input.ReadAsync(new byte[4]).AsTask().WaitAsync(TestBound));
    }

    [Fact]
    public async Task AStreamTheBridgeGaveUpOn_IsRefusedAtTheClientsNextWrite_AndNotBefore()
    {
        // The bridge cancels its read when a peer reconnects, or the transfer ends. Told at once, the client
        // would reconnect and cut that peer in turn, and the two would go on taking turns.
        await using var pair = await WebSocketPair.OpenAsync();
        using var input = InputOn(pair);

        using var cancel = new CancellationTokenSource();
        var read = input.ReadAsync(new byte[4], cancel.Token).AsTask();
        cancel.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => read.WaitAsync(TestBound));

        var ending = input.EndAsync(CancellationToken.None);
        var received = pair.Client.ReceiveAsync(new byte[4].AsMemory(), CancellationToken.None).AsTask();
        await Task.Delay(300);
        Assert.False(received.IsCompleted);

        await SendAsync(pair.Client, 1);

        Assert.Equal(WebSocketMessageType.Close, (await received.WaitAsync(TestBound)).MessageType);
        Assert.Equal((WebSocketCloseStatus)DataPlane.StreamOverCloseStatus, pair.Client.CloseStatus);

        await pair.Client.CloseOutputAsync(WebSocketCloseStatus.NormalClosure, null, CancellationToken.None);
        await ending.WaitAsync(TestBound);
    }

    [Fact]
    public async Task AnEndTheBridgeNeverReached_IsRefused_NotAnswered()
    {
        await using var pair = await WebSocketPair.OpenAsync();
        using var input = InputOn(pair);

        await pair.Client.CloseOutputAsync(EndOfStream, null, CancellationToken.None);
        Assert.True(await input.WaitForFirstByteAsync(CancellationToken.None).WaitAsync(TestBound));

        await input.EndAsync(CancellationToken.None).WaitAsync(TestBound);

        await pair.Client.ReceiveAsync(new byte[4].AsMemory(), CancellationToken.None).AsTask().WaitAsync(TestBound);
        Assert.Equal((WebSocketCloseStatus)DataPlane.StreamOverCloseStatus, pair.Client.CloseStatus);
    }

    [Fact]
    public async Task AfterTheClientsEnd_TheHubAnswersIt_AtOnce()
    {
        await using var pair = await WebSocketPair.OpenAsync();
        using var input = InputOn(pair);

        await pair.Client.CloseOutputAsync(EndOfStream, null, CancellationToken.None);
        await ReadToEndAsync(input);
        await input.EndAsync(CancellationToken.None).WaitAsync(TestBound);

        var received = await pair.Client.ReceiveAsync(new byte[4].AsMemory(), CancellationToken.None).AsTask().WaitAsync(TestBound);

        Assert.Equal(WebSocketMessageType.Close, received.MessageType);
        Assert.Equal(EndOfStream, pair.Client.CloseStatus);
    }

    [Fact]
    public async Task Ending_OnASocketThatHasGone_DoesNotThrow()
    {
        await using var pair = await WebSocketPair.OpenAsync();
        using var input = InputOn(pair);

        pair.Client.Abort();
        await Assert.ThrowsAsync<IOException>(() => input.ReadAsync(new byte[4]).AsTask().WaitAsync(TestBound));

        await input.EndAsync(CancellationToken.None).WaitAsync(TestBound);
    }

    [Fact]
    public async Task Ending_ToAClientThatDoesNotAnswer_ReturnsAtItsBound()
    {
        await using var pair = await WebSocketPair.OpenAsync();
        using var input = InputOn(pair);

        var ending = input.EndAsync(CancellationToken.None);
        await SendAsync(pair.Client, 1);

        await ending.WaitAsync(TestBound);
    }

    [Fact]
    public async Task WaitingForTheFirstByte_EndsWithIt_AndLosesNothing()
    {
        await using var pair = await WebSocketPair.OpenAsync();
        using var input = InputOn(pair);

        var waiting = input.WaitForFirstByteAsync(CancellationToken.None);
        await Task.Delay(200);
        Assert.False(waiting.IsCompleted);

        await SendAsync(pair.Client, 3, 4);

        Assert.True(await waiting.WaitAsync(TestBound));
        var buffer = new byte[4];
        Assert.Equal(2, await input.ReadAsync(buffer).AsTask().WaitAsync(TestBound));
        Assert.Equal(new byte[] { 3, 4 }, buffer[..2]);
    }

    [Fact]
    public async Task WaitingForTheFirstByte_OnASocketThatGoesAway_SaysSo()
    {
        await using var pair = await WebSocketPair.OpenAsync();
        using var input = InputOn(pair);

        var waiting = input.WaitForFirstByteAsync(CancellationToken.None);
        pair.Client.Abort();

        Assert.False(await waiting.WaitAsync(TestBound));
    }

    [Fact]
    public async Task AProbe_IsAnsweredWithAnEmptyTextMessage()
    {
        await using var pair = await WebSocketPair.OpenAsync();
        using var input = InputOn(pair);

        var answering = input.AnswerProbeAsync(CancellationToken.None);
        await pair.Client.SendAsync(ReadOnlyMemory<byte>.Empty, WebSocketMessageType.Text, endOfMessage: true, CancellationToken.None);

        Assert.True(await answering.WaitAsync(TestBound));

        var answer = await pair.Client.ReceiveAsync(new byte[4].AsMemory(), CancellationToken.None).AsTask().WaitAsync(TestBound);
        Assert.Equal(WebSocketMessageType.Text, answer.MessageType);
        Assert.Equal(0, answer.Count);
    }

    [Fact]
    public async Task AnOpeningWithoutAProbe_IsNotAnswered()
    {
        await using var pair = await WebSocketPair.OpenAsync();
        using var input = InputOn(pair);

        await SendAsync(pair.Client, 1);

        Assert.False(await input.AnswerProbeAsync(CancellationToken.None).WaitAsync(TestBound));
    }

    [Fact]
    public async Task AReadLeftWaitingForTwoIntervals_IsACut()
    {
        await using var pair = await WebSocketPair.OpenAsync();
        using var input = InputOn(pair, heartbeat: TimeSpan.FromMilliseconds(100));

        input.JudgeSilence();

        await Assert.ThrowsAsync<IOException>(() => input.ReadAsync(new byte[4]).AsTask().WaitAsync(TestBound));
    }

    [Fact]
    public async Task AHubParkedOnItsPeer_DoesNotJudgeTheClient()
    {
        await using var pair = await WebSocketPair.OpenAsync();
        using var input = InputOn(pair, heartbeat: TimeSpan.FromMilliseconds(100));

        input.JudgeSilence();
        await Task.Delay(TimeSpan.FromMilliseconds(600));

        await SendAsync(pair.Client, 5);

        Assert.Equal(1, await input.ReadAsync(new byte[4]).AsTask().WaitAsync(TestBound));
    }

    [Fact]
    public async Task AStreamWithoutAnInterval_IsNeverJudged()
    {
        await using var pair = await WebSocketPair.OpenAsync();
        using var input = InputOn(pair);

        input.JudgeSilence();
        var read = input.ReadAsync(new byte[4]).AsTask();
        await Task.Delay(TimeSpan.FromMilliseconds(400));
        Assert.False(read.IsCompleted);

        await SendAsync(pair.Client, 5);

        Assert.Equal(1, await read.WaitAsync(TestBound));
    }
}

public class WebSocketRouteTests
{
    private static readonly TimeSpan TestBound = TimeSpan.FromSeconds(10);

    [Fact]
    public async Task ARequestThatLostItsUpgrade_IsAnswered426_AndOpensNothing()
    {
        await using var hub = await TestHubServerFactory.StartLoopbackAsync();
        using var http = new HttpClient();

        using var response = await http.GetAsync($"{hub.BaseUrl}{WireRoutes.StreamBase}/{Guid.NewGuid()}/send").WaitAsync(TestBound);

        Assert.Equal(HttpStatusCode.UpgradeRequired, response.StatusCode);
        Assert.Equal("websocket", response.Headers.Upgrade.Single().Name);
    }

    [Fact]
    public async Task AnUpgradeOfATransferTheHubDoesNotKnow_IsRefused_BeforeAnythingOpens()
    {
        await using var hub = await TestHubServerFactory.StartLoopbackAsync();
        using var client = new ClientWebSocket();
        client.Options.CollectHttpResponseDetails = true;
        client.Options.SetRequestHeader("X-Connection-Type", "Data");

        var uri = new Uri($"{hub.BaseUrl.Replace("http://", "ws://")}{WireRoutes.StreamBase}/{Guid.NewGuid()}/send?{DataPlane.HeartbeatParameter}=15000");
        await Assert.ThrowsAsync<WebSocketException>(() => client.ConnectAsync(uri, CancellationToken.None).WaitAsync(TestBound));

        Assert.Equal(HttpStatusCode.NotFound, client.HttpStatusCode);
        Assert.Contains(client.HttpResponseHeaders!, header => string.Equals(header.Key, WireRoutes.RefusedHeader, StringComparison.OrdinalIgnoreCase));
    }
}
