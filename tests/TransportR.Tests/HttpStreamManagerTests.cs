using System.Net;
using TransportR.Client.SignalR.Internal;
using TransportR.Client.SignalR.Sessions;
using TransportR.Client.SignalR.Configuration;
using TransportR.Protocol;
using Microsoft.Extensions.Logging;
using Microsoft.AspNetCore.SignalR.Client;
using Microsoft.IO;

namespace TransportR.Tests;

public class HttpStreamManagerTests
{
    private readonly StubSessionContext _stubContext;
    private readonly StubHttpMessageHandler _httpHandler;
    private readonly string _transferId = Guid.NewGuid().ToString();

    public HttpStreamManagerTests()
    {
        _httpHandler = new StubHttpMessageHandler();
        var httpClient = new HttpClient(_httpHandler) { BaseAddress = new Uri("http://test/") };
        
        _stubContext = new StubSessionContext(httpClient);
    }

    [Fact]
    public async Task StartStreams_Success_ReturnsSuccessResult()
    {
        // Arrange
        await using var manager = new HttpStreamManager(_stubContext, _stubContext, _transferId, "http://test/data");
        _httpHandler.SetResponse(HttpStatusCode.OK);

        // Act
        manager.StartStreams(
            isSender: true, 
            writeAction: (s, t) => Task.CompletedTask, 
            readAction: (s, t) => Task.CompletedTask, 
            sessionToken: CancellationToken.None);

        var result = await manager.SenderTask!;

        // Assert
        Assert.True(result.IsSuccess);
        Assert.False(result.IsRetryable);
    }

    [Fact]
    public async Task StartStreams_404_ReturnsFatalFailure()
    {
        // Arrange
        await using var manager = new HttpStreamManager(_stubContext, _stubContext, _transferId, "http://test/data");
        _httpHandler.SetResponse(HttpStatusCode.NotFound);

        // Act
        manager.StartStreams(
            isSender: true, 
            writeAction: (s, t) => Task.CompletedTask, 
            readAction: (s, t) => Task.CompletedTask, 
            sessionToken: CancellationToken.None);

        var result = await manager.SenderTask!;

        // Assert
        Assert.False(result.IsSuccess);
        Assert.False(result.IsRetryable); // Fatal
        Assert.Contains("Transfer Gone", result.ErrorMessage);
    }

    [Fact]
    public async Task StartStreams_500_ReturnsRetryableFailure()
    {
        // Arrange
        await using var manager = new HttpStreamManager(_stubContext, _stubContext, _transferId, "http://test/data");
        _httpHandler.SetResponse(HttpStatusCode.InternalServerError);

        // Act
        manager.StartStreams(
            isSender: true, 
            writeAction: (s, t) => Task.CompletedTask, 
            readAction: (s, t) => Task.CompletedTask, 
            sessionToken: CancellationToken.None);

        var result = await manager.SenderTask!;

        // Assert
        Assert.False(result.IsSuccess);
        Assert.True(result.IsRetryable); // Retryable
    }

    [Fact]
    public async Task StartStreams_ProtocolViolation_ReturnsFatalFailure()
    {
        // Arrange
        await using var manager = new HttpStreamManager(_stubContext, _stubContext, _transferId, "http://test/data");
        _httpHandler.SetResponse(HttpStatusCode.OK);

        // Act
        manager.StartStreams(
            isSender: true,
            writeAction: (s, t) => Task.CompletedTask,
            readAction: (s, t) => throw new WireProtocolViolationException("Unsupported protocol version: 1 (expected 2)"),
            sessionToken: CancellationToken.None);

        var result = await manager.SenderTask!;

        // Assert: a protocol violation is never worth retrying - reconnecting repeats it.
        Assert.False(result.IsSuccess);
        Assert.False(result.IsRetryable);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task StartStreams_AsksForHttp11Exactly_InBothDirections(bool isSender)
    {
        // The data plane is HTTP/1.1 on purpose, and the defaults are not what says so: a request
        // built by hand ignores the client's DefaultRequestVersion, and "1.1 or lower" is silent
        // about higher ones. Both directions are checked because each has its own request.
        var capture = new CapturingHandler();
        var client = new HttpClient(capture)
        {
            DefaultRequestVersion = HttpVersion.Version20,
            DefaultVersionPolicy = HttpVersionPolicy.RequestVersionOrHigher,
        };
        var context = new StubSessionContext(client);
        await using var manager = new HttpStreamManager(context, context, _transferId, "http://test/data");

        manager.StartStreams(
            isSender,
            writeAction: (s, t) => Task.CompletedTask,
            readAction: (s, t) => Task.CompletedTask,
            sessionToken: CancellationToken.None);
        await (isSender ? manager.SenderTask! : manager.ReceiverTask!);

        Assert.Equal(2, capture.Requests.Count);
        Assert.All(capture.Requests, request =>
        {
            Assert.Equal(HttpVersion.Version11, request.Version);
            Assert.Equal(HttpVersionPolicy.RequestVersionExact, request.VersionPolicy);
        });
    }

    // ========== STUBS ==========

    private class CapturingHandler : HttpMessageHandler
    {
        private readonly List<(Version Version, HttpVersionPolicy VersionPolicy)> _requests = new();

        public IReadOnlyList<(Version Version, HttpVersionPolicy VersionPolicy)> Requests
        {
            get { lock (_requests) return _requests.ToList(); }
        }

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            lock (_requests) _requests.Add((request.Version, request.VersionPolicy));

            // A write request has to be consumed, as the server would, or its body callback never runs.
            if (request.Content != null) await request.Content.CopyToAsync(Stream.Null, cancellationToken);
            return new HttpResponseMessage(HttpStatusCode.OK) { Content = new ByteArrayContent(Array.Empty<byte>()) };
        }
    }

    private class StubHttpMessageHandler : HttpMessageHandler
    {
        private HttpStatusCode _statusCode = HttpStatusCode.OK;

        public void SetResponse(HttpStatusCode code)
        {
            _statusCode = code;
        }

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var response = new HttpResponseMessage(_statusCode);
            response.Content = new ByteArrayContent(Array.Empty<byte>());
            return Task.FromResult(response);
        }
    }

    private class StubSessionContext : ISessionContext
    {
        public ILogger Logger { get; } = new StubLogger();
        public HttpClient HttpClient { get; }
        
        public HubConnection HubConnection => null!;
        public int BatchFlushIntervalMs => 50;
        public RecyclableMemoryStreamManager StreamManager { get; } = new RecyclableMemoryStreamManager();
        public ClientOptions Options { get; } = new ClientOptions();
        public StubSessionContext(HttpClient httpClient)
        {
            HttpClient = httpClient;
        }

        public void RemoveSession(string transferId) { }
        
        public Task<string?> GetAccessTokenAsync(bool forceRefresh = false) => Task.FromResult<string?>(null);
    }
    
    // Reuse StubLogger from others or duplicate for isolation
    private class StubLogger : ILogger, IDisposable
    {
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) { }
        public bool IsEnabled(LogLevel logLevel) => true;
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => this;
        public void Dispose() { }
    }
}
