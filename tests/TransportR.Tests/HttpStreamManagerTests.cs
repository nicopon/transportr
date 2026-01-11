using System.Net;
using System.Security.Cryptography.X509Certificates;
using TransportR.Client.SignalR.Internal;
using TransportR.Client.SignalR.Sessions;
using TransportR.Client.SignalR.Configuration;
using TransportR.Interfaces;
using Xunit;
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

    // ========== STUBS ==========

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
        public Guid ClientId { get; } = Guid.NewGuid();
        public ILogger Logger { get; } = new StubLogger();
        public IBatchSerializer Serializer { get; } = new StubBatchSerializer();
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
    
    // Reuse StubLogger/Serializer from others or duplicate for isolation
    private class StubLogger : ILogger, IDisposable
    {
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) { }
        public bool IsEnabled(LogLevel logLevel) => true;
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => this;
        public void Dispose() { }
    }

    private class StubBatchSerializer : IBatchSerializer
    {
        public Task SerializeBatchAsync<T>(Stream destination, List<T> batch) => Task.CompletedTask;
        public Task<List<T>?> DeserializeBatchAsync<T>(Stream source) => Task.FromResult<List<T>?>(new List<T>());
    }
}
