using System.Text;
using Microsoft.Extensions.Logging;
using Microsoft.IO;
using TransportR.Interfaces;
using TransportR.Client.Sessions;
using TransportR.Client.Configuration;

namespace TransportR.Tests.TestHelpers;

internal class StubSessionContext: ISessionContext<string>
{
    public Guid ClientId { get; } = Guid.NewGuid();
    public ILogger Logger { get; }
    public IBatchSerializer<string> Serializer { get; } = new StubBatchSerializer();
    public HttpClient HttpClient { get; }
    
    public int BatchFlushIntervalMs => 50;
    public RecyclableMemoryStreamManager StreamManager { get; } = new RecyclableMemoryStreamManager();
    public ClientOptions Options { get; } = new ClientOptions();

    public StubSessionContext(HttpClient? httpClient = null, ILogger? logger = null)
    {
        HttpClient = httpClient ?? new HttpClient();
        Logger = logger ?? new StubLogger();
    }

    public int RemoveSessionCount { get; private set; }
    public void RemoveSession(string transferId) 
    {
        RemoveSessionCount++;
    }
    
    public Task<string?> GetAccessTokenAsync(bool forceRefresh = false) => Task.FromResult<string?>(null);

    /// <summary>Whether the probe of a reconnecting session finds the hub answering; nobody answers by default.</summary>
    public bool ProbeAnswers { get; init; }

    // No hub behind this context: a heartbeat goes nowhere.
    public Task ReportTransferAsync(string transferId, string? affinity, long items, long acks, CancellationToken cancellationToken) => Task.CompletedTask;

    public Task<bool> ProbeAsync(string transferId, string? affinity, CancellationToken cancellationToken) => Task.FromResult(ProbeAnswers);
}

public class StubLogger : ILogger, IDisposable
{
    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) { }
    public bool IsEnabled(LogLevel logLevel) => true;
    public IDisposable? BeginScope<TState>(TState state) where TState : notnull => this;
    public void Dispose() { }
}

public class StubBatchSerializer : IBatchSerializer<string>
{
    public Task SerializeBatchAsync(Stream destination, List<string> batch)
    {
        // Simple serialization
        using var writer = new BinaryWriter(destination, Encoding.UTF8, leaveOpen: true);
        writer.Write(batch.Count);
        writer.Write(new byte[1]); 
        return Task.CompletedTask;
    }

    public Task<List<string>?> DeserializeBatchAsync(Stream source)
    {
        using var reader = new BinaryReader(source, Encoding.UTF8, leaveOpen: true);
        try
        {
            var count = reader.ReadInt32();
            var bytes = reader.ReadBytes(1);
            var list = new List<string>();
            for(int i=0; i<count; i++) list.Add(default!);
            return Task.FromResult<List<string>?>(list);
        }
        catch(EndOfStreamException) { return Task.FromResult<List<string>?>(null); }
    }
}
