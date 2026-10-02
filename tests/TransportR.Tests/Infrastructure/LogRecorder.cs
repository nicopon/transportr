using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace TransportR.Tests.Infrastructure;

/// <summary>Keeps what is logged, for a test about something a client says and does not do.</summary>
public sealed class LogRecorder : ILoggerProvider
{
    private readonly ConcurrentQueue<(LogLevel Level, string Message)> _entries = new();

    public ILoggerFactory Factory => LoggerFactory.Create(builder => builder.AddProvider(this).SetMinimumLevel(LogLevel.Trace));

    public bool Has(LogLevel level, string text) =>
        _entries.Any(e => e.Level == level && e.Message.Contains(text, StringComparison.Ordinal));

    public ILogger CreateLogger(string categoryName) => new Recording(_entries);

    public void Dispose()
    {
    }

    private sealed class Recording(ConcurrentQueue<(LogLevel Level, string Message)> entries) : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
            entries.Enqueue((logLevel, formatter(state, exception)));
    }
}
