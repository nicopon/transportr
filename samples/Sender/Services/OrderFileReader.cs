using System.Runtime.CompilerServices;
using MessagePack;
using MessagePack.Resolvers;
using Microsoft.Extensions.Logging;
using Shared.Models;

namespace Sender.Services;

public class OrderFileReader
{
    private readonly string _filePath;
    private readonly ILogger<OrderFileReader> _logger;
    private readonly MessagePackSerializerOptions _options;

    public OrderFileReader(string filePath, ILogger<OrderFileReader> logger)
    {
        _filePath = filePath;
        _logger = logger;
        // Ensure strictly StandardResolver for compatibility with DatabaseInitializer
        _options = MessagePackSerializerOptions.Standard
            .WithResolver(StandardResolver.Instance);
    }

#pragma warning disable CS1998 // Async method lacks 'await'
    public async IAsyncEnumerable<Order> GetOrdersStreamAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
#pragma warning restore CS1998
    {
        if (!File.Exists(_filePath))
        {
            _logger.LogError("File not found: {FilePath}", _filePath);
            yield break;
        }

        _logger.LogInformation("Reading MessagePack file (optimized): {FilePath}", _filePath);

        // Optimized FileStream configuration
        const int BufferSize = 4 * 1024 * 1024; // 4MB
        using var fs = new FileStream(
            _filePath,
            FileMode.Open,
            FileAccess.Read,
            FileShare.Read,
            bufferSize: BufferSize,
            FileOptions.SequentialScan);

        using var reader = new BinaryReader(fs);

        while (fs.Position < fs.Length && !cancellationToken.IsCancellationRequested)
        {
            // Read size (4 bytes)
            int size;
            try
            {
                size = reader.ReadInt32();
            }
            catch (EndOfStreamException)
            {
                break;
            }

            // Read data directly into pooled buffer
            var buffer = System.Buffers.ArrayPool<byte>.Shared.Rent(size);
            try
            {
                int bytesRead = reader.Read(buffer, 0, size);
                if (bytesRead != size)
                {
                    _logger.LogWarning("Unexpected end of file while reading MessagePack message.");
                    break;
                }

                // Deserialize and yield
                // Using ReadOnlyMemory to avoid copying if possible, though Rent is byte[]
                var order = MessagePackSerializer.Deserialize<Order>(buffer.AsMemory(0, size), _options);
                yield return order;
            }
            finally
            {
                System.Buffers.ArrayPool<byte>.Shared.Return(buffer);
            }
        }
    }
}
