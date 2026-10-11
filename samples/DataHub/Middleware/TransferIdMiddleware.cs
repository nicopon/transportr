using Microsoft.Extensions.Logging;

namespace DataHub.Middleware;

public class TransferIdMiddleware
{
    private readonly RequestDelegate _next;
    private readonly ILogger<TransferIdMiddleware> _logger;
    private const string TransferIdHeader = "X-Transfer-Id";

    public TransferIdMiddleware(RequestDelegate next, ILogger<TransferIdMiddleware> logger)
    {
        _next = next;
        _logger = logger;
    }

    public async Task InvokeAsync(HttpContext context)
    {
        // Get transfer ID from header or generate a new one
        var transferId = context.Request.Headers[TransferIdHeader].FirstOrDefault()
                          ?? Guid.NewGuid().ToString();

        // Add ID to context for logging
        context.Items["TransferId"] = transferId;

        // Add ID to response
        context.Response.Headers.TryAdd(TransferIdHeader, transferId);

        // Structured log scope
        using (_logger.BeginScope(new Dictionary<string, object>
        {
            ["TransferId"] = transferId
        }))
        {
            _logger.LogDebug("Request {Method} {Path} - TransferId: {TransferId}",
                context.Request.Method, context.Request.Path, transferId);

            await _next(context);
        }
    }
}

