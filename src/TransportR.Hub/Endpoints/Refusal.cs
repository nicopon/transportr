using Microsoft.AspNetCore.Http;
using TransportR.Protocol;

namespace TransportR.Hub.Endpoints;

internal static class Refusal
{
    /// <summary>
    /// Answers 404 as the hub's own refusal, marked with <see cref="WireRoutes.RefusedHeader"/> so a
    /// client can tell it from a path no route serves.
    /// </summary>
    public static void Write(HttpResponse response)
    {
        response.Headers[WireRoutes.RefusedHeader] = "1";
        response.StatusCode = StatusCodes.Status404NotFound;
    }

    public static IResult Result() => RefusedResult.Instance;

    private sealed class RefusedResult : IResult, IStatusCodeHttpResult
    {
        public static readonly RefusedResult Instance = new();

        public int? StatusCode => StatusCodes.Status404NotFound;

        public Task ExecuteAsync(HttpContext httpContext)
        {
            Write(httpContext.Response);
            return Task.CompletedTask;
        }
    }
}
