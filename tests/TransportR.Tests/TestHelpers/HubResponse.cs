using System.Net;
using TransportR.Protocol;

namespace TransportR.Tests.TestHelpers;

/// <summary>A response as the hub answers it, for a double that stands for the hub.</summary>
internal static class HubResponse
{
    /// <summary>A response with <paramref name="status"/>; a 404 carries the hub's mark, as every refusal of its own does.</summary>
    public static HttpResponseMessage With(HttpStatusCode status)
    {
        var response = new HttpResponseMessage(status);
        if (status == HttpStatusCode.NotFound) response.Headers.Add(WireRoutes.RefusedHeader, "1");
        return response;
    }
}
