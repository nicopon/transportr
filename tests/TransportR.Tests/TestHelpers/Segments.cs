using System.Net;
using System.Web;
using TransportR.Protocol;

namespace TransportR.Tests.TestHelpers;

/// <summary>
/// How a stub hub takes a segment of a write stream the way the hub does when the peer reads at
/// once: the body is relayed whole, and the answer says so.
/// </summary>
internal static class Segments
{
    /// <summary>Copies the segment's body to <paramref name="sink"/> and answers it as relayed.</summary>
    public static async Task<HttpResponseMessage> RelayAsync(HttpRequestMessage request, Stream sink, CancellationToken cancellationToken)
    {
        var body = await request.Content!.ReadAsByteArrayAsync(cancellationToken);
        await sink.WriteAsync(body, cancellationToken);
        return Answer(HttpStatusCode.OK, Offset(request) + body.Length);
    }

    /// <summary>An answer to a segment, with how much of its epoch's stream was relayed.</summary>
    public static HttpResponseMessage Answer(HttpStatusCode status, long relayed)
    {
        var response = new HttpResponseMessage(status);
        response.Headers.Add(DataPlane.RelayedHeader, relayed.ToString());
        return response;
    }

    public static long Offset(HttpRequestMessage request) =>
        long.Parse(Query(request)[DataPlane.OffsetParameter]!);

    public static Guid Epoch(HttpRequestMessage request) =>
        Guid.Parse(Query(request)[DataPlane.EpochParameter]!);

    /// <summary>Whether this is the segment that ends its stream.</summary>
    public static bool IsFinal(HttpRequestMessage request) =>
        Query(request)[DataPlane.FinalParameter] == "true";

    private static System.Collections.Specialized.NameValueCollection Query(HttpRequestMessage request) =>
        HttpUtility.ParseQueryString(request.RequestUri!.Query);
}
