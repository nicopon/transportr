using Microsoft.AspNetCore.Builder;

namespace TransportR.Hub.Streams;

/// <summary>
/// Brings the WebSocket middleware to the one action that takes an upgrade, so that a host has nothing
/// to add to its pipeline and the other routes are left as they are.
/// </summary>
internal sealed class WebSocketPipeline
{
    public void Configure(IApplicationBuilder app) => app.UseWebSockets();
}
