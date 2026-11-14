using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using TransportR.Client;
using TransportR.Hub;

namespace TransportR.DocSnippets;

/// <summary>
/// The two snippets of "Transfers the host opens" in <c>src/TransportR.Hub/README.md</c>, compiled.
/// Edit them and the document together, as <see cref="ReadmeQuickStart"/>.
/// </summary>
internal static class HubTransfersTheHostOpens
{
    public static async Task Host(WebApplication app, Guid senderId, Guid receiverId)
    {
        var initiator = app.Services.GetRequiredService<ITransferInitiator>();
        var transferId = await initiator.InitTransferAsync(senderId, receiverId, TimeSpan.FromSeconds(20));

        // To end the run before the transfer completes, on the instance that carries it:
        await app.Services.GetRequiredService<ITransferTerminator>().TerminateAsync(transferId);
    }

    public static void Sender(TransportRClient<Order> sender)
    {
        sender.OnSendRequested += send =>
        {
            _ = SendOrdersAsync(send);   // the handler runs inside the handshake: hand the handle off and return
            return Task.CompletedTask;
        };

        async Task SendOrdersAsync(Send<Order> send)
        {
            await using (send)
            {
                foreach (var order in GetOrders())
                    await send.SendAsync(order);
                await send.CompleteAsync();
            }
        }
    }

    private static IEnumerable<Order> GetOrders() => [];
}
