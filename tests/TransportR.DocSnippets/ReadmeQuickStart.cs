using MessagePack;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using TransportR.Client.SignalR;
using TransportR.Hub.SignalR;
using TransportR.Serialization.MessagePack;

namespace TransportR.DocSnippets;

/// <summary>
/// The three Quick Start snippets of the root <c>README.md</c>, compiled.
/// </summary>
/// <remarks>
/// <para>
/// Nothing here runs, and nothing here is a test: this project exists so that an example which
/// stops compiling fails the build instead of waiting for a reader to try it. The hub snippet had
/// been calling a client-side extension and omitting <c>Build()</c> since the day it was written,
/// and no gate had anything to say about it.
/// </para>
/// <para>
/// <b>Edit these bodies and the README together.</b> A snippet that compiles but no longer matches
/// the document is worse than one that does neither, because this project then certifies it.
/// </para>
/// </remarks>
internal static class ReadmeQuickStart
{
    // 1. Create the Hub (Server)
    public static void Hub(string[] args)
    {
        var builder = WebApplication.CreateBuilder(args);

        builder.Services.AddDataHub()
            .UseDevMode()   // trusts client-provided identity — see the Security guide before production
            .Build();

        var app = builder.Build();
        app.MapDataHub();
        app.Run();
    }

    // 2. Send Data (Sender Client)
    public static async Task Sender()
    {
        // Connect to hub
        var sender = new TransportRClientBuilder<Order>()
            .WithUrl("http://localhost:5252")
            .WithMessagePackSerialization()
            .Build();

        await sender.ConnectAsync();

        // Find a receiver and start transfer
        var receivers = await sender.GetReceiversAsync();
        var transfer = await sender.InitTransferAsync(receivers[0].ClientId);

        // Stream data (auto-batched, auto-compressed)
        foreach (var order in GetOrders())
            await transfer.SendAsync(order);

        await transfer.CompleteAsync();
    }

    // 3. Receive Data (Receiver Client)
    public static async Task Receiver()
    {
        var receiver = new TransportRClientBuilder<Order>()
            .WithUrl("http://localhost:5252")
            .WithMessagePackSerialization()
            .Build();

        receiver.OnTransferStarted += async transfer =>
        {
            // The handler owns the handle and must release it -- but only once the transfer has
            // closed: the loop below ends at the last item, which is before the sender has been
            // told so.
            await using (transfer)
            {
                await foreach (var order in transfer.ReceiveAsync())
                {
                    Console.WriteLine($"Received: {order.Id}");
                }

                await transfer.WaitForCompletionAsync();
            }
        };

        await receiver.ConnectAsync();
        await Task.Delay(Timeout.Infinite); // Keep listening
    }

    private static IEnumerable<Order> GetOrders() => [];
}

/// <summary>Stands in for the reader's own item type.</summary>
[MessagePackObject]
public class Order
{
    [Key(0)]
    public int Id { get; set; }
}
