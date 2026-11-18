using System.Runtime.CompilerServices;
using MessagePack;
using Microsoft.AspNetCore.Builder;
using TransportR.Client;
using TransportR.Hub;
using TransportR.Serialization.MessagePack;

[assembly: InternalsVisibleTo("TransportR.Tests")]

namespace TransportR.DocSnippets;

/// <summary>
/// The three Quick Start snippets of the root <c>README.md</c>, compiled.
/// </summary>
/// <remarks>
/// <para>
/// This project compiles them, so an example that stops compiling fails the build. The sender and
/// the receiver also run: <c>ReadmeQuickStartTests</c>, in <c>TransportR.Tests</c>, points both at a
/// local hub and checks that every order arrives. The hub snippet is only compiled; the
/// configuration it builds runs in an integration test.
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
            .UseDevMode();   // trusts client-provided identity — see the Security guide before production

        var app = builder.Build();
        app.MapDataHub();
        app.Run();
    }

    // 2. Send Data (Sender Client)
    // The README declares hubUrl as a variable here; the snippet takes it as a parameter so that a
    // test can point it at its own hub.
    public static async Task Sender(string hubUrl)
    {
        // Connect to hub
        var sender = new TransportRClientBuilder<Order>()
            .WithUrl(hubUrl)
            .WithMessagePackSerialization()
            .Build();

        // In dev mode a client names its groups; with authentication they come from its token.
        await sender.ConnectAsync(["senders"]);

        // Find a receiver and start transfer
        var receivers = await sender.GetReceiversAsync();
        await using var transfer = await sender.InitTransferAsync(receivers[0].ClientId);

        // Stream data (auto-batched, auto-compressed)
        foreach (var order in GetOrders())
            await transfer.SendAsync(order);

        await transfer.CompleteAsync();
    }

    // 3. Receive Data (Receiver Client)
    // The README declares hubUrl and a source cancelled by Ctrl+C here; the snippet takes the url and
    // that source's token as parameters so that a test can point it at its own hub and end its wait.
    public static async Task Receiver(string hubUrl, CancellationToken stop)
    {
        var receiver = new TransportRClientBuilder<Order>()
            .WithUrl(hubUrl)
            .WithMessagePackSerialization()
            .Build();

        receiver.OnTransferStarted += async transfer =>
        {
            // The handler owns the handle and must release it. The loop below ends only once the
            // transfer has reached its outcome, not at the last item, so releasing right after it
            // is correct.
            await using (transfer)
            {
                await foreach (var order in transfer.ReceiveAsync())
                {
                    Console.WriteLine($"Received: {order.Id}");
                }
            }
        };

        await receiver.ConnectAsync(["receivers"]);
        await Task.Delay(Timeout.Infinite, stop).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing); // Keep listening until Ctrl+C
    }

    /// <summary>How many orders <see cref="Sender"/> sends: a count the test can check what arrived against.</summary>
    internal const int OrderCount = 1_000;

    // The README leaves GetOrders to the reader's own data.
    private static IEnumerable<Order> GetOrders() => Enumerable.Range(0, OrderCount).Select(id => new Order { Id = id });
}

/// <summary>Stands in for the reader's own item type.</summary>
[MessagePackObject]
public class Order
{
    [Key(0)]
    public int Id { get; set; }
}
