using Bogus;
using MessagePack;
using MessagePack.Resolvers;
using Shared.Models;

namespace DatabaseInitializer;

class Program
{
    static void Main()
    {
        var linesEnv = Environment.GetEnvironmentVariable("ITEM_COUNT");
        var orderCount = 100000; // Default

        if (int.TryParse(linesEnv, out int parsedLines) && parsedLines >= 1)
        {
            orderCount = parsedLines;
        }
        else
        {
            Console.WriteLine($"ITEM_COUNT environment variable invalid or missing. Using default: {orderCount}");
        }

        Console.WriteLine("=== Data Initialization (MessagePack) ===");

        // Directory setup
        Directory.CreateDirectory("data");
        var filePath = "data/orders.bin";

        if (File.Exists(filePath))
        {
            Console.WriteLine($"Deleting existing file: {filePath}");
            File.Delete(filePath);
        }

        var startTime = DateTime.Now;

        Console.WriteLine($"\nWriting data to {filePath}...");
        Console.WriteLine($"Generating {orderCount} orders...");

        // Ensure we use the StandardResolver to support [Key] annotations
        var options = MessagePackSerializerOptions.Standard
            .WithResolver(StandardResolver.Instance);

        using (var fs = new FileStream(filePath, FileMode.Create, FileAccess.Write, FileShare.None, 65536))
        using (var writer = new BinaryWriter(fs))
        {
            var faker = CreateFaker();
            var sw = System.Diagnostics.Stopwatch.StartNew();

            for (int i = 0; i < orderCount; i++)
            {
                var order = faker.Generate();

                // Serialization using MessagePack (Annotated)
                var bytes = MessagePackSerializer.Serialize(order, options);

                // Size-Prefixed write: [Size (int32)] [Data]
                writer.Write(bytes.Length);
                writer.Write(bytes);

                if (i % 100000 == 0 && i > 0)
                {
                    Console.Write($"\rGenerated: {i}/{orderCount} ({(i / (sw.Elapsed.TotalSeconds + 0.001)):F0} items/s)");
                }
            }

            sw.Stop();
            Console.WriteLine($"\nCompleted in {sw.Elapsed.TotalSeconds:F2}s. Average speed: {(orderCount / sw.Elapsed.TotalSeconds):F0} items/s");
        }
        Console.WriteLine();

        var duration = DateTime.Now - startTime;
        Console.WriteLine($"\nCompleted in {duration.TotalSeconds:F1} seconds.");
    }

    static Faker<Order> CreateFaker()
    {
        return new Faker<Order>()
            .RuleFor(o => o.OrderId, f => f.IndexGlobal)
            .RuleFor(o => o.OrderDate, f => f.Date.Past(1))
            .RuleFor(o => o.CustomerName, f => f.Name.FullName())
            .RuleFor(o => o.CustomerEmail, f => f.Internet.Email())
            .RuleFor(o => o.ShippingAddress, f => f.Address.FullAddress())
            .RuleFor(o => o.BillingAddress, f => f.Address.FullAddress())
            .RuleFor(o => o.TotalAmount, f => f.Finance.Amount(10, 1000))
            .RuleFor(o => o.TaxAmount, f => f.Finance.Amount(1, 100))
            .RuleFor(o => o.DiscountAmount, f => f.Finance.Amount(0, 50))
            .RuleFor(o => o.Currency, f => f.Finance.Currency().Code)
            .RuleFor(o => o.PaymentMethod, f => f.Finance.TransactionType())
            .RuleFor(o => o.Status, f => f.PickRandom("Pending", "Shipped", "Delivered", "Cancelled"))
            .RuleFor(o => o.ShippingPriority, f => f.PickRandom("Standard", "Express", "Overnight"))
            .RuleFor(o => o.TrackingNumber, f => f.Commerce.Ean13())
            .RuleFor(o => o.Notes, f => f.Lorem.Sentence());
    }
}
