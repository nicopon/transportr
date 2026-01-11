# DatabaseInitializer - Technical Documentation

> Test data generator using Bogus and MessagePack.

## Overview

This project generates a binary file containing fictitious Order records:
1. Generates realistic data with **Bogus** (faker library)
2. Serializes each order to **MessagePack** (Binary format)
3. Writes everything to `data/orders.bin`

---

## Project Structure

```
DatabaseInitializer/
├── Program.cs                   # Entry point + generation
└── DatabaseInitializer.csproj
```

---

## Usage

### Command Line

```bash
cd samples/DatabaseInitializer
ITEM_COUNT=2000000 dotnet run
```

### Via Initialization Script

```bash
./init.sh
# Or with custom count
ITEM_COUNT=500000 ./init.sh
```

---

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `ITEM_COUNT` | Number of orders to generate | `2000000` |

### CLI Arguments

```bash
dotnet run -- 500000  # Generates 500k orders
```

---

## Output Format

### File: `data/orders.bin`

```
┌────────────────────────────────────────────┐
│ [Size: Int32] [FlatBuffer: byte[]]         │ Order 1
├────────────────────────────────────────────┤
│ [Size: Int32] [FlatBuffer: byte[]]         │ Order 2
├────────────────────────────────────────────┤
│ ...                                        │
└────────────────────────────────────────────┘
```

### Typical Size

| Order Count | File Size |
|-------------|-----------|
| 100,000 | ~40 MB |
| 1,000,000 | ~400 MB |
| 2,000,000 | ~800 MB |

---

## Order Model

```csharp
public class Order
{
    public int OrderId { get; set; }
    public DateTime OrderDate { get; set; }
    public string CustomerName { get; set; }
    public string CustomerEmail { get; set; }
    public string ShippingAddress { get; set; }
    public string BillingAddress { get; set; }
    public decimal TotalAmount { get; set; }
    public decimal TaxAmount { get; set; }
    public decimal DiscountAmount { get; set; }
    public string Currency { get; set; }
    public string PaymentMethod { get; set; }
    public string Status { get; set; }
    public string ShippingPriority { get; set; }
    public string TrackingNumber { get; set; }
    public string Notes { get; set; }
}
```

---

## Bogus Generator

```csharp
var faker = new Faker<Order>()
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
```

---

## Performance

### Typical Benchmark

```
Generating 2,000,000 orders to data/orders.bin...
Generated: 100000/2000000 (45000 items/s)
Generated: 200000/2000000 (48000 items/s)
...
Completed in 42.5s. Average speed: 47,000 items/s
```

### Optimizations

- 64 KB write buffer


---

## Dependencies

| Package | Version | Usage |
|---------|---------|-------|
| `Bogus` | 35.x | Fake data generation |

| `Shared` | local | Order model + Extensions |

---

## Notes

- Existing file is **deleted** before generation.
- The `data/` directory is created automatically if it doesn't exist.
- Data is deterministic if you set a Bogus seed (not done by default).
