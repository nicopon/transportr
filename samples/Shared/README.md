# Shared - Technical Documentation

> Shared library containing the Order model.

## 📋 Responsibilities

This project contains:
1. The **Order** data model used throughout the solution.


---

## 🏗 Project Structure

```
Shared/

├── Models/
│   └── Order.cs                       # Data Model
└── Shared.csproj
```

---

## 📊 Order Model

```csharp
public class Order
{
    public int OrderId { get; set; }
    public DateTime OrderDate { get; set; }
    public string CustomerName { get; set; } = string.Empty;
    public string CustomerEmail { get; set; } = string.Empty;
    public string ShippingAddress { get; set; } = string.Empty;
    public string BillingAddress { get; set; } = string.Empty;
    public decimal TotalAmount { get; set; }
    public decimal TaxAmount { get; set; }
    public decimal DiscountAmount { get; set; }
    public string Currency { get; set; } = "USD";
    public string PaymentMethod { get; set; } = string.Empty;
    public string Status { get; set; } = string.Empty;
    public string ShippingPriority { get; set; } = string.Empty;
    public string TrackingNumber { get; set; } = string.Empty;
    public string Notes { get; set; } = string.Empty;
}
```

### Fields

| Field | Type | Description |
|-------|------|-------------|
| `OrderId` | int | Unique identifier |
| `OrderDate` | DateTime | Order date |
| `CustomerName` | string | Customer name |
| `CustomerEmail` | string | Customer email |
| `ShippingAddress` | string | Shipping address |
| `BillingAddress` | string | Billing address |
| `TotalAmount` | decimal | Total amount |
| `TaxAmount` | decimal | Taxes |
| `DiscountAmount` | decimal | Discount |
| `Currency` | string | Currency code (USD, EUR, ...) |
| `PaymentMethod` | string | Payment method |
| `Status` | string | Status (Pending, Shipped, ...) |
| `ShippingPriority` | string | Priority (Standard, Express, ...) |
| `TrackingNumber` | string | Tracking number |
| `Notes` | string | Free notes |

---



---
