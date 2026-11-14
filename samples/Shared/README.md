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

`Models/Order.cs` holds the class. It carries the MessagePack contract, `[MessagePackObject]`/`[Key(n)]`, and JSON needs
none, which is what lets the samples switch serializer (`SERIALIZER_TYPE`) on the same type.

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
