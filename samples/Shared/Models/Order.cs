namespace Shared.Models;

using MessagePack;

/// <summary>
/// Represents an order in the system
/// </summary>
[MessagePackObject]
public class Order
{
    [Key(0)]
    public int OrderId { get; set; }
    [Key(1)]
    public DateTime OrderDate { get; set; }
    [Key(2)]
    public string CustomerName { get; set; } = string.Empty;
    [Key(3)]
    public string CustomerEmail { get; set; } = string.Empty;
    [Key(4)]
    public string ShippingAddress { get; set; } = string.Empty;
    [Key(5)]
    public string BillingAddress { get; set; } = string.Empty;
    [Key(6)]
    public decimal TotalAmount { get; set; }
    [Key(7)]
    public decimal TaxAmount { get; set; }
    [Key(8)]
    public decimal DiscountAmount { get; set; }
    [Key(9)]
    public string Currency { get; set; } = "EUR";
    [Key(10)]
    public string PaymentMethod { get; set; } = string.Empty;
    [Key(11)]
    public string Status { get; set; } = string.Empty;
    [Key(12)]
    public string ShippingPriority { get; set; } = string.Empty;
    [Key(13)]
    public string TrackingNumber { get; set; } = string.Empty;
    [Key(14)]
    public string Notes { get; set; } = string.Empty;
}
