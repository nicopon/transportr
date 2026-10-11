namespace Shared.Models;

using System.ComponentModel;
using MessagePack;
using ProtoBuf;

/// <summary>
/// Represents an order in the system
/// </summary>
[MessagePackObject]
[ProtoContract]
public class Order
{
    [Key(0)]
    [ProtoMember(1)]
    public int OrderId { get; set; }
    [Key(1)]
    [ProtoMember(2)]
    public DateTime OrderDate { get; set; }
    [Key(2)]
    [ProtoMember(3)]
    public string CustomerName { get; set; } = string.Empty;
    [Key(3)]
    [ProtoMember(4)]
    public string CustomerEmail { get; set; } = string.Empty;
    [Key(4)]
    [ProtoMember(5)]
    public string ShippingAddress { get; set; } = string.Empty;
    [Key(5)]
    [ProtoMember(6)]
    public string BillingAddress { get; set; } = string.Empty;
    [Key(6)]
    [ProtoMember(7)]
    public decimal TotalAmount { get; set; }
    [Key(7)]
    [ProtoMember(8)]
    public decimal TaxAmount { get; set; }
    [Key(8)]
    [ProtoMember(9)]
    public decimal DiscountAmount { get; set; }
    [Key(9)]
    [ProtoMember(10)]
    [DefaultValue("EUR")]
    public string Currency { get; set; } = "EUR";
    [Key(10)]
    [ProtoMember(11)]
    public string PaymentMethod { get; set; } = string.Empty;
    [Key(11)]
    [ProtoMember(12)]
    public string Status { get; set; } = string.Empty;
    [Key(12)]
    [ProtoMember(13)]
    public string ShippingPriority { get; set; } = string.Empty;
    [Key(13)]
    [ProtoMember(14)]
    public string TrackingNumber { get; set; } = string.Empty;
    [Key(14)]
    [ProtoMember(15)]
    public string Notes { get; set; } = string.Empty;
}
