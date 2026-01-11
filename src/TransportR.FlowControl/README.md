# TransportR.FlowControl

Group-based access control for TransportR data transfers.

## Purpose

Provides authorization logic to control which senders can send data to which receivers based on group membership.

## Configuration

```csharp
services.AddSingleton<IFlowControlService>(sp =>
{
    var options = new FlowControlOptions
    {
        Groups = new()
        {
            ["senders"] = new GroupAccess { CanSendTo = ["receivers"] },
            ["admins"] = new GroupAccess { CanSendTo = ["*"] }  // Wildcard
        }
    };
    return new DefaultFlowControlService(options);
});
```

## Authorization Logic

### Multi-Group Receivers

> [!IMPORTANT]
> When a receiver belongs to **multiple groups**, the sender must have permission to send to **ALL** of the receiver's groups.

**Example:**
- Receiver belongs to: `[group-a, group-b]`
- Sender can send to: `[group-a]`
- **Result:** ❌ Denied (sender cannot reach `group-b`)

**Example:**
- Receiver belongs to: `[group-a, group-b]`
- Sender can send to: `[group-a, group-b]`
- **Result:** ✅ Allowed (all receiver groups covered)

### Multi-Group Senders

When a sender belongs to multiple groups, their permissions are **combined**:

**Example:**
- Sender belongs to: `[role-a, role-b]`
- `role-a` can send to: `[target-a]`
- `role-b` can send to: `[target-b]`
- Receiver belongs to: `[target-a, target-b]`
- **Result:** ✅ Allowed (combined permissions cover all targets)

### Wildcard

Use `"*"` to allow a group to send to any receiver:

```csharp
["admins"] = new GroupAccess { CanSendTo = ["*"] }
```

## Interface

```csharp
public interface IFlowControlService
{
    bool CanSendTo(IReadOnlySet<string> sourceGroups, IReadOnlySet<string> targetGroups);
}
```
