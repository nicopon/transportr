using TransportR.FlowControl;

namespace TransportR.Tests;

public class FlowControlServiceTests
{
    [Fact]
    public void CanSendTo_SingleGroupMatch_ReturnsTrue()
    {
        // Arrange: senders can send to receivers
        var options = new FlowControlOptions
        {
            Groups = new()
            {
                ["senders"] = new GroupAccess { CanSendTo = ["receivers"] }
            }
        };
        var service = new DefaultFlowControlService(options);
        
        // Act
        var result = service.CanSendTo(
            new HashSet<string> { "senders" },
            new HashSet<string> { "receivers" });
        
        // Assert
        Assert.True(result);
    }

    [Fact]
    public void CanSendTo_MultiGroupReceiver_AllCovered_ReturnsTrue()
    {
        // Arrange: senders can send to both group-a and group-b
        var options = new FlowControlOptions
        {
            Groups = new()
            {
                ["senders"] = new GroupAccess { CanSendTo = ["group-a", "group-b"] }
            }
        };
        var service = new DefaultFlowControlService(options);
        
        // Act: receiver is in both groups
        var result = service.CanSendTo(
            new HashSet<string> { "senders" },
            new HashSet<string> { "group-a", "group-b" });
        
        // Assert
        Assert.True(result);
    }

    [Fact]
    public void CanSendTo_MultiGroupReceiver_PartialCoverage_ReturnsFalse()
    {
        // Arrange: senders can only send to group-a, NOT group-b
        var options = new FlowControlOptions
        {
            Groups = new()
            {
                ["senders"] = new GroupAccess { CanSendTo = ["group-a"] }
            }
        };
        var service = new DefaultFlowControlService(options);
        
        // Act: receiver is in both group-a AND group-b
        var result = service.CanSendTo(
            new HashSet<string> { "senders" },
            new HashSet<string> { "group-a", "group-b" });
        
        // Assert: denied because group-b is not covered
        Assert.False(result);
    }

    [Fact]
    public void CanSendTo_Wildcard_CoversAllGroups()
    {
        // Arrange: admins can send to everyone (*)
        var options = new FlowControlOptions
        {
            Groups = new()
            {
                ["admins"] = new GroupAccess { CanSendTo = ["*"] }
            }
        };
        var service = new DefaultFlowControlService(options);
        
        // Act: receiver is in multiple groups
        var result = service.CanSendTo(
            new HashSet<string> { "admins" },
            new HashSet<string> { "group-a", "group-b", "group-c" });
        
        // Assert
        Assert.True(result);
    }

    [Fact]
    public void CanSendTo_MultiSourceGroups_CombinedCoverage_ReturnsTrue()
    {
        // Arrange: sender is in two groups, each covering different targets
        var options = new FlowControlOptions
        {
            Groups = new()
            {
                ["role-a"] = new GroupAccess { CanSendTo = ["target-a"] },
                ["role-b"] = new GroupAccess { CanSendTo = ["target-b"] }
            }
        };
        var service = new DefaultFlowControlService(options);
        
        // Act: sender has both roles, receiver has both targets
        var result = service.CanSendTo(
            new HashSet<string> { "role-a", "role-b" },
            new HashSet<string> { "target-a", "target-b" });
        
        // Assert: allowed because combined coverage covers all targets
        Assert.True(result);
    }

    [Fact]
    public void CanSendTo_EmptyGroups_ReturnsFalse()
    {
        var options = new FlowControlOptions();
        var service = new DefaultFlowControlService(options);
        
        // Empty source
        Assert.False(service.CanSendTo(
            new HashSet<string>(),
            new HashSet<string> { "receivers" }));
        
        // Empty target
        Assert.False(service.CanSendTo(
            new HashSet<string> { "senders" },
            new HashSet<string>()));
    }
}
