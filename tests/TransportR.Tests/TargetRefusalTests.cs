using System.Net;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using TransportR.Interfaces;
using TransportR.Models;
using TransportR.Protocol;
using TransportR.Tests.Infrastructure;

namespace TransportR.Tests;

/// <summary>
/// What a client is told when its transfer is refused is the same whichever way the target fails
/// it: a caller allowed to send must not learn, from the words of the answer, which clients exist
/// beyond its reach or in what state. The hub's log keeps the difference.
/// </summary>
public class TargetRefusalTests
{
    private const string OpenGroup = "test";
    private const string ClosedGroup = "closed";

    private sealed class Rules : IFlowControlService
    {
        public bool CanSendTo(IReadOnlySet<string> sourceGroups, IReadOnlySet<string> targetGroups) =>
            !targetGroups.Contains(ClosedGroup);
    }

    [Fact]
    public async Task ATargetThatIsAbsentUnreachableOrOutOfReach_IsRefusedInTheSameWords_AndTheLogSaysWhich()
    {
        var logs = new LogRecorder();
        using var host = TestHubServerFactory.CreateHost(services =>
        {
            services.AddLogging(logging => logging.AddProvider(logs).SetMinimumLevel(LogLevel.Trace));
            services.AddSingleton<IFlowControlService>(new Rules());
        });

        var sourceId = await RegisterAsync(host, [OpenGroup]);
        var targets = new Dictionary<string, Guid>
        {
            ["absent"] = Guid.NewGuid(),
            ["no stream behind the registration"] = await RegisterAsync(host, [OpenGroup]),
            ["not authorized"] = await RegisterAsync(host, [ClosedGroup]),
            ["token expired"] = await RegisterAsync(host, [OpenGroup], DateTime.UtcNow.AddMinutes(-1)),
            ["holds no group"] = await RegisterAsync(host, []),
        };

        var answers = new Dictionary<string, (HttpStatusCode Status, string Body)>();
        foreach (var (reason, targetId) in targets)
        {
            using var response = await RawHub.SendAsync(host, HttpMethod.Post, sourceId, WireRoutes.TransfersSegment, token: null,
                new InitTransferRequest(targetId, 1000)).WaitAsync(TimeSpan.FromSeconds(30));
            answers[reason] = (response.StatusCode, await response.Content.ReadAsStringAsync());
        }

        Assert.All(answers, answer => Assert.Equal(HttpStatusCode.Conflict, answer.Value.Status));
        Assert.Single(answers.Values.Select(answer => answer.Body).Distinct());

        foreach (var (reason, targetId) in targets)
        {
            Assert.True(logs.Has(LogLevel.Warning, targetId.ToString()), $"The log does not name the target that is {reason}.");
        }
    }

    [Theory]
    [InlineData("token expired")]
    [InlineData("holds no group")]
    public async Task ASourceAtFault_IsToldTheSameWhetherItsTargetExistsOrNot(string fault)
    {
        // A source whose request still attests its subject passes admission with a registration
        // that no longer grants it anything; what it is told must say nothing of its target.
        using var host = TestHubServerFactory.CreateHost();
        var sourceId = fault == "token expired"
            ? await RegisterAsync(host, [OpenGroup], DateTime.UtcNow.AddMinutes(-1))
            : await RegisterAsync(host, []);
        var present = await RegisterAsync(host, [OpenGroup]);

        var toPresent = await AnswerAsync(host, sourceId, present);
        var toAbsent = await AnswerAsync(host, sourceId, Guid.NewGuid());

        Assert.Equal(HttpStatusCode.Conflict, toPresent.Status);
        Assert.Equal(toPresent, toAbsent);
    }

    private static async Task<(HttpStatusCode Status, string Body)> AnswerAsync(IHost host, Guid sourceId, Guid targetId)
    {
        using var response = await RawHub.SendAsync(host, HttpMethod.Post, sourceId, WireRoutes.TransfersSegment, token: null,
            new InitTransferRequest(targetId, 1000)).WaitAsync(TimeSpan.FromSeconds(30));
        return (response.StatusCode, await response.Content.ReadAsStringAsync());
    }

    private static async Task<Guid> RegisterAsync(IHost host, string[] groups, DateTime? tokenExpiry = null)
    {
        var clientId = Guid.NewGuid();
        await host.Services.GetRequiredService<IStateStore>().SaveClientAsync(new ClientRegistration(
            clientId, $"conn-{clientId}", groups.ToHashSet(), null, tokenExpiry, DateTime.UtcNow));
        return clientId;
    }
}
