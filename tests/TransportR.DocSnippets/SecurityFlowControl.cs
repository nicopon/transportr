using Microsoft.Extensions.DependencyInjection;
using TransportR.FlowControl;
using TransportR.Hub;

namespace TransportR.DocSnippets;

/// <summary>
/// The flow control snippet of <c>SecurityArchitecture.md</c> ("Group Access Rules"), compiled.
/// Edit it and the document together, as <see cref="ReadmeQuickStart"/>.
/// </summary>
internal static class SecurityFlowControl
{
    public static void Register(IServiceCollection services)
    {
        var builder = new { Services = services };

        builder.Services.AddSingleton(new FlowControlOptions
        {
            Groups = new()
            {
                ["senders"] = new GroupAccess { CanSendTo = ["receivers"] },
                ["receivers"] = new GroupAccess { CanSendTo = [] },
            }
        });
        builder.Services.AddSingleton<IFlowControlService, DefaultFlowControlService>();
    }
}
