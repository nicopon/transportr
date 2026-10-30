using System.Reflection;
using Microsoft.AspNetCore.Mvc.Controllers;

namespace TransportR.Hub.Controllers;

/// <summary>
/// MVC discovers public controllers only, and the hub's two are internal. This admits exactly
/// those two, so no other internal type of the assembly can become an endpoint.
/// </summary>
internal sealed class DataHubControllerFeatureProvider : ControllerFeatureProvider
{
    protected override bool IsController(TypeInfo typeInfo) =>
        typeInfo.AsType() == typeof(StreamController) || typeInfo.AsType() == typeof(ClientController);
}
