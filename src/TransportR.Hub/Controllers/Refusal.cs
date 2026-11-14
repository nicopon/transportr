using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace TransportR.Hub.Controllers;

internal static class Refusal
{
    /// <summary>
    /// Answers 404 as the hub's own refusal, marked with <see cref="WireRoutes.RefusedHeader"/> so a
    /// client can tell it from a path no route serves. An action that writes its own response returns
    /// right after; one that returns a result returns this one.
    /// </summary>
    public static NotFoundResult Refuse(this ControllerBase controller)
    {
        controller.Response.Headers[WireRoutes.RefusedHeader] = "1";
        controller.Response.StatusCode = StatusCodes.Status404NotFound;
        return controller.NotFound();
    }
}
