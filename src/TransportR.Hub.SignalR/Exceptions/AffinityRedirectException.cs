using Microsoft.AspNetCore.SignalR;

namespace TransportR.Hub.SignalR.Exceptions;

/// <summary>
/// Exception thrown when sender needs to reconnect to receiver's replica.
/// The client should catch this and reconnect with the provided affinity token.
/// </summary>
public class AffinityRedirectException : HubException
{
    /// <summary>
    /// The affinity token to use for reconnection.
    /// Client should set this as cookie value and reconnect.
    /// </summary>
    public string TargetAffinityToken { get; }
    
    /// <summary>
    /// The name of the affinity cookie (e.g., "affinity", "ARRAffinity").
    /// </summary>
    public string CookieName { get; }

    public AffinityRedirectException(string targetAffinityToken, string cookieName)
        : base($"AFFINITY_REDIRECT:{cookieName}:{targetAffinityToken}")
    {
        TargetAffinityToken = targetAffinityToken;
        CookieName = cookieName;
    }
}
