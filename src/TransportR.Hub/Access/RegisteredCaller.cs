using System.Security.Claims;

namespace TransportR.Hub.Access;

/// <summary>
/// Whether a request comes from the client a registration records: the admission of every route that
/// acts for a registered client, on the control plane and for the write-transport probe.
/// </summary>
internal static class RegisteredCaller
{
    /// <summary>
    /// The registration held by the stream of events <paramref name="connectionId"/>, when the request
    /// comes from its client; null otherwise. A route that also names the client in its path passes that
    /// id as <paramref name="named"/>, compared first: a request naming another client goes no further.
    /// </summary>
    public static async Task<ClientRegistration?> OfStreamAsync(
        IStateStore stateStore,
        IClientIdentityProvider identities,
        ClaimsPrincipal? user,
        string connectionId,
        Guid? named = null)
    {
        var client = await stateStore.GetClientByConnectionIdAsync(connectionId);
        return client != null && (named == null || client.ClientId == named) && Attests(identities, user, client)
            ? client
            : null;
    }

    /// <summary>
    /// Whether the request's <paramref name="user"/> is the client of <paramref name="client"/>: the identity
    /// its token attests has the registration's client id and subject.
    /// </summary>
    /// <remarks>
    /// Dev mode arms no authorization, and an unauthenticated caller attests nothing to compare. Past that,
    /// an authenticated caller must have been attested, as on the data plane.
    /// </remarks>
    public static bool Attests(IClientIdentityProvider identities, ClaimsPrincipal? user, ClientRegistration client)
    {
        if (user?.Identity?.IsAuthenticated != true) return true;

        var caller = identities.ResolveIdentity(user, client.ClientId, providedGroups: null);
        return caller?.Subject != null
            && caller.ClientId == client.ClientId
            && AccessPolicy.SameIdentity(client.Subject, caller.Subject) == AccessVerdict.Allowed;
    }
}
