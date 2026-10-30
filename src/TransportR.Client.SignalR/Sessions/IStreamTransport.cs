
namespace TransportR.Client.SignalR.Sessions;

internal interface IStreamTransport
{
    HttpClient HttpClient { get; }
    Task<string?> GetAccessTokenAsync(bool forceRefresh = false);
}
