
namespace TransportR.Client.SignalR.Sessions;

public interface IStreamTransport
{
    HttpClient HttpClient { get; }
    Task<string?> GetAccessTokenAsync(bool forceRefresh = false);
}
