using System.Net.Http.Json;
using TransportR.Hub;

namespace DataHub.Services;

/// <summary>
/// State store that connects to TestProxy's state API.
/// For multi-instance testing via YARP.
/// </summary>
public class TestProxyStateStore : IStateStore
{
    private readonly HttpClient _httpClient;

    public TestProxyStateStore(string proxyUrl)
    {
        _httpClient = new HttpClient { BaseAddress = new Uri(proxyUrl) };
    }

    public async Task SaveClientAsync(ClientRegistration client)
    {
        var response = await _httpClient.PostAsJsonAsync("/api/state/clients", client);
        response.EnsureSuccessStatusCode();
    }

    public async Task<ClientRegistration?> GetClientAsync(Guid clientId)
    {
        try
        {
            return await _httpClient.GetFromJsonAsync<ClientRegistration>($"/api/state/clients/{clientId}");
        }
        catch (HttpRequestException ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            return null;
        }
    }

    public async Task<ClientRegistration?> GetClientByConnectionIdAsync(string connectionId)
    {
        // Fallback: TestProxy stores use GetAllClients since the proxy API may not have a connection index endpoint
        var allClients = await GetAllClientsAsync();
        return allClients.FirstOrDefault(c => c.ConnectionId == connectionId);
    }

    public async Task<IReadOnlyList<ClientRegistration>> GetAllClientsAsync()
    {
        var clients = await _httpClient.GetFromJsonAsync<List<ClientRegistration>>("/api/state/clients");
        return clients ?? [];
    }

    public async Task MarkDisconnectedAsync(string connectionId, DateTime whenUtc)
    {
        var response = await _httpClient.PostAsJsonAsync($"/api/state/connections/{connectionId}/disconnected", whenUtc);
        response.EnsureSuccessStatusCode();
    }

    public async Task DeleteClientAsync(Guid clientId)
    {
        var response = await _httpClient.DeleteAsync($"/api/state/clients/{clientId}");
        response.EnsureSuccessStatusCode();
    }

    public async Task<ClientRegistration?> RenewClientAsync(
        Guid clientId, string? expectedSubject, IReadOnlySet<string> groups, DateTime? tokenExpiry)
    {
        var response = await _httpClient.PostAsJsonAsync(
            $"/api/state/clients/{clientId}/renewal",
            new { ExpectedSubject = expectedSubject, Groups = groups, TokenExpiry = tokenExpiry });

        if (response.StatusCode == System.Net.HttpStatusCode.NotFound) return null;

        response.EnsureSuccessStatusCode();
        return await response.Content.ReadFromJsonAsync<ClientRegistration>();
    }

    // ========== Transfers (HA Sessions) ==========

    public async Task SaveSessionAsync(TransferSession session)
    {
        var response = await _httpClient.PostAsJsonAsync("/api/state/sessions", session);
        response.EnsureSuccessStatusCode();
    }

    public async Task<TransferSession?> GetSessionAsync(string transferId)
    {
        try
        {
            return await _httpClient.GetFromJsonAsync<TransferSession>($"/api/state/sessions/{transferId}");
        }
        catch (HttpRequestException ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            return null;
        }
    }

    public async Task ExtendSessionExpiryAsync(string transferId, DateTime newExpiry)
    {
        var response = await _httpClient.PutAsJsonAsync($"/api/state/sessions/{transferId}/expiry", newExpiry);
        response.EnsureSuccessStatusCode();
    }

    public async Task DeleteSessionAsync(string transferId)
    {
        var response = await _httpClient.DeleteAsync($"/api/state/sessions/{transferId}");
        response.EnsureSuccessStatusCode();
    }
}
