using System.Net.Http.Json;
using TransportR.Interfaces;
using TransportR.Models;

namespace TransportR.Hub.SignalR.Services;

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
        await _httpClient.PostAsJsonAsync("/api/state/clients", client);
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
        await _httpClient.PostAsJsonAsync($"/api/state/connections/{connectionId}/disconnected", whenUtc);
    }

    public async Task DeleteClientAsync(Guid clientId)
    {
        await _httpClient.DeleteAsync($"/api/state/clients/{clientId}");
    }

    public async Task UpdateClientTokenExpiryAsync(Guid clientId, DateTime expiry)
    {
        await _httpClient.PutAsJsonAsync($"/api/state/clients/{clientId}/expiry", expiry);
    }

    // ========== Transfers (HA Sessions) ==========

    public async Task SaveSessionAsync(TransferSession session)
    {
        await _httpClient.PostAsJsonAsync("/api/state/sessions", session);
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
        await _httpClient.PutAsJsonAsync($"/api/state/sessions/{transferId}/expiry", newExpiry);
    }

    public async Task DeleteSessionAsync(string transferId)
    {
        await _httpClient.DeleteAsync($"/api/state/sessions/{transferId}");
    }
}
