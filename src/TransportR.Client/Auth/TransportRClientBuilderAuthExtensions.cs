using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using TransportR.Client;

namespace TransportR.Auth.ClientCredentials;

/// <summary>
/// Extension methods for configuring OAuth2 client credentials authentication on TransportRClientBuilder.
/// </summary>
public static class TransportRClientBuilderAuthExtensions
{
    /// <summary>
    /// Configures the client to acquire tokens using OAuth2 client credentials flow.
    /// </summary>
    public static TransportRClientBuilder<T> WithClientCredentials<T>(
        this TransportRClientBuilder<T> builder, 
        string tokenEndpoint, 
        string clientId, 
        string clientSecret, 
        string? scope,
        ILoggerFactory? loggerFactory = null)
    {
        var options = new ClientCredentialsOptions
        {
            TokenEndpoint = tokenEndpoint,
            ClientId = clientId,
            ClientSecret = clientSecret,
            Scope = scope
        };
        
        var provider = new ClientCredentialsTokenProvider(
            options, 
            loggerFactory?.CreateLogger<ClientCredentialsTokenProvider>() ?? NullLogger<ClientCredentialsTokenProvider>.Instance);
        
        return builder.WithTokenProvider(provider.AsTokenProvider());
    }
}
