using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using TransportR.Client.Auth;

namespace TransportR.Client;

/// <summary>
/// Extension methods for configuring OAuth2 client credentials authentication on TransportRClientBuilder.
/// </summary>
public static class TransportRClientBuilderAuthExtensions
{
    /// <summary>
    /// Configures the client to acquire tokens using OAuth2 client credentials flow.
    /// </summary>
    /// <param name="httpClient">
    /// What the token requests go through, for an identity provider not reached the way the hub is
    /// (another proxy, certificate or authority). The caller owns it: the client neither configures nor
    /// disposes it. When null, they go through the client's own handler, so what
    /// <c>ConfigureHttpHandler</c> sets for the hub applies to them too.
    /// </param>
    public static TransportRClientBuilder<T> WithClientCredentials<T>(
        this TransportRClientBuilder<T> builder,
        string tokenEndpoint,
        string clientId,
        string clientSecret,
        string? scope = null,
        HttpClient? httpClient = null)
    {
        var options = OptionsOf(tokenEndpoint, clientId, clientSecret, scope);
        return builder.WithTokenProviderFactory((handler, loggerFactory) =>
            CreateProvider(options, loggerFactory, httpClient ?? OnTheHubsHandler(handler)));
    }

    /// <inheritdoc cref="WithClientCredentials{T}(TransportRClientBuilder{T}, string, string, string, string?, HttpClient?)"/>
    public static ByteStreamClientBuilder WithClientCredentials(
        this ByteStreamClientBuilder builder,
        string tokenEndpoint,
        string clientId,
        string clientSecret,
        string? scope = null,
        HttpClient? httpClient = null)
    {
        var options = OptionsOf(tokenEndpoint, clientId, clientSecret, scope);
        return builder.WithTokenProviderFactory((handler, loggerFactory) =>
            CreateProvider(options, loggerFactory, httpClient ?? OnTheHubsHandler(handler)));
    }

    private static ClientCredentialsOptions OptionsOf(string tokenEndpoint, string clientId, string clientSecret, string? scope)
    {
        var options = new ClientCredentialsOptions
        {
            TokenEndpoint = tokenEndpoint,
            ClientId = clientId,
            ClientSecret = clientSecret,
            Scope = scope
        };

        // Refused at the call, not at Build where the provider of the hub's handler is made.
        ClientCredentialsTokenProvider.EnsureConfigured(options);
        return options;
    }

    // Its own client, so that nothing the hub's client carries reaches the identity provider, and its
    // own timeout, since the hub's has none. The hub's client owns the handler.
    private static HttpClient OnTheHubsHandler(HttpMessageHandler handler) => new(handler, disposeHandler: false);

    private static Func<Task<string>> CreateProvider(
        ClientCredentialsOptions options, ILoggerFactory? loggerFactory, HttpClient httpClient)
    {
        var provider = new ClientCredentialsTokenProvider(
            options,
            loggerFactory?.CreateLogger<ClientCredentialsTokenProvider>() ?? NullLogger<ClientCredentialsTokenProvider>.Instance,
            httpClient);

        return provider.AsTokenProvider();
    }
}
