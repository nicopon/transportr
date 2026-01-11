namespace Receiver.Configuration;

public class ReceiverOptions
{
    public string HubUrl { get; set; } = "http://localhost:5252/hub/command";
    public string OutputDirectory { get; set; } = "output";
    public bool EnableAuth { get; set; } = false;
    public int BatchSize { get; set; } = 5000;
    public string Group { get; set; } = "receivers";
    public string? AccessToken { get; set; } = null;
    
    /// <summary>
    /// Auth configuration for automatic token acquisition.
    /// </summary>
    public AuthOptions? Auth { get; set; }
}

public class AuthOptions
{
    public string TokenEndpoint { get; set; } = string.Empty;
    public string ClientId { get; set; } = string.Empty;
    public string ClientSecret { get; set; } = string.Empty;
    public string? Scope { get; set; }
    
    public bool IsConfigured => 
        !string.IsNullOrEmpty(TokenEndpoint) && 
        !string.IsNullOrEmpty(ClientId) && 
        !string.IsNullOrEmpty(ClientSecret);
}
