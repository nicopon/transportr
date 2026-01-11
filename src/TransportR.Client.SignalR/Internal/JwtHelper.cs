using System.Text.RegularExpressions;

namespace TransportR.Client.SignalR.Internal;

/// <summary>
/// Utility class for parsing JWT tokens without external dependencies.
/// </summary>
public static partial class JwtHelper
{
    /// <summary>
    /// Extracts the expiration time from a JWT token.
    /// </summary>
    /// <param name="token">The JWT token string.</param>
    /// <returns>The expiration DateTime in UTC, or null if parsing fails.</returns>
    public static DateTime? ExtractExpiry(string? token)
    {
        if (string.IsNullOrEmpty(token)) return null;
        
        try
        {
            // JWT format: header.payload.signature
            var parts = token.Split('.');
            if (parts.Length != 3) return null;
            
            // Decode payload (base64url)
            var payload = parts[1];
            // Pad base64 if needed
            switch (payload.Length % 4)
            {
                case 2: payload += "=="; break;
                case 3: payload += "="; break;
            }
            payload = payload.Replace('-', '+').Replace('_', '/');
            
            var json = System.Text.Encoding.UTF8.GetString(Convert.FromBase64String(payload));
            
            // Extract exp claim using regex
            var match = ExpClaimRegex().Match(json);
            if (match.Success && long.TryParse(match.Groups[1].Value, out var exp))
            {
                return DateTimeOffset.FromUnixTimeSeconds(exp).UtcDateTime;
            }
        }
        catch
        {
            // Ignore parsing errors - token may be malformed
        }
        
        return null;
    }

    /// <summary>
    /// Extracts a specific claim value from a JWT token.
    /// </summary>
    /// <param name="token">The JWT token string.</param>
    /// <param name="claimName">The name of the claim to extract.</param>
    /// <returns>The claim value as string, or null if not found.</returns>
    public static string? ExtractClaim(string? token, string claimName)
    {
        if (string.IsNullOrEmpty(token) || string.IsNullOrEmpty(claimName)) return null;
        
        try
        {
            var parts = token.Split('.');
            if (parts.Length != 3) return null;
            
            var payload = parts[1];
            switch (payload.Length % 4)
            {
                case 2: payload += "=="; break;
                case 3: payload += "="; break;
            }
            payload = payload.Replace('-', '+').Replace('_', '/');
            
            var json = System.Text.Encoding.UTF8.GetString(Convert.FromBase64String(payload));
            
            // Match "claimName": "value" or "claimName": value (for numbers)
            var pattern = $"\"{Regex.Escape(claimName)}\"\\s*:\\s*\"?([^,\"}}]+)\"?";
            var match = Regex.Match(json, pattern);
            if (match.Success)
            {
                return match.Groups[1].Value;
            }
        }
        catch
        {
            // Ignore parsing errors
        }
        
        return null;
    }

    [GeneratedRegex(@"""exp""\s*:\s*(\d+)")]
    private static partial Regex ExpClaimRegex();
}
