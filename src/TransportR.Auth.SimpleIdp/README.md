# TransportR.Auth.SimpleIdp

Embeddable Identity Provider for TransportR authentication testing and development.

## Purpose

Provides a lightweight OpenIddict-based IDP that can be:
1. **Embedded** in an ASP.NET Core application for testing
2. **Standalone** for local development

## Usage

### Embedded in Tests or Demos

```csharp
var builder = WebApplication.CreateBuilder(args);

// Add SimpleIdp with preconfigured clients
builder.AddSimpleIdp(options =>
{
    options.Port = 5100;
    options.AddClient("sender", "sender-secret", "transportr:group:senders");
    options.AddClient("receiver", "receiver-secret", "transportr:group:receivers");
});

var app = builder.Build();
app.UseSimpleIdp();
app.Run();
```

### Pre-configured Clients

| ClientId | Secret | Scope |
|----------|--------|-------|
| `sender` | `sender-secret` | `transportr:group:senders` |
| `receiver` | `receiver-secret` | `transportr:group:receivers` |
| `test-client` | `secret-test` | `transportr:group:test` |

## Endpoints

| Endpoint | Purpose |
|----------|---------|
| `/connect/token` | Token endpoint (client_credentials) |
| `/health` | Health check |
| `/` | Service info |

## Token Acquisition

```bash
curl -X POST http://localhost:5100/connect/token \
  -d "grant_type=client_credentials" \
  -d "client_id=sender" \
  -d "client_secret=sender-secret" \
  -d "scope=transportr:group:senders"
```

## Configuration

| Option | Default | Description |
|--------|---------|-------------|
| `Port` | 5100 | Listening port |
| `TokenLifetime` | 1 hour | JWT expiration |
| `IssuerName` | auto | Token issuer |

## Dependencies

- `OpenIddict` - OAuth 2.0 / OpenID Connect implementation
- `Microsoft.EntityFrameworkCore.InMemory` - In-memory persistence
