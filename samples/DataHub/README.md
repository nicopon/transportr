# DataHub

Sample TransportR hub server application.

## Purpose

Demonstrates how to host a TransportR hub using ASP.NET Core, providing both the control plane (requests and a stream of server-sent events) and the HTTP streaming data plane endpoints.

## Quick Start

```bash
dotnet run --urls http://localhost:5252
```

## Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /health` | Health check |
| `GET /api/client/{id}/events` | A client's stream of server-sent events: opening it registers the client |
| `/api/client/{id}/…` | The other control requests: discovery, transfers, replies, presence, heartbeats and probe (see "Communication Channels" in `TransportRArchitecture.md`) |
| `POST /api/stream/{id}/send` | Data upload |
| `GET /api/stream/{id}/receive` | Data download |
| `POST /api/stream/{id}/abort` | A participant ends its transfer when its `Abort` cannot be written to the stream |
| `PUT /api/client/{id}/token` | A client renews its token |

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `ASPNETCORE_URLS` | http://localhost:5252 | Server URL |
| `HUB_PORT` | 5252 | Port the hub names itself by (issuer of the embedded IdP) |
| `EnableAuth` | false | JWT authentication; dev mode (default group `dev`) when false |
| `EnableEmbeddedIdp` | false | Hosts SimpleIdp on the hub itself |
| `Oidc__Authority` | http://localhost:5100 | External IdP, when auth is on and the embedded one is off |
| `FLOWCONTROL_MODE` | default | `default` (senders → receivers), `wildcard` (adds `test` → `*`), `none` (no rules) |
| `USE_REDIS` | — | Redis connection string: Redis store and affinity handoff |
| `USE_TEST_PROXY` | — | TestProxy URL: its state store and affinity handoff (ignored when `USE_REDIS` is set) |
| `OUTPUT_DIR` | output | Where `hub.log` is written |
| `Logging__LogLevel__Default` | Information | Log level |

## Structure

```
DataHub/
├── Program.cs                    # Application entry point
├── Adapters/
│   └── ProgressMonitorAdapter.cs # IHubProgressMonitor implementation
├── Middleware/
│   └── TransferIdMiddleware.cs   # Reads the X-Transfer-Id header
└── Services/
    ├── DataProxyMetrics.cs       # Metrics of the data plane
    └── ProgressMonitorService.cs # Progress tracking service
```
