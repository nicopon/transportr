# DataHub

Sample TransportR hub server application.

## Purpose

Demonstrates how to host a TransportR hub using ASP.NET Core, providing both SignalR control plane and HTTP streaming data plane endpoints.

## Quick Start

```bash
dotnet run --urls http://localhost:5252
```

## Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /health` | Health check |
| `WS /hub/command` | SignalR command hub |
| `POST /api/stream/{id}/send` | Data upload |
| `GET /api/stream/{id}/receive` | Data download |

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `ASPNETCORE_URLS` | http://localhost:5252 | Server URL |
| `Logging__LogLevel__Default` | Warning | Log level |

## Structure

```
DataHub/
├── Program.cs                    # Application entry point
├── Adapters/
│   └── ProgressMonitorAdapter.cs # IHubProgressMonitor implementation
└── Services/
    └── ProgressMonitorService.cs # Progress tracking service
```
