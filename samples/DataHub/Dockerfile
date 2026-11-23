FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

COPY Directory.Build.props ./
COPY src/TransportR/TransportR.csproj src/TransportR/
COPY src/TransportR.Hub/TransportR.Hub.csproj src/TransportR.Hub/
COPY src/TransportR.Auth.SimpleIdp/TransportR.Auth.SimpleIdp.csproj src/TransportR.Auth.SimpleIdp/
COPY src/TransportR.Storage.Redis/TransportR.Storage.Redis.csproj src/TransportR.Storage.Redis/
COPY samples/Shared/Shared.csproj samples/Shared/
COPY samples/DataHub/DataHub.csproj samples/DataHub/

RUN dotnet restore samples/DataHub/DataHub.csproj

COPY src/TransportR/ src/TransportR/
COPY src/TransportR.Hub/ src/TransportR.Hub/
COPY src/TransportR.Auth.SimpleIdp/ src/TransportR.Auth.SimpleIdp/
COPY src/TransportR.Storage.Redis/ src/TransportR.Storage.Redis/
COPY samples/Shared/ samples/Shared/
COPY samples/DataHub/ samples/DataHub/

RUN dotnet publish samples/DataHub/DataHub.csproj -c Release -o /app/publish --no-restore

FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS runtime
WORKDIR /app

COPY --from=build /app/publish ./

RUN mkdir -p /app/output && chown $APP_UID:$APP_UID /app/output

USER $APP_UID
ENV ASPNETCORE_URLS=http://+:8080
EXPOSE 8080

ENTRYPOINT ["dotnet", "DataHub.dll"]
