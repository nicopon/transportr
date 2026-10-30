#!/bin/bash
set -e

echo "Building solution in Release mode..."

# Core libraries
dotnet build src/TransportR/TransportR.csproj -c Release
dotnet build src/TransportR.Serialization.MessagePack/TransportR.Serialization.MessagePack.csproj -c Release
dotnet build src/TransportR.Serialization.Protobuf/TransportR.Serialization.Protobuf.csproj -c Release
dotnet build src/TransportR.Serialization.Json/TransportR.Serialization.Json.csproj -c Release
dotnet build src/TransportR.Serialization.Arrow/TransportR.Serialization.Arrow.csproj -c Release
dotnet build src/TransportR.Hub/TransportR.Hub.csproj -c Release
dotnet build src/TransportR.Client/TransportR.Client.csproj -c Release
dotnet build src/TransportR.Storage.Redis/TransportR.Storage.Redis.csproj -c Release

# Sample projects
dotnet build samples/Shared/Shared.csproj -c Release
dotnet build samples/DatabaseInitializer/DatabaseInitializer.csproj -c Release
dotnet build samples/Sender/Sender.csproj -c Release
dotnet build samples/Receiver/Receiver.csproj -c Release
dotnet build samples/DataHub/DataHub.csproj -c Release

# Tests
dotnet build tests/TransportR.Tests/TransportR.Tests.csproj -c Release
dotnet build tests/TransportR.TestProxy/TransportR.TestProxy.csproj -c Release

echo "Build complete."
