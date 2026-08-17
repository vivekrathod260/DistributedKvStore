FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build
WORKDIR /src

COPY DistributedKvStore.Node/DistributedKvStore.Node.csproj DistributedKvStore.Node/
COPY DistributedKvStore.Shared/DistributedKvStore.Shared.csproj DistributedKvStore.Shared/
RUN dotnet restore DistributedKvStore.Node/DistributedKvStore.Node.csproj

COPY DistributedKvStore.Node/ DistributedKvStore.Node/
COPY DistributedKvStore.Shared/ DistributedKvStore.Shared/
RUN dotnet publish DistributedKvStore.Node/DistributedKvStore.Node.csproj -c Release -o /app/publish --no-restore

FROM mcr.microsoft.com/dotnet/aspnet:9.0 AS final
WORKDIR /app
COPY --from=build /app/publish .

ENV ASPNETCORE_URLS=http://+:8080
EXPOSE 8080

ENTRYPOINT ["dotnet", "DistributedKvStore.Node.dll"]
