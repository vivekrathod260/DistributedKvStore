using System.Net.Http.Json;
using DistributedKvStore.Node.Data;
using DistributedKvStore.Node.Services.Implementation.State;
using DistributedKvStore.Shared.DTOs;
using DistributedKvStore.Shared.Enums;
using DistributedKvStore.Shared.Models;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace DistributedKvStore.Node.BackgroundServices;

public class RecoverySyncService : BackgroundService
{
    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger<RecoverySyncService> _logger;
    private static readonly TimeSpan SyncInterval = TimeSpan.FromSeconds(30);

    public RecoverySyncService(IServiceProvider serviceProvider, ILogger<RecoverySyncService> logger)
    {
        _serviceProvider = serviceProvider;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await Task.Delay(TimeSpan.FromSeconds(15), stoppingToken);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await PerformRecoverySyncAsync(stoppingToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Recovery sync failed");
            }

            await Task.Delay(SyncInterval, stoppingToken);
        }
    }

    private async Task PerformRecoverySyncAsync(CancellationToken cancellationToken)
    {
        using var scope = _serviceProvider.CreateScope();
        var nodeState = scope.ServiceProvider.GetRequiredService<INodeStateService>();
        var repository = scope.ServiceProvider.GetRequiredService<IDataRepository>();
        var httpClientFactory = scope.ServiceProvider.GetRequiredService<IHttpClientFactory>();

        if (!nodeState.IsInitialized) return;

        var currentNode = nodeState.GetCurrentNode();
        var clusterState = nodeState.GetClusterState();
        var lastOperationId = await repository.GetLastOperationIdAsync();

        // Get peers that are responsible for the same key ranges
        var peers = clusterState.Nodes
            .Where(n => n.NodeId != currentNode.NodeId && n.Status == NodeStatus.Online)
            .ToList();

        if (peers.Count == 0) return;

        // Randomly pick a peer to sync with
        var syncPeer = peers[Random.Shared.Next(peers.Count)];

        try
        {
            var client = httpClientFactory.CreateClient("InternalNode");
            client.BaseAddress = new Uri(syncPeer.BaseUrl);
            client.Timeout = TimeSpan.FromSeconds(30);

            var response = await client.GetFromJsonAsync<SyncResponse>(
                $"/internal/operations?after={lastOperationId}", cancellationToken);

            if (response?.Operations != null && response.Operations.Count > 0)
            {
                _logger.LogInformation("Syncing {Count} operations from peer {NodeId}",
                    response.Operations.Count, syncPeer.NodeId);

                foreach (var operation in response.Operations.OrderBy(o => o.OperationId))
                {
                    await ApplyOperationToDataAsync(repository, nodeState, operation);
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Sync with peer {BaseUrl} failed", syncPeer.BaseUrl);
        }
    }

    private async Task ApplyOperationToDataAsync(IDataRepository repository, INodeStateService nodeState, OperationLog operation)
    {
        var hashRing = nodeState.GetHashRing();
        var hash = hashRing.ComputeHash(operation.Key);

        // Check if this node is responsible for this key
        var replicationFactor = nodeState.GetReplicationFactor();
        var responsibleNodes = hashRing.GetResponsibleNodes(operation.Key, replicationFactor);
        var currentNode = nodeState.GetCurrentNode();

        if (!responsibleNodes.Any(n => n.NodeId == currentNode.NodeId))
            return; // Not responsible for this key

        if (operation.OperationType == OperationType.Delete)
        {
            await repository.DeleteAsync(operation.Key, operation.TimestampUtc);
        }
        else
        {
            var record = new KeyValueRecord
            {
                Key = operation.Key,
                Value = operation.Value ?? string.Empty,
                Hash = hash,
                LastUpdatedUtc = operation.TimestampUtc,
                IsDeleted = false
            };
            await repository.PutAsync(record);
        }

        await repository.ApplyOperationAsync(new OperationLog
        {
            Key = operation.Key,
            Value = operation.Value,
            OperationType = operation.OperationType,
            TimestampUtc = operation.TimestampUtc
        });
    }
}
