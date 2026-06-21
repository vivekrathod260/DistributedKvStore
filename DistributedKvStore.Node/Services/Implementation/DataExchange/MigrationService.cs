using System.Net.Http.Json;
using DistributedKvStore.Node.Data;
using DistributedKvStore.Node.Services.Implementation.State;
using DistributedKvStore.Node.Services.Interfaces;
using DistributedKvStore.Shared.DTOs;
using DistributedKvStore.Shared.Enums;
using DistributedKvStore.Shared.Hashing;
using DistributedKvStore.Shared.Models;
using Microsoft.Extensions.Logging;

namespace DistributedKvStore.Node.Services.Implementation.DataExchange;

public class MigrationService : IMigrationService
{
    private readonly INodeStateService _nodeState;
    private readonly IDataRepository _repository;
    private readonly IHttpClientFactory _httpClientFactory;
    private readonly ILogger<MigrationService> _logger;

    public MigrationService(
        INodeStateService nodeState,
        IDataRepository repository,
        IHttpClientFactory httpClientFactory,
        ILogger<MigrationService> logger)
    {
        _nodeState = nodeState;
        _repository = repository;
        _httpClientFactory = httpClientFactory;
        _logger = logger;
    }

    public async Task MigrateToNewNodeAsync(ClusterNodeInfo newNode)
    {
        var currentNode = _nodeState.GetCurrentNode();
        var clusterState = _nodeState.GetClusterState();

        // Find the predecessor of the new node to determine the hash range it now owns
        var sortedNodes = clusterState.Nodes
            .Where(n => n.Status == NodeStatus.Online || n.Status == NodeStatus.Joining)
            .OrderBy(n => n.HashPosition)
            .ToList();

        var newNodeIndex = sortedNodes.FindIndex(n => n.NodeId == newNode.NodeId);
        if (newNodeIndex < 0) return;

        // The new node's range: from predecessor's position (exclusive) to its own position (inclusive)
        var predecessorIndex = (newNodeIndex - 1 + sortedNodes.Count) % sortedNodes.Count;
        var predecessor = sortedNodes[predecessorIndex];

        ulong rangeStart = predecessor.HashPosition + 1;
        ulong rangeEnd = newNode.HashPosition;

        // Only migrate if this node is the successor (next clockwise after the new node)
        var successorIndex = (newNodeIndex + 1) % sortedNodes.Count;
        var successor = sortedNodes[successorIndex];

        if (successor.NodeId != currentNode.NodeId && currentNode.NodeId != newNode.NodeId)
        {
            // This node is not the successor, skip
            return;
        }

        _logger.LogInformation("Migrating keys in range [{Start}, {End}] to new node {NodeId}",
            rangeStart, rangeEnd, newNode.NodeId);

        var records = await _repository.GetRecordsInHashRangeAsync(rangeStart, rangeEnd);
        if (records.Count == 0) return;

        // Send records in chunks
        const int chunkSize = 100;
        for (int i = 0; i < records.Count; i += chunkSize)
        {
            var chunk = records.Skip(i).Take(chunkSize).ToList();
            try
            {
                var client = _httpClientFactory.CreateClient("InternalNode");
                client.BaseAddress = new Uri(newNode.BaseUrl);
                client.Timeout = TimeSpan.FromSeconds(30);

                var migrationResponse = await client.PostAsJsonAsync("/internal/migrate", new MigrationResponse
                {
                    Records = chunk,
                    IsComplete = i + chunkSize >= records.Count
                });

                if (migrationResponse.IsSuccessStatusCode)
                {
                    _logger.LogDebug("Migrated chunk of {Count} records to {NodeId}", chunk.Count, newNode.NodeId);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to migrate chunk to node {NodeId}", newNode.NodeId);
            }
        }

        // After successful migration, remove records from this node
        await _repository.DeleteRecordsInHashRangeAsync(rangeStart, rangeEnd);
        _logger.LogInformation("Migration to node {NodeId} complete. {Count} records transferred.", newNode.NodeId, records.Count);
    }

    public async Task MigrateFromLeavingNodeAsync(Guid leavingNodeId)
    {
        var clusterState = _nodeState.GetClusterState();
        var leavingNode = clusterState.Nodes.FirstOrDefault(n => n.NodeId == leavingNodeId);
        if (leavingNode == null) return;

        var currentNode = _nodeState.GetCurrentNode();

        // Determine successor of leaving node
        var sortedNodes = clusterState.Nodes
            .Where(n => n.Status != NodeStatus.Failed && n.NodeId != leavingNodeId)
            .OrderBy(n => n.HashPosition)
            .ToList();

        if (sortedNodes.Count == 0) return;

        // Find successor - first node clockwise after the leaving node
        var successor = sortedNodes.FirstOrDefault(n => n.HashPosition > leavingNode.HashPosition)
                        ?? sortedNodes[0];

        if (successor.NodeId != currentNode.NodeId) return;

        _logger.LogInformation("Pulling data from leaving node {NodeId}", leavingNodeId);

        try
        {
            var client = _httpClientFactory.CreateClient("InternalNode");
            client.BaseAddress = new Uri(leavingNode.BaseUrl);
            client.Timeout = TimeSpan.FromSeconds(60);

            // Get predecessor of leaving node to determine range
            var allSorted = clusterState.Nodes
                .OrderBy(n => n.HashPosition)
                .ToList();
            var leavingIndex = allSorted.FindIndex(n => n.NodeId == leavingNodeId);
            var predIndex = (leavingIndex - 1 + allSorted.Count) % allSorted.Count;

            ulong rangeStart = allSorted[predIndex].HashPosition + 1;
            ulong rangeEnd = leavingNode.HashPosition;

            var request = new MigrationRequest
            {
                RequestingNodeId = currentNode.NodeId,
                RangeStart = rangeStart,
                RangeEnd = rangeEnd
            };

            var response = await client.PostAsJsonAsync("/internal/migrate-out", request);
            if (response.IsSuccessStatusCode)
            {
                var migrationData = await response.Content.ReadFromJsonAsync<MigrationResponse>();
                if (migrationData?.Records != null)
                {
                    await _repository.BulkInsertRecordsAsync(migrationData.Records);
                    _logger.LogInformation("Received {Count} records from leaving node {NodeId}",
                        migrationData.Records.Count, leavingNodeId);
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to pull data from leaving node {NodeId}", leavingNodeId);
        }
    }
}
