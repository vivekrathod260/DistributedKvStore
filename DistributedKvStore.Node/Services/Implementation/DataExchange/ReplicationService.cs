using System.Net.Http.Json;
using DistributedKvStore.Node.Data;
using DistributedKvStore.Node.Services.Implementation.State;
using DistributedKvStore.Node.Services.Interfaces;
using DistributedKvStore.Shared.DTOs;
using DistributedKvStore.Shared.Enums;
using DistributedKvStore.Shared.Models;
using Microsoft.Extensions.Logging;

namespace DistributedKvStore.Node.Services.Implementation.DataExchange;

public class ReplicationService : IReplicationService
{
    private readonly IHttpClientFactory _httpClientFactory;
    private readonly INodeStateService _nodeState;
    private readonly IDataRepository _repository;
    private readonly ILogger<ReplicationService> _logger;
    private const int MaxRetries = 3;
    private static readonly TimeSpan RetryDelay = TimeSpan.FromMilliseconds(500);

    public ReplicationService(
        IHttpClientFactory httpClientFactory,
        INodeStateService nodeState,
        IDataRepository repository,
        ILogger<ReplicationService> logger)
    {
        _httpClientFactory = httpClientFactory;
        _nodeState = nodeState;
        _repository = repository;
        _logger = logger;
    }

    public async Task ReplicateAsync(string key, string? value, OperationType opType, DateTime timestamp, ulong hash)
    {
        try
        {
            var replicationFactor = _nodeState.GetReplicationFactor();
            var hashRing = _nodeState.GetHashRing();
            var replicas = hashRing.FindReplicaNodes(key, replicationFactor);
            var currentNode = _nodeState.GetCurrentNode();

            var targetReplicas = replicas
                .Where(r => r.NodeId != currentNode.NodeId)
                .ToList();

            if(targetReplicas.Count == 0)
            {
                _logger.LogDebug("Replication skipped, No other replicas found for key {Key}.", key);
                return;
            }

            var request = new ReplicationRequest
            {
                Key = key,
                Value = value,
                OperationType = opType,
                TimestampUtc = timestamp,
                Hash = hash
            };

            await ReplicateToNodesAsync(request, targetReplicas);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Replication failed for key {Key}.", key);
        }
    }

    public async Task ApplyReplicationAsync(ReplicationRequest request)
    {
        if (request.OperationType == OperationType.Delete)
        {
            await _repository.DeleteAsync(request.Key, request.TimestampUtc);
        }
        else
        {
            var record = new KeyValueRecord
            {
                Key = request.Key,
                Value = request.Value ?? string.Empty,
                Hash = request.Hash,
                LastUpdatedUtc = request.TimestampUtc,
                IsDeleted = false
            };

            await _repository.PutAsync(record);
        }
    }

    private async Task ReplicateToNodesAsync(ReplicationRequest request, List<ClusterNodeInfo> targetNodes)
    {
        var tasks = targetNodes.Select(node => ReplicateToNodeAsync(request, node));
        await Task.WhenAll(tasks);
    }

    private async Task ReplicateToNodeAsync(ReplicationRequest request, ClusterNodeInfo targetNode)
    {
        for (int attempt = 0; attempt < MaxRetries; attempt++)
        {
            try
            {
                var client = _httpClientFactory.CreateClient("InternalNode");
                client.BaseAddress = new Uri(targetNode.BaseUrl);
                client.Timeout = TimeSpan.FromSeconds(5);

                var response = await client.PostAsJsonAsync("/internal/replication", request);
                if (response.IsSuccessStatusCode)
                {
                    _logger.LogDebug("Replicated key {Key} to node {NodeId}", request.Key, targetNode.NodeId);
                    return;
                }

                _logger.LogWarning("Replication to {NodeId} returned {Status}", targetNode.NodeId, response.StatusCode);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Replication attempt {Attempt} to node {NodeId} failed",
                    attempt + 1, targetNode.NodeId);
            }

            if (attempt < MaxRetries - 1)
                await Task.Delay(RetryDelay);
        }

        _logger.LogWarning("Replication to node {NodeId} for key {Key} exhausted retries.", targetNode.NodeId, request.Key);
    }
}
