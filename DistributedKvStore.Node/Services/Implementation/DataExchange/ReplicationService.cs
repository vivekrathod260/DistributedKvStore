using System.Net.Http.Json;
using DistributedKvStore.Node.Services.Interfaces;
using DistributedKvStore.Shared.DTOs;
using DistributedKvStore.Shared.Models;
using Microsoft.Extensions.Logging;

namespace DistributedKvStore.Node.Services.Implementation.DataExchange;

public class ReplicationService : IReplicationService
{
    private readonly IHttpClientFactory _httpClientFactory;
    private readonly ILogger<ReplicationService> _logger;
    private const int MaxRetries = 3;
    private static readonly TimeSpan RetryDelay = TimeSpan.FromMilliseconds(500);

    public ReplicationService(IHttpClientFactory httpClientFactory, ILogger<ReplicationService> logger)
    {
        _httpClientFactory = httpClientFactory;
        _logger = logger;
    }

    public async Task ReplicateToNodesAsync(ReplicationRequest request, List<ClusterNodeInfo> targetNodes)
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

        _logger.LogWarning("Replication to node {NodeId} for key {Key} exhausted retries. Recovery sync will handle it.",
            targetNode.NodeId, request.Key);
    }
}
