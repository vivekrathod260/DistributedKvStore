using System.Net.Http.Json;
using DistributedKvStore.Shared.DTOs;
using DistributedKvStore.Shared.Enums;
using DistributedKvStore.Shared.Hashing;
using DistributedKvStore.Shared.Models;
using Microsoft.Extensions.Logging;

namespace DistributedKvStore.Node.Services;

public class ClusterManagementService : IClusterManagementService
{
    private readonly INodeStateService _nodeState;
    private readonly IGossipService _gossipService;
    private readonly IMigrationService _migrationService;
    private readonly IHttpClientFactory _httpClientFactory;
    private readonly ILogger<ClusterManagementService> _logger;

    public ClusterManagementService(
        INodeStateService nodeState,
        IGossipService gossipService,
        IMigrationService migrationService,
        IHttpClientFactory httpClientFactory,
        ILogger<ClusterManagementService> logger)
    {
        _nodeState = nodeState;
        _gossipService = gossipService;
        _migrationService = migrationService;
        _httpClientFactory = httpClientFactory;
        _logger = logger;
    }

    public Task<ClusterState> StartClusterAsync()
    {
        _nodeState.MarkInitialized();
        _logger.LogInformation("Cluster started with node {NodeId}", _nodeState.GetCurrentNode().NodeId);
        return Task.FromResult(_nodeState.GetClusterState());
    }

    public async Task<ClusterState> AddNodeAsync(string baseUrl)
    {
        var hashRing = new ConsistentHashRing();
        var hashPosition = hashRing.ComputeHash(baseUrl);

        var newNode = new ClusterNodeInfo
        {
            NodeId = Guid.NewGuid(),
            BaseUrl = baseUrl,
            HashPosition = hashPosition,
            Status = NodeStatus.Joining
        };

        _nodeState.AddNode(newNode);
        var version = _nodeState.IncrementVersion();

        _logger.LogInformation("Adding node {NodeId} at position {Position}", newNode.NodeId, hashPosition);

        // Notify the new node about the cluster state
        try
        {
            var client = _httpClientFactory.CreateClient("InternalNode");
            client.BaseAddress = new Uri(baseUrl);
            client.Timeout = TimeSpan.FromSeconds(10);

            var state = _nodeState.GetClusterState();
            await client.PostAsJsonAsync("/internal/sync", new SyncRequest
            {
                NodeId = newNode.NodeId,
                LastOperationId = 0
            });
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Could not notify new node {BaseUrl} about cluster state", baseUrl);
        }

        // Trigger migration to new node
        await _migrationService.MigrateToNewNodeAsync(newNode);

        // Mark node online
        _nodeState.UpdateNodeStatus(newNode.NodeId, NodeStatus.Online);
        _nodeState.IncrementVersion();

        // Broadcast gossip
        await _gossipService.BroadcastNodeStatusChangeAsync(new List<NodeStatusChange>
        {
            new()
            {
                NodeId = newNode.NodeId,
                NewStatus = NodeStatus.Online,
                BaseUrl = newNode.BaseUrl,
                HashPosition = newNode.HashPosition
            }
        });

        return _nodeState.GetClusterState();
    }

    public async Task<ClusterState> RemoveNodeAsync(Guid nodeId)
    {
        _nodeState.UpdateNodeStatus(nodeId, NodeStatus.Leaving);
        _nodeState.IncrementVersion();

        _logger.LogInformation("Removing node {NodeId}", nodeId);

        // Migrate data from leaving node
        await _migrationService.MigrateFromLeavingNodeAsync(nodeId);

        // Remove from cluster
        _nodeState.RemoveNode(nodeId);
        _nodeState.IncrementVersion();

        // Gossip removal
        await _gossipService.BroadcastNodeStatusChangeAsync(new List<NodeStatusChange>
        {
            new()
            {
                NodeId = nodeId,
                NewStatus = NodeStatus.Failed
            }
        });

        return _nodeState.GetClusterState();
    }

    public async Task<ClusterState> RestartNodeAsync(Guid nodeId)
    {
        var clusterState = _nodeState.GetClusterState();
        var node = clusterState.Nodes.FirstOrDefault(n => n.NodeId == nodeId);

        if (node == null)
        {
            _logger.LogWarning("Node {NodeId} not found in cluster", nodeId);
            return clusterState;
        }

        _nodeState.UpdateNodeStatus(nodeId, NodeStatus.Online);
        _nodeState.IncrementVersion();

        await _gossipService.BroadcastNodeStatusChangeAsync(new List<NodeStatusChange>
        {
            new()
            {
                NodeId = nodeId,
                NewStatus = NodeStatus.Online,
                BaseUrl = node.BaseUrl,
                HashPosition = node.HashPosition
            }
        });

        return _nodeState.GetClusterState();
    }

    public Task SetReplicationFactorAsync(int factor)
    {
        _nodeState.SetReplicationFactor(factor);
        _nodeState.IncrementVersion();
        _logger.LogInformation("Replication factor set to {Factor}", factor);
        return Task.CompletedTask;
    }

    public async Task ShutdownClusterAsync()
    {
        _logger.LogInformation("Cluster shutdown initiated");

        var clusterState = _nodeState.GetClusterState();
        var currentNode = _nodeState.GetCurrentNode();

        // Notify all peers
        foreach (var peer in clusterState.Nodes.Where(n => n.NodeId != currentNode.NodeId))
        {
            try
            {
                var client = _httpClientFactory.CreateClient("InternalNode");
                client.BaseAddress = new Uri(peer.BaseUrl);
                client.Timeout = TimeSpan.FromSeconds(5);
                await client.PostAsync("/api/cluster/shutdown", null);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to notify node {NodeId} about shutdown", peer.NodeId);
            }
        }
    }

    public Task<ClusterState> GetClusterStateAsync()
    {
        return Task.FromResult(_nodeState.GetClusterState());
    }
}
