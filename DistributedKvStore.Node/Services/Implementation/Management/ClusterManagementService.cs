using System.Net.Http.Json;
using DistributedKvStore.Node.Persistence;
using DistributedKvStore.Node.Services.Implementation.State;
using DistributedKvStore.Node.Services.Interfaces;
using DistributedKvStore.Shared.DTOs;
using DistributedKvStore.Shared.Enums;
using DistributedKvStore.Shared.Hashing;
using DistributedKvStore.Shared.Models;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace DistributedKvStore.Node.Services.Implementation.Management;

public class ClusterManagementService : IClusterManagementService
{
    private readonly INodeStateService _nodeState;
    private readonly IGossipService _gossipService;
    private readonly IRebalancingService _rebalancingService;
    private readonly IHttpClientFactory _httpClientFactory;
    private readonly IClusterMetadataStore _metadataStore;
    private readonly IHostApplicationLifetime _appLifetime;
    private readonly ILogger<ClusterManagementService> _logger;

    public ClusterManagementService(
        INodeStateService nodeState,
        IGossipService gossipService,
        IRebalancingService rebalancingService,
        IHttpClientFactory httpClientFactory,
        IClusterMetadataStore metadataStore,
        IHostApplicationLifetime appLifetime,
        ILogger<ClusterManagementService> logger)
    {
        _nodeState = nodeState;
        _gossipService = gossipService;
        _rebalancingService = rebalancingService;
        _httpClientFactory = httpClientFactory;
        _metadataStore = metadataStore;
        _appLifetime = appLifetime;
        _logger = logger;
    }

    public async Task<ClusterState> StartClusterAsync()
    {
        _nodeState.MarkInitialized();
        _logger.LogInformation("Cluster started with node {NodeId}", _nodeState.GetCurrentNode().NodeId);

        // Gossip cluster initialization so other nodes also mark themselves initialized.
        await _gossipService.BroadcastClusterInitAsync();

        return _nodeState.GetClusterState();
    }

    public async Task<ClusterState> AddNodeAsync(string baseUrl, Guid? nodeId = null)
    {
        var hashRing = new ConsistentHashRing();
        var hashPosition = hashRing.ComputeHash(baseUrl);

        var newNode = new ClusterNodeInfo
        {
            NodeId = nodeId ?? Guid.NewGuid(),
            BaseUrl = baseUrl,
            HashPosition = hashPosition,
            Status = NodeStatus.Joining
        };

        _nodeState.AddNode(newNode);

        _logger.LogInformation("Adding node {NodeId} at position {Position}", newNode.NodeId, hashPosition);

        // Push the current cluster state (including the new node itself) to the new node, so it doesn't start out only aware of itself.
        try
        {
            var client = _httpClientFactory.CreateClient("InternalNode");
            client.BaseAddress = new Uri(baseUrl);
            client.Timeout = TimeSpan.FromSeconds(10);

            var state = _nodeState.GetClusterState();
            await client.PostAsJsonAsync("/internal/cluster-state", state);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Could not push cluster state to new node {BaseUrl}", baseUrl);
        }

        // Gossip the join so all other nodes add the new node to their state as Joining.
        await _gossipService.BroadcastNodeJoinAsync(newNode.NodeId, newNode.BaseUrl);

        return _nodeState.GetClusterState();
    }

    public async Task<ClusterState> RemoveNodeAsync(Guid nodeId)
    {
        // Gossip removal
        var clusterState = _nodeState.GetClusterState();
        var targetNode = clusterState.Nodes.FirstOrDefault(n => n.NodeId == nodeId);
        if(targetNode == null || new[] { NodeStatus.Online, NodeStatus.Suspect, NodeStatus.Failed }.Contains(targetNode.Status) == false)
        {
            _logger.LogWarning("Node {NodeId} not found in cluster or not in a removable state", nodeId);
            return clusterState;
        }
        
        await _gossipService.BroadcastNodeRemovalProposalAsync(targetNode.NodeId, _nodeState.GetCurrentNode().NodeId);
        await _nodeState.RemoveNode(nodeId);

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

    public async Task SetReplicationFactorAsync(int factor)
    {
        if (factor < 1)
            throw new ArgumentOutOfRangeException(nameof(factor), "Replication factor must be at least 1");

        var currentFactor = _nodeState.GetReplicationFactor();
        if (currentFactor == factor)
        {
            _logger.LogInformation("Replication factor is already {Factor}; nothing to rebalance", factor);
            return;
        }

        _nodeState.SetReplicationFactor(factor);
        _logger.LogInformation("Replication factor changed from {OldFactor} to {NewFactor}", currentFactor, factor);

        await _gossipService.BroadcastReplicationFactorChangeAsync(factor);
        await _rebalancingService.RebalanceOnReplicationFactorChangeAsync(currentFactor, factor);
    }

    public async Task ShutdownClusterAsync()
    {
        _logger.LogInformation("Cluster shutdown initiated");

        var clusterState = _nodeState.GetClusterState();
        var currentNode = _nodeState.GetCurrentNode();

        // Tell every other node to shut itself down.
        foreach (var peer in clusterState.Nodes.Where(n => n.NodeId != currentNode.NodeId))
        {
            try
            {
                var client = _httpClientFactory.CreateClient("InternalNode");
                client.BaseAddress = new Uri(peer.BaseUrl);
                client.Timeout = TimeSpan.FromSeconds(5);
                await client.PostAsync("/internal/shutdown", null);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to notify node {NodeId} about shutdown", peer.NodeId);
            }
        }

        await ShutdownLocalNodeAsync();
    }

    public async Task ShutdownLocalNodeAsync()
    {
        await _metadataStore.SaveAsync(_nodeState.GetSnapshot());
        _logger.LogInformation("Cluster state snapshot saved; stopping node");

        // Delay so the HTTP response for this call can flush before the host stops.
        _ = Task.Run(async () =>
        {
            await Task.Delay(TimeSpan.FromMilliseconds(200));
            _appLifetime.StopApplication();
        });
    }

    public Task<ClusterState> GetClusterStateAsync()
    {
        return Task.FromResult(_nodeState.GetClusterState());
    }
}
