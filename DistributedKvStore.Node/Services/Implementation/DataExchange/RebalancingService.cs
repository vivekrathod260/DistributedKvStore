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

public class RebalancingService : IRebalancingService
{
    private readonly INodeStateService _nodeState;
    private readonly IDataRepository _repository;
    private readonly IHttpClientFactory _httpClientFactory;
    private readonly ILogger<RebalancingService> _logger;

    public RebalancingService(
        INodeStateService nodeState,
        IDataRepository repository,
        IHttpClientFactory httpClientFactory,
        ILogger<RebalancingService> logger)
    {
        _nodeState = nodeState;
        _repository = repository;
        _httpClientFactory = httpClientFactory;
        _logger = logger;
    }

    public async Task OnboardSelfAsync()
    {
        var currentNode = _nodeState.GetCurrentNode();
        var hashRing = _nodeState.GetHashRing();
        var replicationFactor = _nodeState.GetReplicationFactor();

        var successor = hashRing.GetNodeByOffset(currentNode.NodeId, 1);
        if (successor == null || successor.NodeId == currentNode.NodeId)
        {
            _logger.LogInformation("Node {NodeId} has no peers to onboard from", currentNode.NodeId);
            return;
        }

        var ownRange = hashRing.GetHashRange(currentNode.NodeId);
        if (ownRange == null)
        {
            _logger.LogWarning("Node {NodeId} is not present in the hash ring; cannot onboard", currentNode.NodeId);
            return;
        }

        var client = _httpClientFactory.CreateClient("InternalNode");
        client.BaseAddress = new Uri(successor.BaseUrl);
        client.Timeout = TimeSpan.FromSeconds(60);

        // Priority 1: the keys this node is now the primary owner for.
        var ownCount = await PullRangeAsync(client, currentNode.NodeId, successor, ownRange.Value.Start, ownRange.Value.End);
        _logger.LogInformation(
            "Onboarding node {NodeId}: received {Count} primary record(s) from successor {SuccessorId}",
            currentNode.NodeId, ownCount, successor.NodeId);

        // Priority 2: replica data for the preceding replicationFactor nodes. Their ranges are
        // contiguous with each other and with this node's own range, so they merge into one span.
        var farthestNode = currentNode;
        var seenNodeIds = new HashSet<Guid> { currentNode.NodeId };
        for (int i = 1; i <= replicationFactor; i++)
        {
            var precedingNode = hashRing.GetNodeByOffset(currentNode.NodeId, -i);
            if (precedingNode == null || !seenNodeIds.Add(precedingNode.NodeId))
                break; // wrapped all the way around the ring

            farthestNode = precedingNode;
        }

        if (farthestNode.NodeId == currentNode.NodeId)
            return; // no preceding nodes to replicate

        var replicationRangeStart = hashRing.GetHashRange(farthestNode.NodeId)?.Start;
        if (replicationRangeStart == null) return;

        var replicationRangeEnd = unchecked(ownRange.Value.Start - 1);

        var replicaCount = await PullRangeAsync(client, currentNode.NodeId, successor, replicationRangeStart.Value, replicationRangeEnd);
        _logger.LogInformation(
            "Onboarding node {NodeId}: received {Count} replicated record(s) from successor {SuccessorId}",
            currentNode.NodeId, replicaCount, successor.NodeId);
    }

    // Gossip message processing
    public async Task ProcessGossipMessageAsync(GossipMessage message)
    {
        await CheckNodeRemovalRebalancing(message);
        await RebalanceOnNodeJoinAsync(message);
    }

    // Node join
    private async Task RebalanceOnNodeJoinAsync(GossipMessage message)
    {
        if(message.Topic != GossipTopic.NodeStatusChange || message.Payload.NodeStatusChanges == null)  return;

        var newNodeChange = message.Payload.NodeStatusChanges.FirstOrDefault();
        if (newNodeChange == null) return;

        var clusterState = _nodeState.GetClusterState();
        var newNode = clusterState.Nodes.FirstOrDefault(n => n.NodeId == newNodeChange.NodeId);
        if (newNode == null) return;

        if(newNode.Status != NodeStatus.Joining || newNodeChange.NewStatus != NodeStatus.Online) return;

        var hashRing = _nodeState.GetHashRing();
        var replicationFactor = _nodeState.GetReplicationFactor();

        var totalNodes = hashRing.GetSortedNodes().Count;
        if (totalNodes <= replicationFactor) return;

        var effectedNodes = new HashSet<Guid>();
        for(int i = 1; i <= replicationFactor + 1; i++)
        {
            var effectedNode = hashRing.GetNodeByOffset(newNode.NodeId, i);
            if (effectedNode == null || !effectedNodes.Add(effectedNode.NodeId)) break; // wrapped all the way around the ring
        }

        var currentNode = _nodeState.GetCurrentNode();
        if(!effectedNodes.Contains(currentNode.NodeId)) return;  // Didn't affect this node, nothing to rebalance

        var node = hashRing.GetNodeByOffset(currentNode.NodeId, -(replicationFactor+1));
        if(node == null) return;

        var range = hashRing.GetHashRange(node.NodeId);
        if (range == null) return;

        await _repository.DeleteRecordsInHashRangeAsync(range.Value.Start, range.Value.End);
    }

    // Node removal
    private async Task CheckNodeRemovalRebalancing(GossipMessage message)
    {
        if(message.Topic != GossipTopic.NodeStatusChange || message.Payload.NodeStatusChanges == null)  return;

        var nodeChange = message.Payload.NodeStatusChanges.FirstOrDefault();
        if (nodeChange?.NewStatus != NodeStatus.Leaving) return;

        var clusterState = _nodeState.GetClusterState();
        var offlineNode = clusterState.Nodes.FirstOrDefault(n => n.NodeId == nodeChange.NodeId);
        if (offlineNode == null || new[] { NodeStatus.Online, NodeStatus.Suspect, NodeStatus.Failed }.Contains(offlineNode.Status) == false) return;

        await RebalanceOnNodeRemovalAsync(offlineNode);
    }

    // Rebalance data when given node goes offline
    public async Task RebalanceOnNodeRemovalAsync(ClusterNodeInfo offlineNode)
    {
        var hashRing = _nodeState.GetHashRing();
        var replicationFactor = _nodeState.GetReplicationFactor();

        var totalNodes = hashRing.GetSortedNodes().Count;
        if (totalNodes <= replicationFactor) return;

        var effectedNodes = new HashSet<Guid>();
        for(int i = 1; i <= replicationFactor + 1; i++)
        {
            var effectedNode = hashRing.GetNodeByOffset(offlineNode.NodeId, i);
            if (effectedNode == null || !effectedNodes.Add(effectedNode.NodeId)) break;
        }

        var currentNode = _nodeState.GetCurrentNode();
        if(!effectedNodes.Contains(currentNode.NodeId)) return;

        ClusterNodeInfo? targetNode;
        (ulong Start, ulong End)? reqRange;

        if(hashRing.GetNodeByOffset(currentNode.NodeId, -(replicationFactor+1))?.NodeId == offlineNode.NodeId)
        {
            targetNode = hashRing.GetNodeByOffset(currentNode.NodeId, -replicationFactor);
            reqRange = hashRing.GetHashRange(hashRing.GetNodeByOffset(currentNode.NodeId, -(replicationFactor+1))?.NodeId ?? Guid.Empty);
        }
        else
        {
            targetNode = hashRing.GetNodeByOffset(currentNode.NodeId, -(replicationFactor+1));
            reqRange = hashRing.GetHashRange(targetNode?.NodeId ?? Guid.Empty);
        }

        if(targetNode == null || reqRange == null) return;

        var client = _httpClientFactory.CreateClient("InternalNode");
        client.Timeout = TimeSpan.FromSeconds(60);
        client.BaseAddress = new Uri(targetNode.BaseUrl);

        var replicaCount = await PullRangeAsync(client, currentNode.NodeId, targetNode, reqRange.Value.Start, reqRange.Value.End);
        _logger.LogInformation("Fetched {Count} replicated record(s) from Node {NodeId}", replicaCount, targetNode.NodeId);
    }

    // Replication factor change
    public async Task RebalanceOnReplicationFactorChangeAsync(int currentReplicationFactor, int newReplicationFactor)
    {
        if (newReplicationFactor == currentReplicationFactor) return;

        var hashRing = _nodeState.GetHashRing();
        var currentNode = _nodeState.GetCurrentNode();

        var maxOffset = hashRing.GetSortedNodes().Count - 1;
        if (maxOffset < 1) return;

        if (newReplicationFactor > currentReplicationFactor)
        {
            await FetchAdditionalReplicaRangesAsync(hashRing, currentNode, currentReplicationFactor + 1, Math.Min(newReplicationFactor, maxOffset));
        }
        else
        {
            await DropSurplusReplicaRangesAsync(hashRing, currentNode, newReplicationFactor + 1, Math.Min(currentReplicationFactor, maxOffset));
        }
    }

    // Pull the primary ranges of the predecessors sitting [firstOffset, lastOffset] hops back.
    private async Task FetchAdditionalReplicaRangesAsync(IHashRing hashRing, ClusterNodeInfo currentNode, int firstOffset, int lastOffset)
    {
        for (int offset = firstOffset; offset <= lastOffset; offset++)
        {
            var sourceNode = hashRing.GetNodeByOffset(currentNode.NodeId, -offset);
            if (sourceNode == null || sourceNode.NodeId == currentNode.NodeId) break; // wrapped all the way around the ring

            var range = hashRing.GetHashRange(sourceNode.NodeId);
            if (range == null) continue;

            var client = _httpClientFactory.CreateClient("InternalNode");
            client.BaseAddress = new Uri(sourceNode.BaseUrl);
            client.Timeout = TimeSpan.FromSeconds(60);

            var count = await PullRangeAsync(client, currentNode.NodeId, sourceNode, range.Value.Start, range.Value.End);
            _logger.LogInformation(
                "Replication factor increase: node {NodeId} received {Count} replicated record(s) for the range owned by node {SourceNodeId}",
                currentNode.NodeId, count, sourceNode.NodeId);
        }
    }

    // Drop the ranges of the predecessors sitting [firstOffset, lastOffset] hops back
    private async Task DropSurplusReplicaRangesAsync(IHashRing hashRing, ClusterNodeInfo currentNode, int firstOffset, int lastOffset)
    {
        if (firstOffset > lastOffset) return;

        var nearestNode = hashRing.GetNodeByOffset(currentNode.NodeId, -firstOffset);
        var farthestNode = hashRing.GetNodeByOffset(currentNode.NodeId, -lastOffset);
        if (nearestNode == null || farthestNode == null) return;

        var nearestRange = hashRing.GetHashRange(nearestNode.NodeId);
        var farthestRange = hashRing.GetHashRange(farthestNode.NodeId);
        if (nearestRange == null || farthestRange == null) return;

        await _repository.DeleteRecordsInHashRangeAsync(farthestRange.Value.Start, nearestRange.Value.End);

        _logger.LogInformation("Replication factor decrease: node {NodeId} dropped replica range [{Start}, {End}] covering nodes {FarthestNodeId}..{NearestNodeId}",
            currentNode.NodeId, farthestRange.Value.Start, nearestRange.Value.End, farthestNode.NodeId, nearestNode.NodeId);
    }

    // copy given range records from the target node
    private async Task<int> PullRangeAsync(HttpClient client, Guid requestingNodeId, ClusterNodeInfo targetDataNode, ulong rangeStart, ulong rangeEnd)
    {
        try
        {
            var request = new MigrationRequest
            {
                RequestingNodeId = requestingNodeId,
                RangeStart = rangeStart,
                RangeEnd = rangeEnd
            };

            var response = await client.PostAsJsonAsync("/internal/migrate-out", request);
            if (!response.IsSuccessStatusCode)
            {
                _logger.LogWarning(
                    "Target node {TargetNodeId} returned {StatusCode} for range [{Start}, {End}]",
                    targetDataNode.NodeId, response.StatusCode, rangeStart, rangeEnd);
                return 0;
            }

            var migrationData = await response.Content.ReadFromJsonAsync<MigrationResponse>();
            if (migrationData?.Records is not { Count: > 0 })
                return 0;

            await _repository.BulkInsertRecordsAsync(migrationData.Records);
            return migrationData.Records.Count;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to pull hash range [{Start}, {End}] from target node {TargetNodeId}",
                rangeStart, rangeEnd, targetDataNode.NodeId);
            return 0;
        }
    }

}
