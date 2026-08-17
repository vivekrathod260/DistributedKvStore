using System.Collections.Concurrent;
using System.Net.Http.Json;
using DistributedKvStore.Node.Services.Implementation.State;
using DistributedKvStore.Node.Services.Interfaces;
using DistributedKvStore.Shared.DTOs;
using DistributedKvStore.Shared.Enums;
using DistributedKvStore.Shared.Hashing;
using Microsoft.Extensions.Logging;

namespace DistributedKvStore.Node.Services.Implementation.Communication;

public class GossipService : IGossipService
{
    private readonly INodeStateService _nodeState;
    private readonly IHttpClientFactory _httpClientFactory;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<GossipService> _logger;
    private readonly ConcurrentDictionary<Guid, DateTime> _processedMessages = new();
    private static readonly TimeSpan MessageRetention = TimeSpan.FromMinutes(5);
    private static readonly TimeSpan SuspectVerificationWindow = TimeSpan.FromSeconds(15);

    public GossipService(
        INodeStateService nodeState,
        IHttpClientFactory httpClientFactory,
        IServiceScopeFactory scopeFactory,
        ILogger<GossipService> logger)
    {
        _nodeState = nodeState;
        _httpClientFactory = httpClientFactory;
        _scopeFactory = scopeFactory;
        _logger = logger;
    }

    // #################### Core Method: Broadcast a gossip message to a random subset of peers
    public async Task BroadcastGossipAsync(GossipMessage message)
    {
        _processedMessages[message.MessageId] = DateTime.UtcNow;

        var currentNode = _nodeState.GetCurrentNode();
        var clusterState = _nodeState.GetClusterState();

        var somePeers = clusterState.Nodes
            .Where(n => n.NodeId != currentNode.NodeId && n.Status == NodeStatus.Online)
            .OrderBy(_ => Random.Shared.Next()).Take(3).ToList();

        var tasks = somePeers.Select(peer => SendGossipToNodeAsync(message, peer.BaseUrl));
        await Task.WhenAll(tasks);
    }

    private async Task SendGossipToNodeAsync(GossipMessage message, string baseUrl)
    {
        try
        {
            var client = _httpClientFactory.CreateClient("InternalNode");
            client.BaseAddress = new Uri(baseUrl);
            client.Timeout = TimeSpan.FromSeconds(3);

            await client.PostAsJsonAsync("/internal/gossip", message);
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Failed to send gossip to {BaseUrl}", baseUrl);
        }
    }

    public async Task ProcessGossipMessageAsync(GossipMessage message)
    {
        // Deduplicate
        if (_processedMessages.ContainsKey(message.MessageId))
        {
            _logger.LogDebug("Ignoring duplicate gossip message {MessageId}", message.MessageId);
            return;
        }

        _processedMessages[message.MessageId] = DateTime.UtcNow;
        CleanupOldMessages();

        // Receiving any gossip message is itself proof the sender is alive right now.
        _nodeState.TouchLastSeen(message.SenderNodeId);

        if(message.Topic == GossipTopic.NodeStatusChange && message.Payload.NodeStatusChanges != null)
        {
            // Apply state changes
            foreach (var change in message.Payload.NodeStatusChanges!)
            {
                ApplyNodeStatusChange(change);
            }
        }
        else if (message.Topic == GossipTopic.NodeSuspicion && message.Payload.NodeSuspicion != null)
        {
            ApplyNodeSuspicion(message.Payload.NodeSuspicion);
        }
        else if (message.Topic == GossipTopic.NodeJoin && message.Payload.NodeJoin != null)
        {
            ApplyNodeJoin(message.Payload.NodeJoin);
        }
        else if (message.Topic == GossipTopic.ClusterInit)
        {
            ApplyClusterInit();
        }
        else if (message.Topic == GossipTopic.NodeRemovalProposal && message.Payload.NodeRemovalProposal != null)
        {
            ApplyNodeRemovalProposal(message.Payload.NodeRemovalProposal);
        }
        else if (message.Topic == GossipTopic.ReplicationFactorChange && message.Payload.ReplicationFactorChange != null)
        {
            ApplyReplicationFactorChange(message.Payload.ReplicationFactorChange);
        }

        // Forward to other nodes
        await ForwardGossipAsync(message);
    }

    private async Task ForwardGossipAsync(GossipMessage message)
    {
        var currentNode = _nodeState.GetCurrentNode();
        var clusterState = _nodeState.GetClusterState();

        var peers = clusterState.Nodes
            .Where(n => n.NodeId != currentNode.NodeId
                        && n.NodeId != message.SenderNodeId
                        && n.Status == NodeStatus.Online)
            .ToList();

        // Forward to random subset (fan-out)
        var forwardTo = peers.OrderBy(_ => Random.Shared.Next()).Take(3).ToList();
        var tasks = forwardTo.Select(peer => SendGossipToNodeAsync(message, peer.BaseUrl));
        await Task.WhenAll(tasks);
    }

    private void CleanupOldMessages()
    {
        var cutoff = DateTime.UtcNow - MessageRetention;
        var expired = _processedMessages.Where(kv => kv.Value < cutoff).Select(kv => kv.Key).ToList();
        foreach (var id in expired)
        {
            _processedMessages.TryRemove(id, out _);
        }
    }


    // ####################### Utility Methods
    public async Task BroadcastNodeStatusChangeAsync(List<NodeStatusChange> changes)
    {
        var currentNode = _nodeState.GetCurrentNode();

        var message = new GossipMessage
        {
            MessageId = Guid.NewGuid(),
            SenderNodeId = currentNode.NodeId,
            Topic = GossipTopic.NodeStatusChange,
            Payload = new GossipPayload
            {
                NodeStatusChanges = changes
            },
            TimestampUtc = DateTime.UtcNow
        };

        await BroadcastGossipAsync(message);
    }

    private void ApplyNodeStatusChange(NodeStatusChange change)
    {
        var clusterState = _nodeState.GetClusterState();
        var existingNode = clusterState.Nodes.FirstOrDefault(n => n.NodeId == change.NodeId);

        if (existingNode != null)
        {
            if (change.NewStatus == NodeStatus.Failed)
            {
                _logger.LogWarning("Node {NodeId} marked as failed via gossip", change.NodeId);
                _nodeState.RemoveNode(change.NodeId);
                return;
            }

            _nodeState.UpdateNodeStatus(change.NodeId, change.NewStatus);

            if (change.NewStatus == NodeStatus.Online)
            {
                _nodeState.TouchLastSeen(change.NodeId);
            }
        }
        else if (change.NewStatus == NodeStatus.Online || change.NewStatus == NodeStatus.Joining)
        {
            _nodeState.AddNode(new Shared.Models.ClusterNodeInfo
            {
                NodeId = change.NodeId,
                BaseUrl = change.BaseUrl ?? string.Empty,
                HashPosition = change.HashPosition,
                Status = change.NewStatus
            });
        }
    }

    // ####################### Node Join Handling
    public async Task BroadcastNodeJoinAsync(Guid nodeId, string baseUrl)
    {
        var currentNode = _nodeState.GetCurrentNode();
        var hashRing = new ConsistentHashRing();

        var message = new GossipMessage
        {
            MessageId = Guid.NewGuid(),
            SenderNodeId = currentNode.NodeId,
            Topic = GossipTopic.NodeJoin,
            Payload = new GossipPayload
            {
                NodeJoin = new NodeJoinInfo
                {
                    NodeId = nodeId,
                    BaseUrl = baseUrl,
                    HashPosition = hashRing.ComputeHash(baseUrl)
                }
            },
            TimestampUtc = DateTime.UtcNow
        };

        await BroadcastGossipAsync(message);
    }

    private void ApplyNodeJoin(NodeJoinInfo joinInfo)
    {
        var clusterState = _nodeState.GetClusterState();
        var existingNode = clusterState.Nodes.FirstOrDefault(n => n.NodeId == joinInfo.NodeId);

        if (existingNode == null)
        {
            _nodeState.AddNode(new Shared.Models.ClusterNodeInfo
            {
                NodeId = joinInfo.NodeId,
                BaseUrl = joinInfo.BaseUrl,
                HashPosition = joinInfo.HashPosition,
                Status = NodeStatus.Joining
            });

            _logger.LogInformation("Node {NodeId} added as Joining via gossip", joinInfo.NodeId);
        }
    }

    public async Task BroadcastNodeSuspicionAsync(Guid suspectedNodeId, List<Guid> verifierNodeIds)
    {
        var currentNode = _nodeState.GetCurrentNode();

        var message = new GossipMessage
        {
            MessageId = Guid.NewGuid(),
            SenderNodeId = currentNode.NodeId,
            Topic = GossipTopic.NodeSuspicion,
            Payload = new GossipPayload
            {
                NodeSuspicion = new NodeSuspicionInfo
                {
                    SuspectedNodeId = suspectedNodeId,
                    VerifierNodeIds = verifierNodeIds
                }
            },
            TimestampUtc = DateTime.UtcNow
        };

        await BroadcastGossipAsync(message);
    }

    private void ApplyNodeSuspicion(NodeSuspicionInfo suspicion)
    {
        var clusterState = _nodeState.GetClusterState();

        var targetNode = clusterState.Nodes.FirstOrDefault(n => n.NodeId == suspicion.SuspectedNodeId);
        if (targetNode == null)
            return;

        _nodeState.UpdateNodeStatus(suspicion.SuspectedNodeId, NodeStatus.Suspect);

        _logger.LogWarning("Node {NodeId} marked as SUSPECT via gossip (verifiers: {VerifierNodeIds})", suspicion.SuspectedNodeId, string.Join(", ", suspicion.VerifierNodeIds));

        var currentNode = _nodeState.GetCurrentNode();
        if (suspicion.VerifierNodeIds.Contains(currentNode.NodeId))
        {
            _ = Task.Run(() => VerifySuspectedNodeAsync(targetNode));
        }
    }

    private async Task VerifySuspectedNodeAsync(Shared.Models.ClusterNodeInfo targetNode)
    {
        var delay = TimeSpan.FromMilliseconds(Random.Shared.Next(0, (int)SuspectVerificationWindow.TotalMilliseconds));
        await Task.Delay(delay);

        var clusterState = _nodeState.GetClusterState();

        var suspectNode = clusterState.Nodes
            .Where(n => n.NodeId == targetNode.NodeId)
            .FirstOrDefault();

        if(suspectNode?.Status == NodeStatus.Online) return;

        var reachable = await PingNodeAsync(targetNode.BaseUrl);

        if(reachable)
        {
            _logger.LogInformation("Verification ping to suspected node {NodeId} succeeded; broadcasting online status", targetNode.NodeId);

            _nodeState.TouchLastSeen(targetNode.NodeId);
            _nodeState.UpdateNodeStatus(targetNode.NodeId, NodeStatus.Online);

            await BroadcastNodeStatusChangeAsync(new List<NodeStatusChange>
            {
                new()
                {
                    NodeId = targetNode.NodeId,
                    NewStatus = NodeStatus.Online,
                    BaseUrl = targetNode.BaseUrl,
                    HashPosition = targetNode.HashPosition
                }
            });
        }
    }

    // ####################### Cluster Init Handling
    public async Task BroadcastClusterInitAsync()
    {
        var currentNode = _nodeState.GetCurrentNode();

        var message = new GossipMessage
        {
            MessageId = Guid.NewGuid(),
            SenderNodeId = currentNode.NodeId,
            Topic = GossipTopic.ClusterInit,
            Payload = new GossipPayload(),
            TimestampUtc = DateTime.UtcNow
        };

        await BroadcastGossipAsync(message);
    }

    private void ApplyClusterInit()
    {
        if (_nodeState.IsInitialized) return;

        _nodeState.MarkInitialized();
        _logger.LogInformation("Cluster initialized via gossip");
    }

    private async Task<bool> PingNodeAsync(string baseUrl)
    {
        try
        {
            var client = _httpClientFactory.CreateClient("InternalNode");
            client.BaseAddress = new Uri(baseUrl);
            client.Timeout = TimeSpan.FromSeconds(3);

            var response = await client.GetAsync("/internal/heartbeat");
            return response.IsSuccessStatusCode;
        }
        catch
        {
            return false;
        }
    }

    public async Task BroadcastNodeRemovalProposalAsync(Guid offlineNodeId, Guid proposerNodeId)
    {
        var currentNode = _nodeState.GetCurrentNode();

        var message = new GossipMessage
        {
            MessageId = Guid.NewGuid(),
            SenderNodeId = currentNode.NodeId,
            Topic = GossipTopic.NodeRemovalProposal,
            Payload = new GossipPayload
            {
                NodeRemovalProposal = new NodeRemovalProposalInfo
                {
                    OfflineNodeId = offlineNodeId,
                    ProposerNodeId = proposerNodeId
                }
            },
            TimestampUtc = DateTime.UtcNow
        };

        await BroadcastGossipAsync(message);
    }

    private void ApplyNodeRemovalProposal(NodeRemovalProposalInfo proposal)
    {
        _logger.LogWarning("Received node removal proposal for {OfflineNodeId} from proposer {ProposerNodeId}",
            proposal.OfflineNodeId, proposal.ProposerNodeId);
        _nodeState.RemoveNode(proposal.OfflineNodeId);
    }

    public async Task BroadcastReplicationFactorChangeAsync(int newReplicationFactor)
    {
        var currentNode = _nodeState.GetCurrentNode();

        var message = new GossipMessage
        {
            MessageId = Guid.NewGuid(),
            SenderNodeId = currentNode.NodeId,
            Topic = GossipTopic.ReplicationFactorChange,
            Payload = new GossipPayload
            {
                ReplicationFactorChange = new ReplicationFactorChangeInfo
                {
                    NewReplicationFactor = newReplicationFactor
                }
            },
            TimestampUtc = DateTime.UtcNow
        };

        await BroadcastGossipAsync(message);
    }

    private void ApplyReplicationFactorChange(ReplicationFactorChangeInfo change)
    {
        if (change.NewReplicationFactor < 1)
        {
            _logger.LogWarning("Ignoring gossiped replication factor {Factor}: must be at least 1", change.NewReplicationFactor);
            return;
        }

        var currentFactor = _nodeState.GetReplicationFactor();
        if (currentFactor == change.NewReplicationFactor)
            return;

        _nodeState.SetReplicationFactor(change.NewReplicationFactor);
        _logger.LogInformation("Replication factor changed from {OldFactor} to {NewFactor} via gossip", currentFactor, change.NewReplicationFactor);

        _ = Task.Run(() => RebalanceAfterReplicationFactorChangeAsync(currentFactor, change.NewReplicationFactor));
    }

    private async Task RebalanceAfterReplicationFactorChangeAsync(int currentFactor, int newFactor)
    {
        try
        {
            using var scope = _scopeFactory.CreateScope();
            var rebalancingService = scope.ServiceProvider.GetRequiredService<IRebalancingService>();
            await rebalancingService.RebalanceOnReplicationFactorChangeAsync(currentFactor, newFactor);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Rebalancing after replication factor change from {OldFactor} to {NewFactor} failed",
                currentFactor, newFactor);
        }
    }
}
