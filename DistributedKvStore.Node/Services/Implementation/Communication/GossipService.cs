using System.Collections.Concurrent;
using System.Net.Http.Json;
using DistributedKvStore.Node.Services.Implementation.State;
using DistributedKvStore.Node.Services.Interfaces;
using DistributedKvStore.Shared.DTOs;
using DistributedKvStore.Shared.Enums;
using Microsoft.Extensions.Logging;

namespace DistributedKvStore.Node.Services.Implementation.Communication;

public class GossipService : IGossipService
{
    private readonly INodeStateService _nodeState;
    private readonly IHttpClientFactory _httpClientFactory;
    private readonly ILogger<GossipService> _logger;
    private readonly ConcurrentDictionary<Guid, DateTime> _processedMessages = new();
    private static readonly TimeSpan MessageRetention = TimeSpan.FromMinutes(5);
    private static readonly TimeSpan SuspectVerificationWindow = TimeSpan.FromSeconds(15);

    public GossipService(
        INodeStateService nodeState,
        IHttpClientFactory httpClientFactory,
        ILogger<GossipService> logger)
    {
        _nodeState = nodeState;
        _httpClientFactory = httpClientFactory;
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

        if(message.Topic == GossipTopic.NodeStatusChange && message.Payload.NodeStatusChanges != null)
        {
            // Apply state changes
            foreach (var change in message.Payload.NodeStatusChanges!)
            {
                ApplyNodeStatusChange(change, message.ClusterLastUpdatedAt);
            }
        }
        else if (message.Topic == GossipTopic.NodeSuspicion && message.Payload.NodeSuspicion != null)
        {
            ApplyNodeSuspicion(message.Payload.NodeSuspicion, message.ClusterLastUpdatedAt);
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
        var lastUpdatedAt = _nodeState.TouchLastUpdated();

        var message = new GossipMessage
        {
            MessageId = Guid.NewGuid(),
            ClusterLastUpdatedAt = lastUpdatedAt,
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

    private void ApplyNodeStatusChange(NodeStatusChange change, DateTime clusterLastUpdatedAt)
    {
        var clusterState = _nodeState.GetClusterState();

        if (clusterLastUpdatedAt <= clusterState.ClusterLastUpdatedAt)
            return;

        var existingNode = clusterState.Nodes.FirstOrDefault(n => n.NodeId == change.NodeId);
        if (existingNode != null)
        {
            _nodeState.UpdateNodeStatus(change.NodeId, change.NewStatus);

            if (change.NewStatus == NodeStatus.Failed)
            {
                _logger.LogWarning("Node {NodeId} marked as failed via gossip", change.NodeId);
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

        _nodeState.UpdateClusterState(new Shared.Models.ClusterState
        {
            ClusterLastUpdatedAt = clusterLastUpdatedAt,
            ReplicationFactor = clusterState.ReplicationFactor,
            Nodes = _nodeState.GetClusterState().Nodes
        });
    }

    public async Task BroadcastNodeSuspicionAsync(Guid suspectedNodeId, List<Guid> verifierNodeIds)
    {
        var currentNode = _nodeState.GetCurrentNode();
        var lastUpdatedAt = _nodeState.TouchLastUpdated();

        var message = new GossipMessage
        {
            MessageId = Guid.NewGuid(),
            ClusterLastUpdatedAt = lastUpdatedAt,
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

    private void ApplyNodeSuspicion(NodeSuspicionInfo suspicion, DateTime clusterLastUpdatedAt)
    {
        var clusterState = _nodeState.GetClusterState();

        if (clusterLastUpdatedAt <= clusterState.ClusterLastUpdatedAt)
            return;

        var targetNode = clusterState.Nodes.FirstOrDefault(n => n.NodeId == suspicion.SuspectedNodeId);
        if (targetNode == null)
            return;

        _nodeState.UpdateNodeStatus(suspicion.SuspectedNodeId, NodeStatus.Suspect);

        _logger.LogWarning("Node {NodeId} marked as SUSPECT via gossip (verifiers: {VerifierNodeIds})", suspicion.SuspectedNodeId, string.Join(", ", suspicion.VerifierNodeIds));

        _nodeState.UpdateClusterState(new Shared.Models.ClusterState
        {
            ClusterLastUpdatedAt = clusterLastUpdatedAt,
            ReplicationFactor = clusterState.ReplicationFactor,
            Nodes = _nodeState.GetClusterState().Nodes
        });

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
        else
        {
            // No reply from the suspected node - do nothing for now.
            _logger.LogDebug("Verification ping to suspected node {NodeId} got no reply", targetNode.NodeId);
            return;
        }
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
}
