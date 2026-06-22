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

    public GossipService(
        INodeStateService nodeState,
        IHttpClientFactory httpClientFactory,
        ILogger<GossipService> logger)
    {
        _nodeState = nodeState;
        _httpClientFactory = httpClientFactory;
        _logger = logger;
    }

    public async Task BroadcastNodeStatusChangeAsync(List<NodeStatusChange> changes)
    {
        var currentNode = _nodeState.GetCurrentNode();
        var version = _nodeState.IncrementVersion();

        var message = new GossipMessage
        {
            MessageId = Guid.NewGuid(),
            ClusterVersion = version,
            SenderNodeId = currentNode.NodeId,
            NodeStatusChanges = changes,
            TimestampUtc = DateTime.UtcNow
        };

        await BroadcastGossipAsync(message);
    }

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

        // Apply state changes
        foreach (var change in message.NodeStatusChanges)
        {
            ApplyNodeStatusChange(change, message.ClusterVersion);
        }

        // Forward to other nodes
        await ForwardGossipAsync(message);
    }

    private void ApplyNodeStatusChange(NodeStatusChange change, long clusterVersion)
    {
        var clusterState = _nodeState.GetClusterState();

        if (clusterVersion <= clusterState.Version)
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
            Version = clusterVersion,
            ReplicationFactor = clusterState.ReplicationFactor,
            Nodes = _nodeState.GetClusterState().Nodes
        });
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
}
