using System.Collections.Concurrent;
using DistributedKvStore.Node.Services.Implementation.State;
using DistributedKvStore.Node.Services.Interfaces;
using DistributedKvStore.Shared.DTOs;
using DistributedKvStore.Shared.Enums;
using DistributedKvStore.Shared.Models;

namespace DistributedKvStore.Node.Services.Implementation.Communication;

public class GossipService : IGossipService
{
    private readonly INodeStateService _nodeState;
    private readonly IHttpClientFactory _httpClientFactory;
    private readonly ILogger<GossipService> _logger;
    private readonly ConcurrentDictionary<string, DateTime> _processedMessages = new();
    private static readonly TimeSpan MessageRetention = TimeSpan.FromMinutes(5);

    public GossipService(INodeStateService nodeState, IHttpClientFactory httpClientFactory, ILogger<GossipService> logger)
    {
        _nodeState = nodeState;
        _httpClientFactory = httpClientFactory;
        _logger = logger;
    }

    // #################### Core Method: Broadcast a gossip message to a random subset of peers
    public async Task BroadcastGossipAsync(GossipMessage message)
    {
        _processedMessages[GetMessageKey(message)] = DateTime.UtcNow;

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
        var msgKey = GetMessageKey(message);

        if (_processedMessages.ContainsKey(msgKey))
        {
            _logger.LogDebug("Ignoring duplicate gossip message {MessageId}", message.MessageId);
            return;
        }

        _processedMessages[msgKey] = DateTime.UtcNow;
        CleanupOldMessages();

        // Process the message based on its topic
        if (message.Topic == GossipTopic.NodeStatusChange && message.Payload.NodeStatusChanges != null)
        {
            foreach (var change in message.Payload.NodeStatusChanges!)
            {
                ApplyNodeStatusChange(change, message.ClusterVersion);
            }

            await ForwardGossipAsync(message);
            return;
        }

        if (message.Topic == GossipTopic.SuspectDetected && message.Payload.NodeSuspicionMessage != null)
        {
            bool suspectAlreadyProcessed = message.Payload.NodeSuspicionMessage.Processors
                                            .Contains(_nodeState.GetCurrentNode().NodeId);
            if (suspectAlreadyProcessed)
            {
                _logger.LogDebug("Ignoring suspect gossip message {MessageId} as this node has already reported", message.MessageId);
                return;
            }

            await ProcessSuspectCheckAsync(message);
            return;
        }
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

    private string GetMessageKey(GossipMessage message)
    {
        if (message.Topic == GossipTopic.SuspectDetected && message.Payload.NodeSuspicionMessage != null)
        {
            var reporters = string.Join(',', message.Payload.NodeSuspicionMessage.Reporters.Distinct().OrderBy(id => id));
            return $"{message.Topic}:{message.MessageId:N}:{reporters}";
        }

        return $"{message.Topic}:{message.MessageId:N}";
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
        var version = _nodeState.IncrementVersion();

        var message = new GossipMessage
        {
            MessageId = Guid.NewGuid(),
            ClusterVersion = version,
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
            _nodeState.AddNode(new ClusterNodeInfo
            {
                NodeId = change.NodeId,
                BaseUrl = change.BaseUrl ?? string.Empty,
                HashPosition = change.HashPosition,
                Status = change.NewStatus
            });
        }

        _nodeState.UpdateClusterState(new ClusterState
        {
            Version = clusterVersion,
            ReplicationFactor = clusterState.ReplicationFactor,
            Nodes = _nodeState.GetClusterState().Nodes
        });
    }

    public async Task BroadcastNodeSuspicionAsync(ClusterNodeInfo suspectedNode)
    {
        var currentNode = _nodeState.GetCurrentNode();

        _nodeState.UpdateNodeStatus(suspectedNode.NodeId, NodeStatus.Suspect);
        var version = _nodeState.IncrementVersion();

        var message = new GossipMessage
        {
            MessageId = Guid.NewGuid(),
            ClusterVersion = version,
            SenderNodeId = currentNode.NodeId,
            Topic = GossipTopic.SuspectDetected,
            Payload = new GossipPayload
            {
                NodeSuspicionMessage = new NodeSuspicionMessage
                {
                    SuspectedNode = suspectedNode,
                    Processors = new List<Guid> { currentNode.NodeId },
                    Reporters = new List<Guid> { currentNode.NodeId }
                }
            },
            TimestampUtc = DateTime.UtcNow
        };

        await BroadcastGossipAsync(message);
    }

    private async Task ProcessSuspectCheckAsync(GossipMessage message)
    {
        var suspicion = message.Payload.NodeSuspicionMessage!;
        var suspectedNode = suspicion.SuspectedNode;
        var currentNode = _nodeState.GetCurrentNode();
        var clusterState = _nodeState.GetClusterState();

        if (suspectedNode.NodeId == currentNode.NodeId)
        {
            return;
        }

        var processors = suspicion.Processors.Distinct().ToList();
        if (!processors.Contains(currentNode.NodeId))
        {
            processors.Add(currentNode.NodeId);
        }

        suspicion.Processors = processors;
        message.SenderNodeId = currentNode.NodeId;

        if (await IsNodeReachableAsync(suspectedNode))
        {
            if (suspectedNode.Status == NodeStatus.Suspect || suspectedNode.Status == NodeStatus.Failed)
            {
                await MarkNodeOnlineAsync(suspectedNode);
            }
            return;
        }
        else
        {
            var reporters = suspicion.Reporters.Distinct().ToList();
            if (!reporters.Contains(currentNode.NodeId))
            {
                reporters.Add(currentNode.NodeId);
            }

            suspicion.Reporters = reporters;

            if (HasMajorityReporters(reporters, clusterState.Nodes))
            {
                _logger.LogWarning(
                    "Node {NodeId} reached failure quorum with reporters {ReporterCount}/{NodeCount}",
                    suspectedNode.NodeId,
                    reporters.Count,
                    clusterState.Nodes.Count);

                await MarkNodeFailedAsync(suspectedNode);
                return;
            }

            _nodeState.UpdateNodeStatus(suspectedNode.NodeId, NodeStatus.Suspect);
            _nodeState.IncrementVersion();
        }

        // Forward the suspicion message to other nodes that haven't processed it yet
        var peers = clusterState.Nodes
            .Where(n => n.Status == NodeStatus.Online && suspicion.Processors.Contains(n.NodeId) == false)
            .ToList();

        var forwardTo = peers.OrderBy(_ => Random.Shared.Next()).Take(2).ToList();
        var tasks = forwardTo.Select(peer => SendGossipToNodeAsync(message, peer.BaseUrl));
        await Task.WhenAll(tasks);
    }

    private bool HasMajorityReporters(IReadOnlyCollection<Guid> reporters, IReadOnlyCollection<ClusterNodeInfo> clusterNodes)
    {
        var eligibleNodeCount = clusterNodes.Count(n => n.Status != NodeStatus.Leaving);
        return reporters.Distinct().Count() > eligibleNodeCount / 2;
    }

    private async Task MarkNodeOnlineAsync(ClusterNodeInfo node)
    {
        _logger.LogInformation("Node {NodeId} is reachable", node.NodeId);
        _nodeState.UpdateNodeStatus(node.NodeId, NodeStatus.Online);

        await BroadcastNodeStatusChangeAsync(new List<NodeStatusChange>
        {
            new()
            {
                NodeId = node.NodeId,
                NewStatus = NodeStatus.Online,
                BaseUrl = node.BaseUrl,
                HashPosition = node.HashPosition
            }
        });
    }

    private async Task MarkNodeFailedAsync(ClusterNodeInfo node)
    {
        _logger.LogWarning("Node {NodeId} is failed", node.NodeId);
        _nodeState.UpdateNodeStatus(node.NodeId, NodeStatus.Failed);

        await BroadcastNodeStatusChangeAsync(new List<NodeStatusChange>
        {
            new()
            {
                NodeId = node.NodeId,
                NewStatus = NodeStatus.Failed,
                BaseUrl = node.BaseUrl,
                HashPosition = node.HashPosition
            }
        });
    }

    private async Task<bool> IsNodeReachableAsync(ClusterNodeInfo node, CancellationToken cancellationToken = default)
    {
        var client = _httpClientFactory.CreateClient("InternalNode");
        client.BaseAddress = new Uri(node.BaseUrl);
        client.Timeout = TimeSpan.FromSeconds(3);

        var directResponse = await client.GetAsync("/internal/ping", cancellationToken);

        if (directResponse.IsSuccessStatusCode && ((await directResponse.Content.ReadFromJsonAsync<PingResponse>())?.IsInitialized ?? false))
        {
            return true;
        }

        var currentNode = _nodeState.GetCurrentNode();
        var clusterState = _nodeState.GetClusterState();

        var proxyNodes = clusterState.Nodes
            .Where(n => n.NodeId != currentNode.NodeId && n.NodeId != node.NodeId && n.Status == NodeStatus.Online)
            .OrderBy(_ => Random.Shared.Next())
            .Take(3)
            .ToList();

        foreach (var proxyNode in proxyNodes)
        {
            if (await ProxyPingAsync(proxyNode, node, cancellationToken))
            {
                return true;
            }
        }

        return false;
    }

    private async Task<bool> ProxyPingAsync(ClusterNodeInfo helperNode, ClusterNodeInfo targetNode, CancellationToken cancellationToken)
    {
        var client = _httpClientFactory.CreateClient("InternalNode");
        client.BaseAddress = new Uri(helperNode.BaseUrl);
        client.Timeout = TimeSpan.FromSeconds(3);

        var response = await client.PostAsJsonAsync("/internal/proxy-ping", new ProxyPingRequest
        {
            TargetNode = targetNode
        }, cancellationToken);

        return response.IsSuccessStatusCode && ((await response.Content.ReadFromJsonAsync<PingResponse>())?.IsInitialized ?? false);
    }
}
