using DistributedKvStore.Node.Services.Implementation.State;
using DistributedKvStore.Node.Services.Interfaces;
using DistributedKvStore.Shared.DTOs;
using DistributedKvStore.Shared.Enums;
using DistributedKvStore.Shared.Models;

namespace DistributedKvStore.Node.BackgroundServices;

public class PingService : BackgroundService
{
    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger<PingService> _logger;
    private static readonly TimeSpan PingInterval = TimeSpan.FromSeconds(5);

    public PingService(IServiceProvider serviceProvider, ILogger<PingService> logger)
    {
        _serviceProvider = serviceProvider;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken); // Initial delay

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await CheckPingAsync(stoppingToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Ping failed");
            }

            await Task.Delay(PingInterval, stoppingToken);
        }
    }

    private async Task CheckPingAsync(CancellationToken cancellationToken)
    {
        using var scope = _serviceProvider.CreateScope();
        var nodeState = scope.ServiceProvider.GetRequiredService<INodeStateService>();
        var gossipService = scope.ServiceProvider.GetRequiredService<IGossipService>();
        var httpClientFactory = scope.ServiceProvider.GetRequiredService<IHttpClientFactory>();

        if (!nodeState.IsInitialized) return;

        var currentNode = nodeState.GetCurrentNode();
        var clusterState = nodeState.GetClusterState();

        var peers = clusterState.Nodes
            .Where(n => n.NodeId != currentNode.NodeId && n.Status != NodeStatus.Leaving)
            .ToList();

        if (peers.Count == 0)
        {
            return;
        }

        var targetNode = peers[Random.Shared.Next(peers.Count)];

        if (await IsNodeReachableAsync(targetNode, httpClientFactory, cancellationToken))
        {
            await MarkNodeOnlineAsync(targetNode, nodeState, gossipService);
            return;
        }

        var proxyNodes = clusterState.Nodes
            .Where(n => n.NodeId != currentNode.NodeId && n.NodeId != targetNode.NodeId && n.Status == NodeStatus.Online)
            .OrderBy(_ => Random.Shared.Next())
            .Take(Random.Shared.Next(1, 3))
            .ToList();

        foreach (var proxyNode in proxyNodes)
        {
            try
            {
                if (await ProxyPingAsync(proxyNode, targetNode, httpClientFactory, cancellationToken))
                {
                    await MarkNodeOnlineAsync(targetNode, nodeState, gossipService);
                    return;
                }
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Proxy ping from {HelperNodeId} to {TargetNodeId} failed", proxyNode.NodeId, targetNode.NodeId);
            }
        }

        await MarkNodeSuspectAsync(targetNode, nodeState, gossipService);
    }

    private async Task<bool> IsNodeReachableAsync(
        ClusterNodeInfo node,
        IHttpClientFactory httpClientFactory,
        CancellationToken cancellationToken)
    {
        var client = httpClientFactory.CreateClient("InternalNode");
        client.BaseAddress = new Uri(node.BaseUrl);
        client.Timeout = TimeSpan.FromSeconds(3);

        var response = await client.GetAsync("/internal/ping", cancellationToken);
        return response.IsSuccessStatusCode && ((await response.Content.ReadFromJsonAsync<PingResponse>())?.IsInitialized ?? false);
    }

    private async Task<bool> ProxyPingAsync(
        ClusterNodeInfo helperNode,
        ClusterNodeInfo targetNode,
        IHttpClientFactory httpClientFactory,
        CancellationToken cancellationToken)
    {
        var client = httpClientFactory.CreateClient("InternalNode");
        client.BaseAddress = new Uri(helperNode.BaseUrl);
        client.Timeout = TimeSpan.FromSeconds(3);

        var response = await client.PostAsJsonAsync("/internal/proxy-ping", new ProxyPingRequest
        {
            TargetNode = targetNode
        }, cancellationToken);

        return response.IsSuccessStatusCode && ((await response.Content.ReadFromJsonAsync<PingResponse>())?.IsInitialized ?? false);
    }

    private async Task MarkNodeOnlineAsync(ClusterNodeInfo node, INodeStateService nodeState, IGossipService gossipService)
    {
        if (node.Status == NodeStatus.Online)
        {
            return;
        }

        _logger.LogInformation("Node {NodeId} is reachable", node.NodeId);
        nodeState.UpdateNodeStatus(node.NodeId, NodeStatus.Online);
        nodeState.IncrementVersion();

        await gossipService.BroadcastNodeStatusChangeAsync(new List<NodeStatusChange>
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

    private async Task MarkNodeSuspectAsync(ClusterNodeInfo node, INodeStateService nodeState, IGossipService gossipService)
    {
        if (node.Status == NodeStatus.Suspect)
        {
            return;
        }

        _logger.LogWarning("Node {NodeId} is suspect", node.NodeId);
        nodeState.UpdateNodeStatus(node.NodeId, NodeStatus.Suspect);
        nodeState.IncrementVersion();

        await gossipService.BroadcastNodeStatusChangeAsync(new List<NodeStatusChange>
        {
            new()
            {
                NodeId = node.NodeId,
                NewStatus = NodeStatus.Suspect,
                BaseUrl = node.BaseUrl,
                HashPosition = node.HashPosition
            }
        });
    }
}
