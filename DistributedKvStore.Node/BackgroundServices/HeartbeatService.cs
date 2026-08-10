using System.Net.Http.Json;
using DistributedKvStore.Node.Services.Implementation.State;
using DistributedKvStore.Node.Services.Interfaces;
using DistributedKvStore.Shared.DTOs;
using DistributedKvStore.Shared.Enums;
using DistributedKvStore.Shared.Models;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace DistributedKvStore.Node.BackgroundServices;

public class HeartbeatService : BackgroundService
{
    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger<HeartbeatService> _logger;
    private static readonly TimeSpan HeartbeatInterval = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan FailedThreshold = TimeSpan.FromSeconds(30);
    private const int ProxyCount = 3;

    private readonly Dictionary<Guid, DateTime> _lastHeartbeat = new();

    public HeartbeatService(IServiceProvider serviceProvider, ILogger<HeartbeatService> logger)
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
                await CheckHeartbeatsAsync(stoppingToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Heartbeat check failed");
            }

            await Task.Delay(HeartbeatInterval, stoppingToken);
        }
    }

    private async Task CheckHeartbeatsAsync(CancellationToken cancellationToken)
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
            .Take(2)
            .ToList(); // Joining, Online, Suspect only

        foreach (var node in peers)
        {
            var reachable = await PingDirectlyAsync(node, httpClientFactory, cancellationToken);

            if(!reachable) // Proxy heartbeat check
            {
                reachable = await IsReachableViaProxiesAsync(node, currentNode, clusterState, httpClientFactory, cancellationToken);
            }

            if (reachable) // Online
            {
                _lastHeartbeat[node.NodeId] = DateTime.UtcNow;

                // If node was suspect, mark it back online
                if (node.Status == NodeStatus.Suspect)
                {
                    nodeState.UpdateNodeStatus(node.NodeId, NodeStatus.Online);
                    nodeState.TouchLastUpdated();
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

                continue;
            }
            // Both direct and indirect probes failed - mark suspect right away
            else if (node.Status != NodeStatus.Suspect && node.Status != NodeStatus.Failed)
            {
                _logger.LogWarning("Node {NodeId} marked as SUSPECT (direct and indirect heartbeat failed)", node.NodeId);

                nodeState.UpdateNodeStatus(node.NodeId, NodeStatus.Suspect);
                nodeState.TouchLastUpdated();

                await gossipService.BroadcastNodeStatusChangeAsync(new List<NodeStatusChange>
                {
                    new() { NodeId = node.NodeId, NewStatus = NodeStatus.Suspect }
                });
            }

            // // Escalate to failed once the node has been unreachable for too long
            // if (!_lastHeartbeat.TryGetValue(node.NodeId, out var lastSeen))
            // {
            //     _lastHeartbeat[node.NodeId] = DateTime.UtcNow;
            //     lastSeen = DateTime.UtcNow;
            // }

            // var timeSinceLastHeartbeat = DateTime.UtcNow - lastSeen;

            // if (timeSinceLastHeartbeat > FailedThreshold && node.Status != NodeStatus.Failed)
            // {
            //     _logger.LogWarning("Node {NodeId} marked as FAILED (no confirmed heartbeat for {Seconds}s)",
            //         node.NodeId, timeSinceLastHeartbeat.TotalSeconds);

            //     nodeState.UpdateNodeStatus(node.NodeId, NodeStatus.Failed);
            //     nodeState.TouchLastUpdated();

            //     await gossipService.BroadcastNodeStatusChangeAsync(new List<NodeStatusChange>
            //     {
            //         new() { NodeId = node.NodeId, NewStatus = NodeStatus.Failed }
            //     });
            // }
        }
    }

    private async Task<bool> PingDirectlyAsync(ClusterNodeInfo node, IHttpClientFactory httpClientFactory, CancellationToken cancellationToken)
    {
        try
        {
            var client = httpClientFactory.CreateClient("InternalNode");
            client.BaseAddress = new Uri(node.BaseUrl);
            client.Timeout = TimeSpan.FromSeconds(3);

            var response = await client.GetAsync("/internal/heartbeat", cancellationToken);
            return response.IsSuccessStatusCode;
        }
        catch
        {
            // Node unreachable
            return false;
        }
    }

    private async Task<bool> IsReachableViaProxiesAsync(
        ClusterNodeInfo target,
        ClusterNodeInfo currentNode,
        ClusterState clusterState,
        IHttpClientFactory httpClientFactory,
        CancellationToken cancellationToken)
    {
        var proxies = clusterState.Nodes
            .Where(n => n.NodeId != currentNode.NodeId && n.NodeId != target.NodeId && n.Status == NodeStatus.Online)
            .OrderBy(_ => Random.Shared.Next())
            .Take(ProxyCount)
            .ToList();

        if (proxies.Count == 0) return false;

        var tasks = proxies.Select(proxy => AskProxyToHeartbeatAsync(proxy, target, httpClientFactory, cancellationToken));
        var results = await Task.WhenAll(tasks);
        return results.Any(reachable => reachable);
    }

    private async Task<bool> AskProxyToHeartbeatAsync(
        ClusterNodeInfo proxy,
        ClusterNodeInfo target,
        IHttpClientFactory httpClientFactory,
        CancellationToken cancellationToken)
    {
        try
        {
            var client = httpClientFactory.CreateClient("InternalNode");
            client.BaseAddress = new Uri(proxy.BaseUrl);
            client.Timeout = TimeSpan.FromSeconds(3);

            var response = await client.PostAsJsonAsync("/internal/heartbeat-proxy", new ProxyHeartbeatRequest { TargetNodeId = target.NodeId }, cancellationToken);

            if (!response.IsSuccessStatusCode) return false;

            var result = await response.Content.ReadFromJsonAsync<ProxyHeartbeatResponse>(cancellationToken);
            return result?.Reachable ?? false;
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Indirect heartbeat via proxy {ProxyNodeId} for target {TargetNodeId} failed", proxy.NodeId, target.NodeId);
            return false;
        }
    }
}
