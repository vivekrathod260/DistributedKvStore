using System.Net.Http.Json;
using DistributedKvStore.Node.Services.Implementation.State;
using DistributedKvStore.Node.Services.Interfaces;
using DistributedKvStore.Shared.DTOs;
using DistributedKvStore.Shared.Enums;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace DistributedKvStore.Node.BackgroundServices;

public class HeartbeatService : BackgroundService
{
    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger<HeartbeatService> _logger;
    private static readonly TimeSpan HeartbeatInterval = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan SuspectThreshold = TimeSpan.FromSeconds(15);
    private static readonly TimeSpan FailedThreshold = TimeSpan.FromSeconds(30);

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
            .ToList();

        foreach (var peer in peers)
        {
            try
            {
                var client = httpClientFactory.CreateClient("InternalNode");
                client.BaseAddress = new Uri(peer.BaseUrl);
                client.Timeout = TimeSpan.FromSeconds(3);

                var response = await client.GetAsync("/internal/heartbeat", cancellationToken);
                if (response.IsSuccessStatusCode)
                {
                    _lastHeartbeat[peer.NodeId] = DateTime.UtcNow;

                    // If node was suspect, mark it back online
                    if (peer.Status == NodeStatus.Suspect)
                    {
                        nodeState.UpdateNodeStatus(peer.NodeId, NodeStatus.Online);
                        nodeState.IncrementVersion();
                        await gossipService.BroadcastNodeStatusChangeAsync(new List<NodeStatusChange>
                        {
                            new()
                            {
                                NodeId = peer.NodeId,
                                NewStatus = NodeStatus.Online,
                                BaseUrl = peer.BaseUrl,
                                HashPosition = peer.HashPosition
                            }
                        });
                    }

                    continue;
                }
            }
            catch
            {
                // Node unreachable
            }

            // Evaluate failure state
            if (!_lastHeartbeat.TryGetValue(peer.NodeId, out var lastSeen))
            {
                _lastHeartbeat[peer.NodeId] = DateTime.UtcNow;
                lastSeen = DateTime.UtcNow;
            }

            var timeSinceLastHeartbeat = DateTime.UtcNow - lastSeen;

            if (timeSinceLastHeartbeat > FailedThreshold && peer.Status != NodeStatus.Failed)
            {
                _logger.LogWarning("Node {NodeId} marked as FAILED (no heartbeat for {Seconds}s)",
                    peer.NodeId, timeSinceLastHeartbeat.TotalSeconds);

                nodeState.UpdateNodeStatus(peer.NodeId, NodeStatus.Failed);
                nodeState.IncrementVersion();

                await gossipService.BroadcastNodeStatusChangeAsync(new List<NodeStatusChange>
                {
                    new() { NodeId = peer.NodeId, NewStatus = NodeStatus.Failed }
                });
            }
            else if (timeSinceLastHeartbeat > SuspectThreshold && peer.Status == NodeStatus.Online)
            {
                _logger.LogWarning("Node {NodeId} marked as SUSPECT (no heartbeat for {Seconds}s)",
                    peer.NodeId, timeSinceLastHeartbeat.TotalSeconds);

                nodeState.UpdateNodeStatus(peer.NodeId, NodeStatus.Suspect);
                nodeState.IncrementVersion();

                await gossipService.BroadcastNodeStatusChangeAsync(new List<NodeStatusChange>
                {
                    new() { NodeId = peer.NodeId, NewStatus = NodeStatus.Suspect }
                });
            }
        }
    }
}
