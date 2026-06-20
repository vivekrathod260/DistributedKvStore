using System.Net.Http.Json;
using DistributedKvStore.Shared.Enums;
using DistributedKvStore.Shared.Models;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace DistributedKvStore.Node.BackgroundServices;

public class ClusterTopologyRefreshService : BackgroundService
{
    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger<ClusterTopologyRefreshService> _logger;
    private static readonly TimeSpan RefreshInterval = TimeSpan.FromSeconds(30);

    public ClusterTopologyRefreshService(IServiceProvider serviceProvider, ILogger<ClusterTopologyRefreshService> logger)
    {
        _serviceProvider = serviceProvider;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await Task.Delay(TimeSpan.FromSeconds(10), stoppingToken);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await RefreshTopologyAsync(stoppingToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Cluster topology refresh failed");
            }

            await Task.Delay(RefreshInterval, stoppingToken);
        }
    }

    private async Task RefreshTopologyAsync(CancellationToken cancellationToken)
    {
        using var scope = _serviceProvider.CreateScope();
        var nodeState = scope.ServiceProvider.GetRequiredService<Services.INodeStateService>();
        var httpClientFactory = scope.ServiceProvider.GetRequiredService<IHttpClientFactory>();

        if (!nodeState.IsInitialized) return;

        var currentNode = nodeState.GetCurrentNode();
        var clusterState = nodeState.GetClusterState();

        var peers = clusterState.Nodes
            .Where(n => n.NodeId != currentNode.NodeId && n.Status == NodeStatus.Online)
            .OrderBy(_ => Random.Shared.Next())
            .ToList();

        foreach (var peer in peers)
        {
            try
            {
                var client = httpClientFactory.CreateClient("InternalNode");
                client.BaseAddress = new Uri(peer.BaseUrl);
                client.Timeout = TimeSpan.FromSeconds(5);

                var response = await client.GetFromJsonAsync<ClusterState>(
                    "/api/cluster/state", cancellationToken);

                if (response != null && response.Version > clusterState.Version)
                {
                    nodeState.UpdateClusterState(response);
                    _logger.LogInformation("Topology refreshed from {NodeId}. New version: {Version}",
                        peer.NodeId, response.Version);
                    return;
                }
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Could not refresh topology from {BaseUrl}", peer.BaseUrl);
            }
        }
    }
}
