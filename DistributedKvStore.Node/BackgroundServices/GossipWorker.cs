using System.Net.Http.Json;
using DistributedKvStore.Node.Services.Implementation.State;
using DistributedKvStore.Shared.DTOs;
using DistributedKvStore.Shared.Enums;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace DistributedKvStore.Node.BackgroundServices;

public class GossipWorker : BackgroundService
{
    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger<GossipWorker> _logger;
    private static readonly TimeSpan GossipInterval = TimeSpan.FromSeconds(10);

    public GossipWorker(IServiceProvider serviceProvider, ILogger<GossipWorker> logger)
    {
        _serviceProvider = serviceProvider;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await Task.Delay(TimeSpan.FromSeconds(8), stoppingToken);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await PerformGossipRoundAsync(stoppingToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Gossip round failed");
            }

            await Task.Delay(GossipInterval, stoppingToken);
        }
    }

    private async Task PerformGossipRoundAsync(CancellationToken cancellationToken)
    {
        using var scope = _serviceProvider.CreateScope();
        var nodeState = scope.ServiceProvider.GetRequiredService<INodeStateService>();
        var httpClientFactory = scope.ServiceProvider.GetRequiredService<IHttpClientFactory>();

        if (!nodeState.IsInitialized) return;

        var currentNode = nodeState.GetCurrentNode();
        var clusterState = nodeState.GetClusterState();

        // Randomly select a peer to exchange state with
        var peers = clusterState.Nodes
            .Where(n => n.NodeId != currentNode.NodeId && n.Status == NodeStatus.Online)
            .ToList();

        if (peers.Count == 0) return;

        var randomPeer = peers[Random.Shared.Next(peers.Count)];

        try
        {
            var client = httpClientFactory.CreateClient("InternalNode");
            client.BaseAddress = new Uri(randomPeer.BaseUrl);
            client.Timeout = TimeSpan.FromSeconds(5);

            var response = await client.GetFromJsonAsync<Shared.Models.ClusterState>(
                "/api/cluster/state", cancellationToken);

            if (response != null && response.Version > clusterState.Version)
            {
                nodeState.UpdateClusterState(response);
                _logger.LogInformation("Updated cluster state from peer {NodeId} to version {Version}",
                    randomPeer.NodeId, response.Version);
            }
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Failed to exchange gossip with {BaseUrl}", randomPeer.BaseUrl);
        }
    }
}
