using System.Net.Http.Json;
using DistributedKvStore.Shared.DTOs;
using DistributedKvStore.Shared.Enums;
using DistributedKvStore.Shared.Hashing;
using DistributedKvStore.Shared.Models;
using Microsoft.Extensions.Logging;

namespace DistributedKvStore.Client;

public class DistributedClient : IDistributedClient
{
    private readonly List<string> _seedNodes;
    private readonly HttpClient _httpClient;
    private readonly ILogger<DistributedClient>? _logger;

    private readonly ConsistentHashRing _hashRing;
    private ClusterState? _clusterState;
    private readonly object _stateLock = new();
    private readonly CancellationTokenSource _cts = new();
    private Task? _refreshTask;

    public DistributedClient(IEnumerable<string> seedNodes, HttpClient? httpClient = null, ILogger<DistributedClient>? logger = null)
    {
        _seedNodes = seedNodes.ToList();
        _httpClient = httpClient ?? new HttpClient { Timeout = TimeSpan.FromSeconds(5) };
        _logger = logger;

        _hashRing = new ConsistentHashRing();
        InitializeAsync().GetAwaiter().GetResult();
        _refreshTask = Task.Run(() => BackgroundRefreshAsync(_cts.Token));
    }

    private async Task InitializeAsync()
    {
        var shuffledSeeds = _seedNodes.OrderBy(_ => Random.Shared.Next()).ToList();

        foreach (var seed in shuffledSeeds)
        {
            try
            {
                var response = await _httpClient.GetFromJsonAsync<ClusterState>($"{seed}/api/cluster/state");
                if (response != null)
                {
                    lock (_stateLock)
                    {
                        _clusterState = response;
                        _hashRing.BuildRing(response.Nodes);
                    }
                    _logger?.LogInformation("Connected to cluster via {Seed}. LastUpdatedAt: {ClusterLastUpdatedAt}", seed, response.ClusterLastUpdatedAt);
                    return;
                }
            }
            catch (Exception ex)
            {
                _logger?.LogWarning(ex, "Failed to connect to seed {Seed}", seed);
            }
        }

        throw new InvalidOperationException("Unable to connect to any seed node. Cluster might be unavailable.");
    }

    public async Task<string?> GetAsync(string key)
    {
        var nodes = GetResponsibleNodes(key);
        if (nodes.Count == 0)
            throw new InvalidOperationException("No nodes available for the requested key.");

        foreach (var node in nodes)
        {
            try
            {
                var response = await _httpClient.GetAsync($"{node.BaseUrl}/api/data/{Uri.EscapeDataString(key)}");
                if (response.IsSuccessStatusCode)
                {
                    var result = await response.Content.ReadFromJsonAsync<KeyValueResponse>();
                    return result?.Value;
                }

                if (response.StatusCode == System.Net.HttpStatusCode.NotFound)
                    return null;
            }
            catch (Exception ex)
            {
                _logger?.LogWarning(ex, "Failed to read from node {NodeId}, trying next replica", node.NodeId);
            }
        }

        throw new InvalidOperationException($"Unable to read key '{key}' from any responsible node.");
    }

    public async Task PutAsync(string key, string value)
    {
        var nodes = GetResponsibleNodes(key);
        if (nodes.Count == 0)
            throw new InvalidOperationException("No nodes available for the requested key.");

        var request = new KeyValueRequest { Key = key, Value = value };

        foreach (var node in nodes)
        {
            try
            {
                var response = await _httpClient.PostAsJsonAsync($"{node.BaseUrl}/api/data", request);
                if (response.IsSuccessStatusCode)
                    return;
            }
            catch (Exception ex)
            {
                _logger?.LogWarning(ex, "Failed to write to node {NodeId}, trying next replica", node.NodeId);
            }
        }

        throw new InvalidOperationException($"Unable to write key '{key}' to any responsible node.");
    }

    public async Task UpdateAsync(string key, string value)
    {
        var nodes = GetResponsibleNodes(key);
        if (nodes.Count == 0)
            throw new InvalidOperationException("No nodes available for the requested key.");

        var request = new KeyValueRequest { Key = key, Value = value };

        foreach (var node in nodes)
        {
            try
            {
                var response = await _httpClient.PutAsJsonAsync($"{node.BaseUrl}/api/data", request);
                if (response.IsSuccessStatusCode)
                    return;
            }
            catch (Exception ex)
            {
                _logger?.LogWarning(ex, "Failed to update on node {NodeId}, trying next replica", node.NodeId);
            }
        }

        throw new InvalidOperationException($"Unable to update key '{key}' on any responsible node.");
    }

    public async Task DeleteAsync(string key)
    {
        var nodes = GetResponsibleNodes(key);
        if (nodes.Count == 0)
            throw new InvalidOperationException("No nodes available for the requested key.");

        foreach (var node in nodes)
        {
            try
            {
                var response = await _httpClient.DeleteAsync($"{node.BaseUrl}/api/data/{Uri.EscapeDataString(key)}");
                if (response.IsSuccessStatusCode)
                    return;
            }
            catch (Exception ex)
            {
                _logger?.LogWarning(ex, "Failed to delete on node {NodeId}, trying next replica", node.NodeId);
            }
        }

        throw new InvalidOperationException($"Unable to delete key '{key}' on any responsible node.");
    }

    private List<ClusterNodeInfo> GetResponsibleNodes(string key)
    {
        lock (_stateLock)
        {
            if (_clusterState == null)
                return new List<ClusterNodeInfo>();

            return _hashRing.GetResponsibleNodes(key, _clusterState.ReplicationFactor)
                .Where(n => n.Status == NodeStatus.Online || n.Status == NodeStatus.Joining)
                .ToList();
        }
    }

    private async Task BackgroundRefreshAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(TimeSpan.FromSeconds(30), cancellationToken);
                await RefreshTopologyAsync();
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger?.LogWarning(ex, "Background topology refresh failed");
            }
        }
    }

    private async Task RefreshTopologyAsync()
    {
        List<ClusterNodeInfo> nodes;
        lock (_stateLock)
        {
            if (_clusterState == null) return;
            nodes = _clusterState.Nodes
                .Where(n => n.Status == NodeStatus.Online)
                .OrderBy(_ => Random.Shared.Next())
                .ToList();
        }

        foreach (var node in nodes)
        {
            try
            {
                var response = await _httpClient.GetFromJsonAsync<ClusterState>($"{node.BaseUrl}/api/cluster/state");
                if (response != null)
                {
                    lock (_stateLock)
                    {
                        if (response.ClusterLastUpdatedAt > (_clusterState?.ClusterLastUpdatedAt ?? DateTime.MinValue))
                        {
                            _clusterState = response;
                            _hashRing.BuildRing(response.Nodes);
                            _logger?.LogInformation("Topology refreshed to timestamp {ClusterLastUpdatedAt}", response.ClusterLastUpdatedAt);
                        }
                    }
                    return;
                }
            }
            catch
            {
                // Try next node
            }
        }

        // Fallback to seed nodes
        foreach (var seed in _seedNodes)
        {
            try
            {
                var response = await _httpClient.GetFromJsonAsync<ClusterState>($"{seed}/api/cluster/state");
                if (response != null)
                {
                    lock (_stateLock)
                    {
                        if (response.ClusterLastUpdatedAt > (_clusterState?.ClusterLastUpdatedAt ?? DateTime.MinValue))
                        {
                            _clusterState = response;
                            _hashRing.BuildRing(response.Nodes);
                        }
                    }
                    return;
                }
            }
            catch
            {
                // Try next seed
            }
        }
    }

    public void Dispose()
    {
        _httpClient.Dispose();
        _cts.Cancel();
        _cts.Dispose();
        _refreshTask?.Wait(TimeSpan.FromSeconds(5));
    }
}
