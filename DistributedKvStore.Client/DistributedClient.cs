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

    // Init class
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
                    _logger?.LogInformation("Connected to cluster via {Seed}. {NodeCount} nodes known", seed, response.Nodes.Count);
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

    // Get
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

    // Add
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

    // Update
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

    // Delete
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


    // Background refresh task to periodically update the cluster topology
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
                    MergeClusterState(response);
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
                    MergeClusterState(response);
                    return;
                }
            }
            catch
            {
                // Try next seed
            }
        }
    }



    // ######### Cluster management methods ##########
    public async Task<ClusterState> GetClusterStateAsync()
    {
        var response = await ExecuteOnAnyNodeAsync(target => _httpClient.GetAsync($"{target}/api/cluster/state"), "get cluster state");
        var state = await response.Content.ReadFromJsonAsync<ClusterState>()
            ?? throw new InvalidOperationException("Get cluster state returned no state.");
        MergeClusterState(state);
        return state;
    }

    public async Task<ClusterState> GetClusterStateAsync(string nodeBaseUrl)
    {
        var response = await _httpClient.GetAsync($"{nodeBaseUrl}/api/cluster/state");
        response.EnsureSuccessStatusCode();
        var state = await response.Content.ReadFromJsonAsync<ClusterState>();
        return state ?? throw new InvalidOperationException($"Node at '{nodeBaseUrl}' returned no cluster state.");
    }

    public async Task<ClusterState> StartClusterAsync()
    {
        var response = await ExecuteOnAnyNodeAsync(baseUrl => _httpClient.PostAsync($"{baseUrl}/api/cluster/start", null), "start cluster");
        var state = await response.Content.ReadFromJsonAsync<ClusterState>()
            ?? throw new InvalidOperationException("Start cluster returned no state.");
        MergeClusterState(state);
        return state;
    }

    public async Task ShutdownClusterAsync()
    {
        await ExecuteOnAnyNodeAsync(baseUrl => _httpClient.PostAsync($"{baseUrl}/api/cluster/shutdown", null), "shutdown cluster");
    }

    public async Task<ClusterState> AddNodeAsync(string baseUrl, Guid? nodeId = null)
    {
        var request = new AddNodeRequest { BaseUrl = baseUrl, NodeId = nodeId };
        var response = await ExecuteOnAnyNodeAsync(
            target => _httpClient.PostAsJsonAsync($"{target}/api/cluster/add-node", request),
            "add node");
        var state = await response.Content.ReadFromJsonAsync<ClusterState>()
            ?? throw new InvalidOperationException("Add node returned no state.");
        MergeClusterState(state);
        return state;
    }

    public async Task<ClusterState> RemoveNodeAsync(Guid nodeId)
    {
        var request = new RemoveNodeRequest { NodeId = nodeId };
        var response = await ExecuteOnAnyNodeAsync(
            target => _httpClient.PostAsJsonAsync($"{target}/api/cluster/remove-node", request),
            "remove node");
        var state = await response.Content.ReadFromJsonAsync<ClusterState>()
            ?? throw new InvalidOperationException("Remove node returned no state.");
        MergeClusterState(state);
        return state;
    }

    public async Task<ClusterState> SetReplicationFactorAsync(int replicationFactor)
    {
        var request = new SetReplicationFactorRequest { ReplicationFactor = replicationFactor };
        var response = await ExecuteOnAnyNodeAsync(
            baseUrl => _httpClient.PostAsJsonAsync($"{baseUrl}/api/cluster/set-replication-factor", request),
            "set replication factor");
        var state = await response.Content.ReadFromJsonAsync<ClusterState>()
            ?? throw new InvalidOperationException("Set replication factor returned no state.");
        MergeClusterState(state);
        return state;
    }

    public async Task<HeartbeatResponse?> CheckHealthAsync(string nodeBaseUrl)
    {
        try
        {
            var response = await _httpClient.GetAsync($"{nodeBaseUrl}/internal/heartbeat");
            if (!response.IsSuccessStatusCode)
                return null;

            return await response.Content.ReadFromJsonAsync<HeartbeatResponse>();
        }
        catch (Exception ex)
        {
            _logger?.LogWarning(ex, "Health check failed for node at {BaseUrl}", nodeBaseUrl);
            return null;
        }
    }



    // ############## Util methods ##############
    private async Task<HttpResponseMessage> ExecuteOnAnyNodeAsync(Func<string, Task<HttpResponseMessage>> sendRequest, string operation)
    {
        var candidates = GetCandidateBaseUrls();
        if (candidates.Count == 0) throw new InvalidOperationException("No known cluster nodes available.");

        Exception? lastException = null;
        foreach (var baseUrl in candidates)
        {
            try
            {
                var response = await sendRequest(baseUrl);
                if (response.IsSuccessStatusCode) return response;

                _logger?.LogWarning("Request to {BaseUrl} to {Operation} failed with status {StatusCode}", baseUrl, operation, response.StatusCode);
            }
            catch (Exception ex)
            {
                lastException = ex;
                _logger?.LogWarning(ex, "Failed to {Operation} via {BaseUrl}, trying next node", operation, baseUrl);
            }
        }

        throw new InvalidOperationException($"Unable to {operation} via any known node.", lastException);
    }

    private List<string> GetCandidateBaseUrls()
    {
        lock (_stateLock)
        {
            var candidates = new List<string>();
            if (_clusterState != null)
            {
                candidates.AddRange(_clusterState.Nodes
                    .Where(n => n.Status == NodeStatus.Online || n.Status == NodeStatus.Joining)
                    .Select(n => n.BaseUrl));
            }
            candidates.AddRange(_seedNodes);
            return candidates.Distinct().OrderBy(_ => Random.Shared.Next()).ToList();
        }
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

    private void MergeClusterState(ClusterState incoming)
    {
        lock (_stateLock)
        {
            _clusterState = incoming;
            _hashRing.BuildRing(incoming.Nodes);
            _logger?.LogInformation("Topology refreshed, {NodeCount} nodes known", incoming.Nodes.Count);
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
