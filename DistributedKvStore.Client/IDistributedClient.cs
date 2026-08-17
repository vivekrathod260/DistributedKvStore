using DistributedKvStore.Shared.DTOs;
using DistributedKvStore.Shared.Models;

namespace DistributedKvStore.Client;

public interface IDistributedClient : IDisposable
{
    Task<string?> GetAsync(string key);
    Task PutAsync(string key, string value);
    Task UpdateAsync(string key, string value);
    Task DeleteAsync(string key);

    Task<ClusterState> StartClusterAsync();
    Task ShutdownClusterAsync();
    Task<ClusterState> SetReplicationFactorAsync(int replicationFactor);
    Task<ClusterState> AddNodeAsync(string baseUrl, Guid? nodeId = null);
    Task<ClusterState> RemoveNodeAsync(Guid nodeId);
    Task<ClusterState> GetClusterStateAsync();
    Task<ClusterState> GetClusterStateAsync(string nodeBaseUrl);
    Task<HeartbeatResponse?> CheckHealthAsync(string nodeBaseUrl);
}
