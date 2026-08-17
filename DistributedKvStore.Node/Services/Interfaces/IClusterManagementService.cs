using DistributedKvStore.Shared.DTOs;
using DistributedKvStore.Shared.Models;

namespace DistributedKvStore.Node.Services.Interfaces;

public interface IClusterManagementService
{
    Task<ClusterState> StartClusterAsync();
    Task<ClusterState> AddNodeAsync(string baseUrl, Guid? nodeId = null);
    Task<ClusterState> RemoveNodeAsync(Guid nodeId);
    Task<ClusterState> RestartNodeAsync(Guid nodeId);
    Task SetReplicationFactorAsync(int factor);
    Task ShutdownClusterAsync();
    Task ShutdownLocalNodeAsync();
    Task<ClusterState> GetClusterStateAsync();
}
