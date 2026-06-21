using DistributedKvStore.Shared.DTOs;
using DistributedKvStore.Shared.Models;

namespace DistributedKvStore.Node.Services.Interfaces;

public interface IReplicationService
{
    Task ReplicateToNodesAsync(ReplicationRequest request, List<ClusterNodeInfo> targetNodes);
}
