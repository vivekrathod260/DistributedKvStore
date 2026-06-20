using DistributedKvStore.Shared.DTOs;
using DistributedKvStore.Shared.Models;

namespace DistributedKvStore.Node.Services;

public interface IReplicationService
{
    Task ReplicateToNodesAsync(ReplicationRequest request, List<ClusterNodeInfo> targetNodes);
}
