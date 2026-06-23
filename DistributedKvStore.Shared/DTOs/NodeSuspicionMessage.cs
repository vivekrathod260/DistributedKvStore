using DistributedKvStore.Shared.Models;

namespace DistributedKvStore.Shared.DTOs;

public class NodeSuspicionMessage
{
    public ClusterNodeInfo SuspectedNode { get; set; } = new();
    public List<Guid> Reporters { get; set; } = new();
}
