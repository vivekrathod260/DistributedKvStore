using DistributedKvStore.Shared.Enums;

namespace DistributedKvStore.Shared.Models;

public class ClusterNodeInfo
{
    public Guid NodeId { get; set; }
    public string BaseUrl { get; set; } = string.Empty;
    public ulong HashPosition { get; set; }
    public NodeStatus Status { get; set; }
}
