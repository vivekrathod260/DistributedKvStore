using DistributedKvStore.Shared.Models;

namespace DistributedKvStore.Shared.DTOs;

public class ProxyPingRequest
{
    public ClusterNodeInfo TargetNode { get; set; }
}
