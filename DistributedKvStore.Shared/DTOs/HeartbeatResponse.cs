namespace DistributedKvStore.Shared.DTOs;

public class HeartbeatResponse
{
    public Guid NodeId { get; set; }
    public DateTime ClusterLastUpdatedAt { get; set; }
    public DateTime TimestampUtc { get; set; }
}

public class ProxyHeartbeatRequest
{
    public Guid TargetNodeId { get; set; }
}

public class ProxyHeartbeatResponse
{
    public bool Reachable { get; set; }
}
