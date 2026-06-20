namespace DistributedKvStore.Shared.DTOs;

public class HeartbeatResponse
{
    public Guid NodeId { get; set; }
    public long ClusterVersion { get; set; }
    public DateTime TimestampUtc { get; set; }
}
