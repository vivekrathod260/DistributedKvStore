namespace DistributedKvStore.Shared.DTOs;

public class PingResponse
{
    public Guid NodeId { get; set; }
    public bool IsInitialized { get; set; } = false;
    public long ClusterVersion { get; set; }
    public DateTime TimestampUtc { get; set; }
}
