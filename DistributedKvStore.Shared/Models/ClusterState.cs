namespace DistributedKvStore.Shared.Models;

public class ClusterState
{
    public int ReplicationFactor { get; set; }
    public List<ClusterNodeInfo> Nodes { get; set; } = new();
    public bool IsInitialized { get; set; }
    public DateTime? InitializedAtUtc { get; set; }
}
