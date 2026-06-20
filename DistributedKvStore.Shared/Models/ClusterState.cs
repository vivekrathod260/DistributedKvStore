namespace DistributedKvStore.Shared.Models;

public class ClusterState
{
    public long Version { get; set; }
    public int ReplicationFactor { get; set; }
    public List<ClusterNodeInfo> Nodes { get; set; } = new();
}
