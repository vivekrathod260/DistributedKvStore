using DistributedKvStore.Shared.Models;

namespace DistributedKvStore.Shared.Models;

public class ClusterSnapshot
{
    public ClusterState State { get; set; } = new();
    public bool IsInitialized { get; set; }
    public DateTime? InitializedAtUtc { get; set; }
}
