using DistributedKvStore.Shared.Models;

namespace DistributedKvStore.Shared.Hashing;

public interface IHashRing
{
    void BuildRing(IEnumerable<ClusterNodeInfo> nodes);
    ulong ComputeHash(string key);
    ClusterNodeInfo? FindPrimaryNode(string key);
    List<ClusterNodeInfo> FindReplicaNodes(string key, int replicationFactor);
    List<ClusterNodeInfo> GetResponsibleNodes(string key, int replicationFactor);
}
