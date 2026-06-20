using System.Security.Cryptography;
using System.Text;
using DistributedKvStore.Shared.Enums;
using DistributedKvStore.Shared.Models;

namespace DistributedKvStore.Shared.Hashing;

public class ConsistentHashRing : IHashRing
{
    private readonly object _lock = new();
    private List<ClusterNodeInfo> _sortedNodes = new();

    public void BuildRing(IEnumerable<ClusterNodeInfo> nodes)
    {
        lock (_lock)
        {
            _sortedNodes = nodes
                .Where(n => n.Status == NodeStatus.Online || n.Status == NodeStatus.Joining)
                .OrderBy(n => n.HashPosition)
                .ToList();
        }
    }

    public ulong ComputeHash(string key)
    {
        var hashBytes = SHA256.HashData(Encoding.UTF8.GetBytes(key));
        return BitConverter.ToUInt64(hashBytes, 0);
    }

    public ClusterNodeInfo? FindPrimaryNode(string key)
    {
        var hash = ComputeHash(key);
        return FindPrimaryNodeByHash(hash);
    }

    public ClusterNodeInfo? FindPrimaryNodeByHash(ulong hash)
    {
        lock (_lock)
        {
            if (_sortedNodes.Count == 0)
                return null;

            foreach (var node in _sortedNodes)
            {
                if (node.HashPosition >= hash)
                    return node;
            }

            // Wrap around - first node owns keys beyond last position
            return _sortedNodes[0];
        }
    }

    public List<ClusterNodeInfo> FindReplicaNodes(string key, int replicationFactor)
    {
        var hash = ComputeHash(key);
        return FindReplicaNodesByHash(hash, replicationFactor);
    }

    public List<ClusterNodeInfo> FindReplicaNodesByHash(ulong hash, int replicationFactor)
    {
        lock (_lock)
        {
            if (_sortedNodes.Count == 0)
                return new List<ClusterNodeInfo>();

            var primary = FindPrimaryNodeByHash(hash);
            if (primary == null)
                return new List<ClusterNodeInfo>();

            var primaryIndex = _sortedNodes.IndexOf(primary);
            var replicas = new List<ClusterNodeInfo>();

            var replicaCount = Math.Min(replicationFactor - 1, _sortedNodes.Count - 1);
            for (int i = 1; i <= replicaCount; i++)
            {
                var idx = (primaryIndex + i) % _sortedNodes.Count;
                replicas.Add(_sortedNodes[idx]);
            }

            return replicas;
        }
    }

    public List<ClusterNodeInfo> GetResponsibleNodes(string key, int replicationFactor)
    {
        var hash = ComputeHash(key);
        lock (_lock)
        {
            if (_sortedNodes.Count == 0)
                return new List<ClusterNodeInfo>();

            var primary = FindPrimaryNodeByHash(hash);
            if (primary == null)
                return new List<ClusterNodeInfo>();

            var result = new List<ClusterNodeInfo> { primary };
            var replicas = FindReplicaNodesByHash(hash, replicationFactor);
            result.AddRange(replicas);

            return result;
        }
    }

    public List<ClusterNodeInfo> GetSortedNodes()
    {
        lock (_lock)
        {
            return new List<ClusterNodeInfo>(_sortedNodes);
        }
    }
}
