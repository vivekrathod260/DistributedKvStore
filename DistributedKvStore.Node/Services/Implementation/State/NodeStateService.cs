using DistributedKvStore.Node.Persistence;
using DistributedKvStore.Node.Services.Interfaces;
using DistributedKvStore.Shared.Enums;
using DistributedKvStore.Shared.Hashing;
using DistributedKvStore.Shared.Models;

namespace DistributedKvStore.Node.Services.Implementation.State;

public interface INodeStateService
{
    ClusterState GetClusterState();
    ClusterNodeInfo GetCurrentNode();
    void UpdateClusterState(ClusterState state);
    void UpdateNodeStatus(Guid nodeId, NodeStatus status);
    void AddNode(ClusterNodeInfo node);
    Task RemoveNode(Guid nodeId);
    void TouchLastSeen(Guid nodeId);
    DateTime? GetLastSeenUtc(Guid nodeId);
    int GetReplicationFactor();
    void SetReplicationFactor(int factor);
    IHashRing GetHashRing();
    bool IsInitialized { get; }
    DateTime? InitializedAtUtc { get; }
    void MarkInitialized();
    ClusterSnapshot GetSnapshot();
    void RestoreFromSnapshot(ClusterSnapshot snapshot);
}

public class NodeStateService : INodeStateService
{
    private readonly ReaderWriterLockSlim _lock = new();
    private ClusterState _clusterState;
    private readonly ClusterNodeInfo _currentNode;
    private readonly ConsistentHashRing _hashRing;
    private readonly Dictionary<Guid, DateTime> _lastSeenUtc = new();
    private bool _isInitialized;
    private readonly IRebalancingService? _rebalancingService;
    private readonly IGossipService? _gossipService;
    private DateTime? _initializedAtUtc;
    private static readonly TimeSpan FailedThreshold = TimeSpan.FromSeconds(30);

    public bool IsInitialized
    {
        get
        {
            _lock.EnterReadLock();
            try { return _isInitialized; }
            finally { _lock.ExitReadLock(); }
        }
    }

    public DateTime? InitializedAtUtc
    {
        get
        {
            _lock.EnterReadLock();
            try { return _initializedAtUtc; }
            finally { _lock.ExitReadLock(); }
        }
    }

    public NodeStateService(Guid nodeId, string baseUrl, ulong hashPosition, IRebalancingService? rebalancingService = null, IGossipService? gossipService = null)
    {
        _rebalancingService = rebalancingService;
        _gossipService = gossipService;
        _currentNode = new ClusterNodeInfo
        {
            NodeId = nodeId,
            BaseUrl = baseUrl,
            HashPosition = hashPosition,
            Status = NodeStatus.Online
        };

        _clusterState = new ClusterState
        {
            ReplicationFactor = 0,
            Nodes = new List<ClusterNodeInfo> { _currentNode }
        };

        _hashRing = new ConsistentHashRing();
        _hashRing.BuildRing(_clusterState.Nodes);
        _lastSeenUtc[nodeId] = DateTime.UtcNow;
    }

    public void MarkInitialized()
    {
        _lock.EnterWriteLock();
        try
        {
            if (_isInitialized) return;
            _isInitialized = true;
            _initializedAtUtc = DateTime.UtcNow;
        }
        finally { _lock.ExitWriteLock(); }
    }

    public ClusterState GetClusterState()
    {
        _lock.EnterWriteLock();
        try
        {
            PruneExpiredSuspects();

            return new ClusterState
            {
                ReplicationFactor = _clusterState.ReplicationFactor,
                Nodes = _clusterState.Nodes.Select(n => new ClusterNodeInfo
                {
                    NodeId = n.NodeId,
                    BaseUrl = n.BaseUrl,
                    HashPosition = n.HashPosition,
                    Status = n.Status
                }).ToList()
            };
        }
        finally { _lock.ExitWriteLock(); }
    }

    // Lazily proposes removal of Suspect nodes nobody has confirmed alive for too long.
    private void PruneExpiredSuspects()
    {
        var now = DateTime.UtcNow;
        var expired = _clusterState.Nodes
            .Where(n => n.Status == NodeStatus.Suspect && (!_lastSeenUtc.TryGetValue(n.NodeId, out var lastSeen) || now - lastSeen > FailedThreshold))
            .ToList();

        if (expired.Count == 0)
            return;

        foreach (var offlineNode in expired)
        {
            var successor = _hashRing.FindSuccessorNode(offlineNode, _clusterState.Nodes);
            if (successor?.NodeId == _currentNode.NodeId)
            {
                Task.Run(async () =>
                {
                    await RemoveNode(offlineNode.NodeId);
                });
                
                if (_gossipService != null)
                {
                    _ = _gossipService.BroadcastNodeRemovalProposalAsync(offlineNode.NodeId, _currentNode.NodeId);
                }
            }
        }
    }

    public ClusterNodeInfo GetCurrentNode()
    {
        _lock.EnterReadLock();
        try
        {
            return new ClusterNodeInfo
            {
                NodeId = _currentNode.NodeId,
                BaseUrl = _currentNode.BaseUrl,
                HashPosition = _currentNode.HashPosition,
                Status = _currentNode.Status
            };
        }
        finally { _lock.ExitReadLock(); }
    }

    // Merges an incoming snapshot into the local view per-member
    public void UpdateClusterState(ClusterState state)
    {
        _lock.EnterWriteLock();
        try
        {
            var currNodeInfo = state.Nodes.FirstOrDefault(n => n.BaseUrl == _currentNode.BaseUrl);
            if (currNodeInfo != null)
            {
                _currentNode.NodeId = currNodeInfo.NodeId;
                _currentNode.HashPosition = currNodeInfo.HashPosition;
                _currentNode.Status = currNodeInfo.Status;
            }
            else
            {
                state.Nodes.Add(new ClusterNodeInfo
                {
                    NodeId = _currentNode.NodeId,
                    BaseUrl = _currentNode.BaseUrl,
                    HashPosition = _currentNode.HashPosition,
                    Status = _currentNode.Status
                });
            }

            _clusterState = state;
            _hashRing.BuildRing(_clusterState.Nodes);
            SyncLastSeenTracking();
        }
        finally { _lock.ExitWriteLock(); }
    }

    public void UpdateNodeStatus(Guid nodeId, NodeStatus status)
    {
        _lock.EnterWriteLock();
        try
        {
            var node = _clusterState.Nodes.FirstOrDefault(n => n.NodeId == nodeId);
            if (node != null)
            {
                node.Status = status;
                _hashRing.BuildRing(_clusterState.Nodes);
            }
        }
        finally { _lock.ExitWriteLock(); }
    }

    public void AddNode(ClusterNodeInfo node)
    {
        _lock.EnterWriteLock();
        try
        {
            if (_clusterState.Nodes.All(n => n.NodeId != node.NodeId))
            {
                _clusterState.Nodes.Add(node);
                _hashRing.BuildRing(_clusterState.Nodes);
                SyncLastSeenTracking();
            }
        }
        finally { _lock.ExitWriteLock(); }
    }

    public async Task RemoveNode(Guid nodeId)
    {
        var targetNode = _clusterState.Nodes.FirstOrDefault(n => n.NodeId == nodeId);
        if (targetNode != null && _rebalancingService != null)
        {
            await _rebalancingService.RebalanceOnNodeRemovalAsync(targetNode);
        }

        _lock.EnterWriteLock();
        try
        {
            _clusterState.Nodes.RemoveAll(n => n.NodeId == nodeId);
            _hashRing.BuildRing(_clusterState.Nodes);
            _lastSeenUtc.Remove(nodeId);
        }
        finally { _lock.ExitWriteLock(); }
    }

    // Keeps the local last-seen tracking dictionary in sync with whatever node IDs are
    // currently known, whether they arrived via a granular AddNode or a wholesale
    // UpdateClusterState snapshot replace. Must be called while holding the write lock.
    private void SyncLastSeenTracking()
    {
        var now = DateTime.UtcNow;
        var knownIds = new HashSet<Guid>(_clusterState.Nodes.Select(n => n.NodeId));

        foreach (var nodeId in knownIds)
        {
            if (!_lastSeenUtc.ContainsKey(nodeId))
            {
                _lastSeenUtc[nodeId] = now;
            }
        }

        var staleIds = _lastSeenUtc.Keys.Where(id => !knownIds.Contains(id)).ToList();
        foreach (var staleId in staleIds)
        {
            _lastSeenUtc.Remove(staleId);
        }
    }

    public void TouchLastSeen(Guid nodeId)
    {
        _lock.EnterWriteLock();
        try { _lastSeenUtc[nodeId] = DateTime.UtcNow; }
        finally { _lock.ExitWriteLock(); }
    }

    public DateTime? GetLastSeenUtc(Guid nodeId)
    {
        _lock.EnterReadLock();
        try { return _lastSeenUtc.TryGetValue(nodeId, out var lastSeen) ? lastSeen : null; }
        finally { _lock.ExitReadLock(); }
    }

    public int GetReplicationFactor()
    {
        _lock.EnterReadLock();
        try { return _clusterState.ReplicationFactor; }
        finally { _lock.ExitReadLock(); }
    }

    public void SetReplicationFactor(int factor)
    {
        _lock.EnterWriteLock();
        try { _clusterState.ReplicationFactor = factor; }
        finally { _lock.ExitWriteLock(); }
    }

    public IHashRing GetHashRing()
    {
        return _hashRing;
    }

    public ClusterSnapshot GetSnapshot()
    {
        _lock.EnterReadLock();
        try
        {
            return new ClusterSnapshot
            {
                State = new ClusterState
                {
                    ReplicationFactor = _clusterState.ReplicationFactor,
                    Nodes = _clusterState.Nodes.Select(n => new ClusterNodeInfo
                    {
                        NodeId = n.NodeId,
                        BaseUrl = n.BaseUrl,
                        HashPosition = n.HashPosition,
                        Status = n.Status
                    }).ToList()
                },
                IsInitialized = _isInitialized,
                InitializedAtUtc = _initializedAtUtc
            };
        }
        finally { _lock.ExitReadLock(); }
    }

    // Restores a previously saved snapshot
    public void RestoreFromSnapshot(ClusterSnapshot snapshot)
    {
        _lock.EnterWriteLock();
        try
        {
            _clusterState = snapshot.State;
            _hashRing.BuildRing(_clusterState.Nodes);
            SyncLastSeenTracking();
            _isInitialized = snapshot.IsInitialized;
            _initializedAtUtc = DateTime.UtcNow;
        }
        finally { _lock.ExitWriteLock(); }
    }
}
