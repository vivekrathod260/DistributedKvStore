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
    void RemoveNode(Guid nodeId);
    long IncrementVersion();
    int GetReplicationFactor();
    void SetReplicationFactor(int factor);
    IHashRing GetHashRing();
    bool IsInitialized { get; }
    void MarkInitialized();
}

public class NodeStateService : INodeStateService
{
    private readonly ReaderWriterLockSlim _lock = new();
    private ClusterState _clusterState;
    private readonly ClusterNodeInfo _currentNode;
    private readonly ConsistentHashRing _hashRing;
    private bool _isInitialized;

    public bool IsInitialized
    {
        get
        {
            _lock.EnterReadLock();
            try { return _isInitialized; }
            finally { _lock.ExitReadLock(); }
        }
    }

    public NodeStateService(Guid nodeId, string baseUrl, ulong hashPosition)
    {
        _currentNode = new ClusterNodeInfo
        {
            NodeId = nodeId,
            BaseUrl = baseUrl,
            HashPosition = hashPosition,
            Status = NodeStatus.Online
        };

        _clusterState = new ClusterState
        {
            Version = 1,
            ReplicationFactor = 3,
            Nodes = new List<ClusterNodeInfo> { _currentNode }
        };

        _hashRing = new ConsistentHashRing();
        _hashRing.BuildRing(_clusterState.Nodes);
    }

    public void MarkInitialized()
    {
        _lock.EnterWriteLock();
        try { _isInitialized = true; }
        finally { _lock.ExitWriteLock(); }
    }

    public ClusterState GetClusterState()
    {
        _lock.EnterReadLock();
        try
        {
            return new ClusterState
            {
                Version = _clusterState.Version,
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
        finally { _lock.ExitReadLock(); }
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

    public void UpdateClusterState(ClusterState state)
    {
        _lock.EnterWriteLock();
        try
        {
            if (state.Version > _clusterState.Version)
            {
                _clusterState = state;
                _hashRing.BuildRing(_clusterState.Nodes);
            }
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
            }
        }
        finally { _lock.ExitWriteLock(); }
    }

    public void RemoveNode(Guid nodeId)
    {
        _lock.EnterWriteLock();
        try
        {
            _clusterState.Nodes.RemoveAll(n => n.NodeId == nodeId);
            _hashRing.BuildRing(_clusterState.Nodes);
        }
        finally { _lock.ExitWriteLock(); }
    }

    public long IncrementVersion()
    {
        _lock.EnterWriteLock();
        try
        {
            _clusterState.Version++;
            return _clusterState.Version;
        }
        finally { _lock.ExitWriteLock(); }
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
}
