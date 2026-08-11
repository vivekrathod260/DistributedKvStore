using DistributedKvStore.Shared.DTOs;

namespace DistributedKvStore.Node.Services.Interfaces;

public interface IGossipService
{
    Task ProcessGossipMessageAsync(GossipMessage message);
    Task BroadcastGossipAsync(GossipMessage message);
    Task BroadcastNodeStatusChangeAsync(List<NodeStatusChange> changes);
    Task BroadcastNodeSuspicionAsync(Guid suspectedNodeId, List<Guid> verifierNodeIds);
    Task BroadcastNodeJoinAsync(Guid nodeId, string baseUrl);
}
