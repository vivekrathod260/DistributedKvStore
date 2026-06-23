using DistributedKvStore.Shared.DTOs;
using DistributedKvStore.Shared.Models;

namespace DistributedKvStore.Node.Services.Interfaces;

public interface IGossipService
{
    Task ProcessGossipMessageAsync(GossipMessage message);
    Task BroadcastGossipAsync(GossipMessage message);
    Task BroadcastNodeStatusChangeAsync(List<NodeStatusChange> changes);
    Task BroadcastNodeSuspicionAsync(ClusterNodeInfo suspectedNode);
}
