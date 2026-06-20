using DistributedKvStore.Shared.DTOs;

namespace DistributedKvStore.Node.Services;

public interface IGossipService
{
    Task ProcessGossipMessageAsync(GossipMessage message);
    Task BroadcastGossipAsync(GossipMessage message);
    Task BroadcastNodeStatusChangeAsync(List<NodeStatusChange> changes);
}
