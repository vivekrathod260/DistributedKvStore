using DistributedKvStore.Shared.DTOs;
using DistributedKvStore.Shared.Models;

namespace DistributedKvStore.Node.Services.Interfaces;

public interface IMigrationService
{
    Task OnboardSelfAsync();
    Task ProcessGossipMessageAsync(GossipMessage message);
    Task RebalanceOnNodeRemovalAsync(ClusterNodeInfo offlineNode);
}
