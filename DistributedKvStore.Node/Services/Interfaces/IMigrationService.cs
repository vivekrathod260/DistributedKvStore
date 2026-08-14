using DistributedKvStore.Shared.DTOs;

namespace DistributedKvStore.Node.Services.Interfaces;

public interface IMigrationService
{
    Task OnboardSelfAsync();
    Task RebalanceOnNodeJoinAsync(GossipMessage message);
}
