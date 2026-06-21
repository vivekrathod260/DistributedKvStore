using DistributedKvStore.Shared.Models;

namespace DistributedKvStore.Node.Services.Interfaces;

public interface IMigrationService
{
    Task MigrateToNewNodeAsync(ClusterNodeInfo newNode);
    Task MigrateFromLeavingNodeAsync(Guid leavingNodeId);
}
