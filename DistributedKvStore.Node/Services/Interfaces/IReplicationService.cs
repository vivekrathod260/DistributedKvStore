using DistributedKvStore.Shared.DTOs;
using DistributedKvStore.Shared.Enums;
using DistributedKvStore.Shared.Models;

namespace DistributedKvStore.Node.Services.Interfaces;

public interface IReplicationService
{
    Task ReplicateAsync(string key, string? value, OperationType opType, DateTime timestamp, ulong hash);
    Task ApplyReplicationAsync(ReplicationRequest request);
}
