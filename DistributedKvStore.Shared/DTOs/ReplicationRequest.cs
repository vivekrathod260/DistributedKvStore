using DistributedKvStore.Shared.Enums;
using DistributedKvStore.Shared.Models;

namespace DistributedKvStore.Shared.DTOs;

public class ReplicationRequest
{
    public string Key { get; set; } = string.Empty;
    public string? Value { get; set; }
    public OperationType OperationType { get; set; }
    public DateTime TimestampUtc { get; set; }
    public long OperationId { get; set; }
    public ulong Hash { get; set; }
}
