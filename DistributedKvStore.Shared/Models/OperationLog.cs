using DistributedKvStore.Shared.Enums;

namespace DistributedKvStore.Shared.Models;

public class OperationLog
{
    public long OperationId { get; set; }
    public string Key { get; set; } = string.Empty;
    public string? Value { get; set; }
    public OperationType OperationType { get; set; }
    public DateTime TimestampUtc { get; set; }
}
