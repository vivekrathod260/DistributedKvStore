using DistributedKvStore.Shared.Models;

namespace DistributedKvStore.Shared.DTOs;

public class MigrationRequest
{
    public Guid RequestingNodeId { get; set; }
    public ulong RangeStart { get; set; }
    public ulong RangeEnd { get; set; }
}

public class MigrationResponse
{
    public List<KeyValueRecord> Records { get; set; } = new();
    public bool IsComplete { get; set; }
}
