using DistributedKvStore.Shared.Models;

namespace DistributedKvStore.Shared.DTOs;

public class SyncRequest
{
    public Guid NodeId { get; set; }
    public long LastOperationId { get; set; }
}

public class SyncResponse
{
    public List<OperationLog> Operations { get; set; } = new();
    public long LatestOperationId { get; set; }
}
