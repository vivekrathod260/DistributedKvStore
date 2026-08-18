namespace DistributedKvStore.Shared.DTOs;

public class AddNodeRequest
{
    public string BaseUrl { get; set; } = string.Empty;
    public Guid? NodeId { get; set; }
}

public class RemoveNodeRequest
{
    public Guid NodeId { get; set; }
}

public class SetReplicationFactorRequest
{
    public int ReplicationFactor { get; set; }
}
