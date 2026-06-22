using DistributedKvStore.Shared.Enums;

namespace DistributedKvStore.Shared.DTOs;

public class GossipMessage
{
    public Guid MessageId { get; set; }
    public long ClusterVersion { get; set; }
    public Guid SenderNodeId { get; set; }
    public GossipTopic Topic { get; set; }
    public GossipPayload Payload { get; set; } = new(); 
    public DateTime TimestampUtc { get; set; }
}

public class GossipPayload
{
    public List<NodeStatusChange>? NodeStatusChanges { get; set; } = new();
}

public class NodeStatusChange
{
    public Guid NodeId { get; set; }
    public NodeStatus NewStatus { get; set; }
    public string? BaseUrl { get; set; }
    public ulong HashPosition { get; set; }
}
