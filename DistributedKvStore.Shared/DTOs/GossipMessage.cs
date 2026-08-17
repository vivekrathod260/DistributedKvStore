using DistributedKvStore.Shared.Enums;

namespace DistributedKvStore.Shared.DTOs;

public class GossipMessage
{
    public Guid MessageId { get; set; }
    public Guid SenderNodeId { get; set; }
    public GossipTopic Topic { get; set; }
    public GossipPayload Payload { get; set; } = new();
    public DateTime TimestampUtc { get; set; }
}

public class GossipPayload
{
    public List<NodeStatusChange>? NodeStatusChanges { get; set; } = new();
    public NodeSuspicionInfo? NodeSuspicion { get; set; }
    public NodeJoinInfo? NodeJoin { get; set; }
    public NodeRemovalProposalInfo? NodeRemovalProposal { get; set; }
    public ReplicationFactorChangeInfo? ReplicationFactorChange { get; set; }
}

public class NodeStatusChange
{
    public Guid NodeId { get; set; }
    public NodeStatus NewStatus { get; set; }
    public string? BaseUrl { get; set; }
    public ulong HashPosition { get; set; }
}

public class NodeSuspicionInfo
{
    public Guid SuspectedNodeId { get; set; }
    public List<Guid> VerifierNodeIds { get; set; } = new();
}

public class NodeJoinInfo
{
    public Guid NodeId { get; set; }
    public string BaseUrl { get; set; } = string.Empty;
    public ulong HashPosition { get; set; }
}

public class NodeRemovalProposalInfo
{
    public Guid OfflineNodeId { get; set; }
    public Guid ProposerNodeId { get; set; }
}

public class ReplicationFactorChangeInfo
{
    public int NewReplicationFactor { get; set; }
}
