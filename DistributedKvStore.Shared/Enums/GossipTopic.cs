namespace DistributedKvStore.Shared.Enums;

public enum GossipTopic
{
    NodeStatusChange = 0,
    NodeSuspicion = 1,
    NodeJoin = 2,
    NodeRemovalProposal = 3,
    ReplicationFactorChange = 4
}