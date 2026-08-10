namespace DistributedKvStore.Shared.Enums;

public enum NodeStatus
{
    Online = 0,
    Joining = 1,
    Suspect = 2,
    Failed = 3,
    Leaving = 4,
}
