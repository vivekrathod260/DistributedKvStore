namespace DistributedKvStore.Shared.Enums;

public enum NodeStatus
{
    Online = 0,
    Suspect = 1,
    Failed = 2,
    Leaving = 3,
    Joining = 4
}
