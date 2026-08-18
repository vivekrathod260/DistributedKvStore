using DistributedKvStore.Shared.Models;

namespace DistributedKvStore.Shared.Hashing;

/// <summary>
/// Pure ring-offset arithmetic over an explicit, caller-supplied node list (sorted by HashPosition).
/// Lets callers reason about topology relative to a node that isn't part of the live routing
/// ring anymore (e.g. a departing node already excluded from ConsistentHashRing's BuildRing),
/// without perturbing the shared ring used for request routing.
/// </summary>
public static class HashRingMath
{
    public static ClusterNodeInfo? GetNodeByOffset(IReadOnlyList<ClusterNodeInfo> sortedNodes, Guid currentNodeId, int offset)
    {
        var count = sortedNodes.Count;
        if (count == 0)
            return null;

        var index = -1;
        for (var i = 0; i < count; i++)
        {
            if (sortedNodes[i].NodeId == currentNodeId)
            {
                index = i;
                break;
            }
        }

        if (index == -1)
            return null;

        offset %= count;

        var newIndex = (index + offset + count) % count;
        return sortedNodes[newIndex];
    }

    public static (ulong Start, ulong End)? GetHashRange(IReadOnlyList<ClusterNodeInfo> sortedNodes, Guid nodeId)
    {
        var count = sortedNodes.Count;
        if (count == 0)
            return null;

        var index = -1;
        for (var i = 0; i < count; i++)
        {
            if (sortedNodes[i].NodeId == nodeId)
            {
                index = i;
                break;
            }
        }

        if (index == -1)
            return null;

        var predecessorIndex = (index - 1 + count) % count;
        var predecessor = sortedNodes[predecessorIndex];
        var node = sortedNodes[index];

        return (Start: unchecked(predecessor.HashPosition + 1), End: node.HashPosition);
    }
}
