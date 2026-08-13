namespace DistributedKvStore.Shared.Models;

public class KeyValueRecord
{
    public ulong Hash { get; set; }
    public string Key { get; set; } = string.Empty;
    public string Value { get; set; } = string.Empty;
    public DateTime LastUpdatedUtc { get; set; }
    public bool IsDeleted { get; set; }
}
