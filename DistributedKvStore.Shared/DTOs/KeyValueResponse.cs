namespace DistributedKvStore.Shared.DTOs;

public class KeyValueResponse
{
    public string Key { get; set; } = string.Empty;
    public string? Value { get; set; }
    public DateTime LastUpdatedUtc { get; set; }
    public bool IsDeleted { get; set; }
}
