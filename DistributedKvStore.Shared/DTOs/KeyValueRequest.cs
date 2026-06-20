namespace DistributedKvStore.Shared.DTOs;

public class KeyValueRequest
{
    public string Key { get; set; } = string.Empty;
    public string Value { get; set; } = string.Empty;
}
