using System.Text.Json;
using DistributedKvStore.Shared.Models;

namespace DistributedKvStore.Node.Persistence;

public interface IClusterMetadataStore
{
    Task SaveAsync(ClusterSnapshot snapshot);
    Task<ClusterSnapshot?> LoadAsync();
}

public class ClusterMetadataStore : IClusterMetadataStore
{
    private readonly string _path;
    private static readonly JsonSerializerOptions SerializerOptions = new() { WriteIndented = true };

    public ClusterMetadataStore(string path)
    {
        _path = path;
    }

    public async Task SaveAsync(ClusterSnapshot snapshot)
    {
        var json = JsonSerializer.Serialize(snapshot, SerializerOptions);
        var tempPath = _path + ".tmp";
        await File.WriteAllTextAsync(tempPath, json);
        File.Move(tempPath, _path, overwrite: true);
    }

    public async Task<ClusterSnapshot?> LoadAsync()
    {
        if (!File.Exists(_path))
            return null;

        var json = await File.ReadAllTextAsync(_path);
        return JsonSerializer.Deserialize<ClusterSnapshot>(json, SerializerOptions);
    }
}
