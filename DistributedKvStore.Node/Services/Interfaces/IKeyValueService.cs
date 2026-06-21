using DistributedKvStore.Shared.DTOs;
using DistributedKvStore.Shared.Models;

namespace DistributedKvStore.Node.Services.Interfaces;

public interface IKeyValueService
{
    Task<KeyValueResponse?> GetAsync(string key);
    Task PutAsync(string key, string value);
    Task UpdateAsync(string key, string value);
    Task DeleteAsync(string key);
    Task ApplyReplicationAsync(ReplicationRequest request);
}
