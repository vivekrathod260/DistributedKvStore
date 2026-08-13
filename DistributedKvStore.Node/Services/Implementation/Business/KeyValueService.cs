using DistributedKvStore.Node.Data;
using DistributedKvStore.Node.Services.Implementation.State;
using DistributedKvStore.Node.Services.Interfaces;
using DistributedKvStore.Shared.DTOs;
using DistributedKvStore.Shared.Enums;
using DistributedKvStore.Shared.Models;

namespace DistributedKvStore.Node.Services.Implementation.Business;

public class KeyValueService : IKeyValueService
{
    private readonly IDataRepository _repository;
    private readonly INodeStateService _nodeState;
    private readonly IReplicationService _replicationService;

    public KeyValueService(
        IDataRepository repository,
        INodeStateService nodeState,
        IReplicationService replicationService)
    {
        _repository = repository;
        _nodeState = nodeState;
        _replicationService = replicationService;
    }

    public async Task<KeyValueResponse?> GetAsync(string key)
    {
        var record = await _repository.GetAsync(key);
        if (record == null || record.IsDeleted)
            return null;

        return new KeyValueResponse
        {
            Key = record.Key,
            Value = record.Value,
            LastUpdatedUtc = record.LastUpdatedUtc,
            IsDeleted = record.IsDeleted
        };
    }

    public async Task PutAsync(string key, string value)
    {
        var hashRing = _nodeState.GetHashRing();
        var hash = hashRing.ComputeHash(key);
        var timestamp = DateTime.UtcNow;

        var record = new KeyValueRecord
        {
            Key = key,
            Value = value,
            Hash = hash,
            LastUpdatedUtc = timestamp,
            IsDeleted = false
        };

        await _repository.PutAsync(record);

        _ = Task.Run(() => _replicationService.ReplicateAsync(key, value, OperationType.Put, timestamp, hash));
    }

    public async Task UpdateAsync(string key, string value)
    {
        var hashRing = _nodeState.GetHashRing();
        var hash = hashRing.ComputeHash(key);
        var timestamp = DateTime.UtcNow;

        var record = new KeyValueRecord
        {
            Key = key,
            Value = value,
            Hash = hash,
            LastUpdatedUtc = timestamp,
            IsDeleted = false
        };

        await _repository.PutAsync(record);

        _ = Task.Run(() => _replicationService.ReplicateAsync(key, value, OperationType.Update, timestamp, hash));
    }

    public async Task DeleteAsync(string key)
    {
        var hashRing = _nodeState.GetHashRing();
        var hash = hashRing.ComputeHash(key);
        var timestamp = DateTime.UtcNow;

        await _repository.DeleteAsync(key, timestamp);

        _ = Task.Run(() => _replicationService.ReplicateAsync(key, null, OperationType.Delete, timestamp, hash));
    }
}
