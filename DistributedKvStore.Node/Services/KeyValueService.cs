using DistributedKvStore.Node.Data;
using DistributedKvStore.Shared.DTOs;
using DistributedKvStore.Shared.Enums;
using DistributedKvStore.Shared.Models;
using Microsoft.Extensions.Logging;

namespace DistributedKvStore.Node.Services;

public class KeyValueService : IKeyValueService
{
    private readonly IDataRepository _repository;
    private readonly INodeStateService _nodeState;
    private readonly IReplicationService _replicationService;
    private readonly ILogger<KeyValueService> _logger;

    public KeyValueService(
        IDataRepository repository,
        INodeStateService nodeState,
        IReplicationService replicationService,
        ILogger<KeyValueService> logger)
    {
        _repository = repository;
        _nodeState = nodeState;
        _replicationService = replicationService;
        _logger = logger;
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

        var operation = new OperationLog
        {
            Key = key,
            Value = value,
            OperationType = OperationType.Put,
            TimestampUtc = timestamp
        };

        var operationId = await _repository.ApplyOperationAsync(operation);

        _ = Task.Run(() => ReplicateAsync(key, value, OperationType.Put, timestamp, operationId, hash));
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

        var operation = new OperationLog
        {
            Key = key,
            Value = value,
            OperationType = OperationType.Update,
            TimestampUtc = timestamp
        };

        var operationId = await _repository.ApplyOperationAsync(operation);

        _ = Task.Run(() => ReplicateAsync(key, value, OperationType.Update, timestamp, operationId, hash));
    }

    public async Task DeleteAsync(string key)
    {
        var hashRing = _nodeState.GetHashRing();
        var hash = hashRing.ComputeHash(key);
        var timestamp = DateTime.UtcNow;

        await _repository.DeleteAsync(key, timestamp);

        var operation = new OperationLog
        {
            Key = key,
            Value = null,
            OperationType = OperationType.Delete,
            TimestampUtc = timestamp
        };

        var operationId = await _repository.ApplyOperationAsync(operation);

        _ = Task.Run(() => ReplicateAsync(key, null, OperationType.Delete, timestamp, operationId, hash));
    }

    public async Task ApplyReplicationAsync(ReplicationRequest request)
    {
        if (request.OperationType == OperationType.Delete)
        {
            await _repository.DeleteAsync(request.Key, request.TimestampUtc);
        }
        else
        {
            var record = new KeyValueRecord
            {
                Key = request.Key,
                Value = request.Value ?? string.Empty,
                Hash = request.Hash,
                LastUpdatedUtc = request.TimestampUtc,
                IsDeleted = false
            };

            await _repository.PutAsync(record);
        }

        var operation = new OperationLog
        {
            Key = request.Key,
            Value = request.Value,
            OperationType = request.OperationType,
            TimestampUtc = request.TimestampUtc
        };

        await _repository.ApplyOperationAsync(operation);
    }

    private async Task ReplicateAsync(string key, string? value, OperationType opType, DateTime timestamp, long operationId, ulong hash)
    {
        try
        {
            var replicationFactor = _nodeState.GetReplicationFactor();
            var hashRing = _nodeState.GetHashRing();
            var replicas = hashRing.FindReplicaNodes(key, replicationFactor);
            var currentNode = _nodeState.GetCurrentNode();

            var targetReplicas = replicas
                .Where(r => r.NodeId != currentNode.NodeId)
                .ToList();

            var request = new ReplicationRequest
            {
                Key = key,
                Value = value,
                OperationType = opType,
                TimestampUtc = timestamp,
                OperationId = operationId,
                Hash = hash
            };

            await _replicationService.ReplicateToNodesAsync(request, targetReplicas);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Replication failed for key {Key}. Recovery sync will handle it.", key);
        }
    }
}
