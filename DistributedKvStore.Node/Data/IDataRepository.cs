using DistributedKvStore.Shared.Models;

namespace DistributedKvStore.Node.Data;

public interface IDataRepository
{
    Task<KeyValueRecord?> GetAsync(string key);
    Task PutAsync(KeyValueRecord record);
    Task DeleteAsync(string key, DateTime timestampUtc);
    Task<List<OperationLog>> GetOperationsAfterAsync(long afterOperationId);
    Task<long> ApplyOperationAsync(OperationLog operation);
    Task<long> GetLastOperationIdAsync();
    Task<List<KeyValueRecord>> GetRecordsInHashRangeAsync(ulong rangeStart, ulong rangeEnd);
    Task DeleteRecordsInHashRangeAsync(ulong rangeStart, ulong rangeEnd);
    Task BulkInsertRecordsAsync(IEnumerable<KeyValueRecord> records);
}
