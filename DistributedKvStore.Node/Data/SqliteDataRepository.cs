using Microsoft.EntityFrameworkCore;
using DistributedKvStore.Shared.Enums;
using DistributedKvStore.Shared.Models;

namespace DistributedKvStore.Node.Data;

public interface IDataRepository
{
    Task<KeyValueRecord?> GetAsync(string key);
    Task PutAsync(KeyValueRecord record);
    Task DeleteAsync(string key, DateTime timestampUtc);
    Task<long> ApplyOperationAsync(OperationLog operation);
    Task<long> GetLastOperationIdAsync();
    Task<List<OperationLog>> GetOperationsAfterAsync(long afterOperationId);
    Task<List<KeyValueRecord>> GetRecordsInHashRangeAsync(ulong rangeStart, ulong rangeEnd);
    Task BulkInsertRecordsAsync(IEnumerable<KeyValueRecord> records);
    Task DeleteRecordsInHashRangeAsync(ulong rangeStart, ulong rangeEnd);
}

public class SqliteDataRepository : IDataRepository
{
    private readonly IDbContextFactory<NodeDbContext> _contextFactory;

    public SqliteDataRepository(IDbContextFactory<NodeDbContext> contextFactory)
    {
        _contextFactory = contextFactory;
    }

    public async Task<KeyValueRecord?> GetAsync(string key)
    {
        await using var context = await _contextFactory.CreateDbContextAsync();
        return await context.KeyValueRecords.AsNoTracking().FirstOrDefaultAsync(r => r.Key == key);
    }

    public async Task PutAsync(KeyValueRecord record)
    {
        await using var context = await _contextFactory.CreateDbContextAsync();
        var existing = await context.KeyValueRecords.FirstOrDefaultAsync(r => r.Key == record.Key);

        if (existing != null)
        {
            // Last Write Wins
            if (record.LastUpdatedUtc >= existing.LastUpdatedUtc)
            {
                existing.Value = record.Value;
                existing.Hash = record.Hash;
                existing.LastUpdatedUtc = record.LastUpdatedUtc;
                existing.IsDeleted = record.IsDeleted;
            }
        }
        else
        {
            context.KeyValueRecords.Add(record);
        }

        await context.SaveChangesAsync();
    }

    public async Task DeleteAsync(string key, DateTime timestampUtc)
    {
        await using var context = await _contextFactory.CreateDbContextAsync();
        var existing = await context.KeyValueRecords.FirstOrDefaultAsync(r => r.Key == key);

        if (existing != null)
        {
            if (timestampUtc >= existing.LastUpdatedUtc)
            {
                existing.IsDeleted = true;
                existing.LastUpdatedUtc = timestampUtc;
                await context.SaveChangesAsync();
            }
        }
    }


    public async Task<long> ApplyOperationAsync(OperationLog operation)
    {
        await using var context = await _contextFactory.CreateDbContextAsync();
        context.OperationLogs.Add(operation);
        await context.SaveChangesAsync();
        return operation.OperationId;
    }

    public async Task<long> GetLastOperationIdAsync()
    {
        await using var context = await _contextFactory.CreateDbContextAsync();
        var lastOp = await context.OperationLogs
            .OrderByDescending(o => o.OperationId)
            .FirstOrDefaultAsync();
        return lastOp?.OperationId ?? 0;
    }

    public async Task<List<OperationLog>> GetOperationsAfterAsync(long afterOperationId)
    {
        await using var context = await _contextFactory.CreateDbContextAsync();
        return await context.OperationLogs
            .AsNoTracking()
            .Where(o => o.OperationId > afterOperationId)
            .OrderBy(o => o.OperationId)
            .ToListAsync();
    }


    public async Task<List<KeyValueRecord>> GetRecordsInHashRangeAsync(ulong rangeStart, ulong rangeEnd)
    {
        await using var context = await _contextFactory.CreateDbContextAsync();

        if (rangeStart <= rangeEnd)
        {
            return await context.KeyValueRecords
                .AsNoTracking()
                .Where(r => r.Hash >= rangeStart && r.Hash <= rangeEnd)
                .ToListAsync();
        }
        else
        {
            // Wrap-around range
            return await context.KeyValueRecords
                .AsNoTracking()
                .Where(r => r.Hash >= rangeStart || r.Hash <= rangeEnd)
                .ToListAsync();
        }
    }

    public async Task BulkInsertRecordsAsync(IEnumerable<KeyValueRecord> records)
    {
        await using var context = await _contextFactory.CreateDbContextAsync();

        foreach (var record in records)
        {
            var existing = await context.KeyValueRecords.FirstOrDefaultAsync(r => r.Key == record.Key);
            if (existing != null)
            {
                if (record.LastUpdatedUtc >= existing.LastUpdatedUtc)
                {
                    existing.Value = record.Value;
                    existing.Hash = record.Hash;
                    existing.LastUpdatedUtc = record.LastUpdatedUtc;
                    existing.IsDeleted = record.IsDeleted;
                }
            }
            else
            {
                context.KeyValueRecords.Add(record);
            }
        }

        await context.SaveChangesAsync();
    }

    public async Task DeleteRecordsInHashRangeAsync(ulong rangeStart, ulong rangeEnd)
    {
        await using var context = await _contextFactory.CreateDbContextAsync();

        List<KeyValueRecord> records;
        if (rangeStart <= rangeEnd)
        {
            records = await context.KeyValueRecords
                .Where(r => r.Hash >= rangeStart && r.Hash <= rangeEnd)
                .ToListAsync();
        }
        else
        {
            records = await context.KeyValueRecords
                .Where(r => r.Hash >= rangeStart || r.Hash <= rangeEnd)
                .ToListAsync();
        }

        context.KeyValueRecords.RemoveRange(records);
        await context.SaveChangesAsync();
    }
}
