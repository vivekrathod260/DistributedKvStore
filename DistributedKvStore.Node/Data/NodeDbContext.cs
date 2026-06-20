using Microsoft.EntityFrameworkCore;
using DistributedKvStore.Shared.Models;

namespace DistributedKvStore.Node.Data;

public class NodeDbContext : DbContext
{
    public DbSet<KeyValueRecord> KeyValueRecords => Set<KeyValueRecord>();
    public DbSet<OperationLog> OperationLogs => Set<OperationLog>();

    public NodeDbContext(DbContextOptions<NodeDbContext> options) : base(options) { }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<KeyValueRecord>(entity =>
        {
            entity.HasKey(e => e.Key);
            entity.Property(e => e.Key).HasMaxLength(512);
            entity.Property(e => e.Value).IsRequired();
            entity.HasIndex(e => e.Hash);
            entity.HasIndex(e => e.LastUpdatedUtc);
        });

        modelBuilder.Entity<OperationLog>(entity =>
        {
            entity.HasKey(e => e.OperationId);
            entity.Property(e => e.OperationId).ValueGeneratedOnAdd();
            entity.Property(e => e.Key).HasMaxLength(512);
            entity.HasIndex(e => e.Key);
            entity.HasIndex(e => e.TimestampUtc);
        });
    }
}
