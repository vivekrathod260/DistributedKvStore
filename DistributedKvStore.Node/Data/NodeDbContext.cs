using Microsoft.EntityFrameworkCore;
using DistributedKvStore.Shared.Models;

namespace DistributedKvStore.Node.Data;

public class NodeDbContext : DbContext
{
    public DbSet<KeyValueRecord> KeyValueRecords => Set<KeyValueRecord>();

    public NodeDbContext(DbContextOptions<NodeDbContext> options) : base(options) { }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<KeyValueRecord>(entity =>
        {
            entity.HasKey(e => e.Key);
            entity.Property(e => e.Key).HasMaxLength(512);
            entity.Property(e => e.Value).IsRequired();
            entity.HasIndex(e => e.Hash);
        });
    }
}
