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

            // SQLite has no native unsigned 64-bit type, so ulong.Hash can't be
            // stored/compared directly. Store it as a fixed-width, zero-padded
            // decimal string (max ulong is 20 digits)
            entity.Property(e => e.Hash)
                .HasConversion(
                    v => v.ToString("D20"),
                    v => ulong.Parse(v))
                .HasMaxLength(20)
                .IsFixedLength();

            entity.HasIndex(e => e.Hash);
        });
    }
}
