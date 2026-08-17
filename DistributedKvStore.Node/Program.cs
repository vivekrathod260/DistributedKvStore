using DistributedKvStore.Node.BackgroundServices;
using DistributedKvStore.Node.Data;
using DistributedKvStore.Node.Persistence;
using DistributedKvStore.Node.Services.Implementation.Business;
using DistributedKvStore.Node.Services.Implementation.Communication;
using DistributedKvStore.Node.Services.Implementation.DataExchange;
using DistributedKvStore.Node.Services.Implementation.Management;
using DistributedKvStore.Node.Services.Implementation.State;
using DistributedKvStore.Node.Services.Interfaces;
using DistributedKvStore.Shared.Hashing;
using Microsoft.EntityFrameworkCore;

var builder = WebApplication.CreateBuilder(args);

// Configuration
var nodeId = Guid.Parse(builder.Configuration["Node:Id"] ?? Guid.NewGuid().ToString());
var baseUrl = builder.Configuration["Node:BaseUrl"] ?? "http://localhost:5000";

// Compute hash position from base URL
var hashRing = new ConsistentHashRing();
var hashPosition = hashRing.ComputeHash(baseUrl);

// Database
var dbPath = builder.Configuration["Node:DbPath"] ?? $"node_{nodeId:N}.db";
builder.Services.AddDbContextFactory<NodeDbContext>(options => options.UseSqlite($"Data Source={dbPath}"));

// Cluster metadata (membership/replication-factor/init snapshot survives a graceful shutdown)
var clusterStatePath = builder.Configuration["Node:ClusterStatePath"] ?? Path.ChangeExtension(dbPath, ".cluster.json");
var clusterMetadataStore = new ClusterMetadataStore(clusterStatePath);
builder.Services.AddSingleton<IClusterMetadataStore>(clusterMetadataStore);

// Register NodeStateService as singleton, restoring a prior snapshot if one was left by a graceful shutdown
var nodeStateService = new NodeStateService(nodeId, baseUrl, hashPosition);
var savedSnapshot = await clusterMetadataStore.LoadAsync();
if (savedSnapshot != null)
{
    nodeStateService.RestoreFromSnapshot(savedSnapshot);
}
builder.Services.AddSingleton<INodeStateService>(nodeStateService);

// Repositories
builder.Services.AddScoped<IDataRepository, SqliteDataRepository>();

// Services
builder.Services.AddScoped<IKeyValueService, KeyValueService>();
builder.Services.AddSingleton<IGossipService, GossipService>();
builder.Services.AddScoped<IReplicationService, ReplicationService>();
builder.Services.AddScoped<IRebalancingService, RebalancingService>();
builder.Services.AddScoped<IClusterManagementService, ClusterManagementService>();

// HttpClient
builder.Services.AddHttpClient("InternalNode", client =>
{
    client.Timeout = TimeSpan.FromSeconds(10);
});

// Background Services
builder.Services.AddHostedService<HeartbeatService>();

// Controllers
builder.Services.AddControllers();

var app = builder.Build();

// Ensure database is created
using (var scope = app.Services.CreateScope())
{
    var contextFactory = scope.ServiceProvider.GetRequiredService<IDbContextFactory<NodeDbContext>>();
    await using var context = await contextFactory.CreateDbContextAsync();
    await context.Database.EnsureCreatedAsync();
}

app.MapControllers();

app.Run();
