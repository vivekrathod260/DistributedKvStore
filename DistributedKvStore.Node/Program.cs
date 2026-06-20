using DistributedKvStore.Node.BackgroundServices;
using DistributedKvStore.Node.Data;
using DistributedKvStore.Node.Services;
using DistributedKvStore.Shared.Hashing;
using Microsoft.EntityFrameworkCore;

var builder = WebApplication.CreateBuilder(args);

// Configuration
var nodeId = Guid.Parse(builder.Configuration["Node:Id"] ?? Guid.NewGuid().ToString());
var baseUrl = builder.Configuration["Node:BaseUrl"] ?? "http://localhost:5000";

// Compute hash position from base URL
var hashRing = new ConsistentHashRing();
var hashPosition = hashRing.ComputeHash(baseUrl);

// Register NodeStateService as singleton
var nodeStateService = new NodeStateService(nodeId, baseUrl, hashPosition);
builder.Services.AddSingleton<INodeStateService>(nodeStateService);

// Database
var dbPath = builder.Configuration["Node:DbPath"] ?? $"node_{nodeId:N}.db";
builder.Services.AddDbContextFactory<NodeDbContext>(options => options.UseSqlite($"Data Source={dbPath}"));

// Repositories
builder.Services.AddScoped<IDataRepository, SqliteDataRepository>();

// Services
builder.Services.AddScoped<IKeyValueService, KeyValueService>();
builder.Services.AddSingleton<IGossipService, GossipService>();
builder.Services.AddScoped<IReplicationService, ReplicationService>();
builder.Services.AddScoped<IMigrationService, MigrationService>();
builder.Services.AddScoped<IClusterManagementService, ClusterManagementService>();

// HttpClient
builder.Services.AddHttpClient("InternalNode", client =>
{
    client.Timeout = TimeSpan.FromSeconds(10);
});

// Background Services
builder.Services.AddHostedService<HeartbeatService>();
builder.Services.AddHostedService<GossipWorker>();
builder.Services.AddHostedService<RecoverySyncService>();
builder.Services.AddHostedService<ClusterTopologyRefreshService>();

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
