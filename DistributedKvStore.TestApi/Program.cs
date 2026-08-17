using DistributedKvStore.Client;
using DistributedKvStore.Shared.DTOs;

var builder = WebApplication.CreateBuilder(args);


var seedNodes = builder.Configuration.GetSection("Cluster:SeedNodes").Get<string[]>();

if(seedNodes == null || seedNodes.Length == 0)
{
    throw new InvalidOperationException("No seed nodes configured. Please specify at least one seed node in the configuration.");
}

builder.Services.AddHttpClient("DistributedKvStore");
builder.Services.AddDistributedKvClient(seedNodes);

var app = builder.Build();


// ########### Key Val CRUD Endpoints ############
app.MapGet("/key/{key}", async (string key, IDistributedClient client) =>
{
    var value = await client.GetAsync(key);
    if (value == null)
        return Results.NotFound(new { Key = key, Message = "Key not found" });

    return Results.Ok(new KeyValueResponse { Key = key, Value = value, LastUpdatedUtc = DateTime.UtcNow });
});

app.MapPost("/key", async (KeyValueRequest request, IDistributedClient client) =>
{
    await client.PutAsync(request.Key, request.Value);
    return Results.Ok(new { Message = $"Key '{request.Key}' stored successfully" });
});

app.MapPut("/key", async (KeyValueRequest request, IDistributedClient client) =>
{
    await client.UpdateAsync(request.Key, request.Value);
    return Results.Ok(new { Message = $"Key '{request.Key}' updated successfully" });
});

app.MapDelete("/key/{key}", async (string key, IDistributedClient client) =>
{
    await client.DeleteAsync(key);
    return Results.Ok(new { Message = $"Key '{key}' deleted successfully" });
});


// ########### Cluster Management Endpoints ############
app.MapGet("/cluster/state", async (IDistributedClient client) =>
{
    var state = await client.GetClusterStateAsync();
    return Results.Ok(state);
});

app.MapGet("/cluster/state/node", async (string baseUrl, IDistributedClient client) =>
{
    var state = await client.GetClusterStateAsync(baseUrl);
    return Results.Ok(state);
});

app.MapPost("/cluster/start", async (IDistributedClient client) =>
{
    var state = await client.StartClusterAsync();
    return Results.Ok(state);
});

app.MapPost("/cluster/shutdown", async (IDistributedClient client) =>
{
    await client.ShutdownClusterAsync();
    return Results.Ok(new { Message = "Cluster shutdown initiated" });
});

app.MapPost("/cluster/nodes", async (AddNodeRequest request, IDistributedClient client) =>
{
    var state = await client.AddNodeAsync(request.BaseUrl, request.NodeId);
    return Results.Ok(state);
});

app.MapDelete("/cluster/nodes/{nodeId:guid}", async (Guid nodeId, IDistributedClient client) =>
{
    var state = await client.RemoveNodeAsync(nodeId);
    return Results.Ok(state);
});

app.MapPost("/cluster/replication-factor", async (SetReplicationFactorRequest request, IDistributedClient client) =>
{
    var state = await client.SetReplicationFactorAsync(request.ReplicationFactor);
    return Results.Ok(state);
});

app.MapGet("/cluster/health", async (string baseUrl, IDistributedClient client) =>
{
    var health = await client.CheckHealthAsync(baseUrl);
    if (health == null)
        return Results.Ok(new { BaseUrl = baseUrl, Healthy = false });

    return Results.Ok(new { BaseUrl = baseUrl, Healthy = true, health.NodeId, health.TimestampUtc });
});

app.Run();
