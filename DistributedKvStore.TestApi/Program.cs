using DistributedKvStore.Client;
using DistributedKvStore.Shared.DTOs;

var builder = WebApplication.CreateBuilder(args);

// Configure seed nodes from configuration
var seedNodes = builder.Configuration.GetSection("Cluster:SeedNodes").Get<string[]>()
    ?? new[] { "http://localhost:5001", "http://localhost:5002", "http://localhost:5003" };

builder.Services.AddHttpClient("DistributedKvStore");
builder.Services.AddDistributedKvClient(seedNodes);

var app = builder.Build();

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

app.Run();
