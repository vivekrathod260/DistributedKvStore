using DistributedKvStore.Node.Data;
using DistributedKvStore.Node.Services.Implementation.Communication;
using DistributedKvStore.Node.Services.Implementation.State;
using DistributedKvStore.Node.Services.Interfaces;
using DistributedKvStore.Shared.DTOs;
using DistributedKvStore.Shared.Enums;
using DistributedKvStore.Shared.Models;
using Microsoft.AspNetCore.Mvc;
using System.Net.Http;
using System.Threading;

namespace DistributedKvStore.Node.Controllers;

[ApiController]
[Route("internal")]
public class InternalController : ControllerBase
{
    private readonly IKeyValueService _kvService;
    private readonly IGossipService _gossipService;
    private readonly INodeStateService _nodeState;
    private readonly IDataRepository _repository;
    private readonly IHttpClientFactory _httpClientFactory;
    private readonly ILogger<InternalController> _logger;

    public InternalController(
        IKeyValueService kvService,
        IGossipService gossipService,
        INodeStateService nodeState,
        IDataRepository repository,
        IHttpClientFactory httpClientFactory,
        ILogger<InternalController> logger)
    {
        _kvService = kvService;
        _gossipService = gossipService;
        _nodeState = nodeState;
        _repository = repository;
        _httpClientFactory = httpClientFactory;
        _logger = logger;
    }

    [HttpPost("replication")]
    public async Task<IActionResult> Replication([FromBody] ReplicationRequest request)
    {
        await _kvService.ApplyReplicationAsync(request);
        return Ok();
    }

    [HttpPost("gossip")]
    public async Task<IActionResult> Gossip([FromBody] GossipMessage message)
    {
        await _gossipService.ProcessGossipMessageAsync(message);
        return Ok();
    }

    [HttpPost("migrate")]
    public async Task<IActionResult> Migrate([FromBody] MigrationResponse migrationData)
    {
        if (migrationData.Records.Count > 0)
        {
            await _repository.BulkInsertRecordsAsync(migrationData.Records);
            _logger.LogInformation("Received migration of {Count} records", migrationData.Records.Count);
        }
        return Ok();
    }

    [HttpPost("migrate-out")]
    public async Task<IActionResult> MigrateOut([FromBody] MigrationRequest request)
    {
        var records = await _repository.GetRecordsInHashRangeAsync(request.RangeStart, request.RangeEnd);
        var response = new MigrationResponse
        {
            Records = records,
            IsComplete = true
        };
        return Ok(response);
    }

    [HttpPost("sync")]
    public async Task<IActionResult> Sync([FromBody] SyncRequest request)
    {
        var operations = await _repository.GetOperationsAfterAsync(request.LastOperationId);
        var lastOpId = await _repository.GetLastOperationIdAsync();

        var response = new SyncResponse
        {
            Operations = operations,
            LatestOperationId = lastOpId
        };
        return Ok(response);
    }

    [HttpGet("operations")]
    public async Task<IActionResult> GetOperations([FromQuery] long after = 0)
    {
        var operations = await _repository.GetOperationsAfterAsync(after);
        var lastOpId = await _repository.GetLastOperationIdAsync();

        var response = new SyncResponse
        {
            Operations = operations,
            LatestOperationId = lastOpId
        };
        return Ok(response);
    }

    [HttpGet("ping")]
    public IActionResult Ping()
    {
        var currentNode = _nodeState.GetCurrentNode();
        var clusterState = _nodeState.GetClusterState();

        return Ok(new PingResponse
        {
            NodeId = currentNode.NodeId,
            IsInitialized = _nodeState.IsInitialized,
            ClusterVersion = clusterState.Version,
            TimestampUtc = DateTime.UtcNow
        });
    }

    [HttpPost("proxy-ping")]
    public async Task<IActionResult> ProxyPing(ProxyPingRequest request)
    {
        var client = _httpClientFactory.CreateClient("InternalNode");
        client.BaseAddress = new Uri(request.TargetNode.BaseUrl);
        client.Timeout = TimeSpan.FromSeconds(3);

        var response = await client.GetAsync("/internal/ping");
        if (response.IsSuccessStatusCode)
        {
            return Ok(await response.Content.ReadFromJsonAsync<PingResponse>());
        }
        else
        {
            return StatusCode((int)response.StatusCode, await response.Content.ReadAsStringAsync());
        }
    }
}
