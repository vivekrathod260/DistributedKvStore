using DistributedKvStore.Node.Services;
using DistributedKvStore.Shared.DTOs;
using Microsoft.AspNetCore.Mvc;

namespace DistributedKvStore.Node.Controllers;

[ApiController]
[Route("api/cluster")]
public class ClusterController : ControllerBase
{
    private readonly IClusterManagementService _clusterService;
    private readonly INodeStateService _nodeState;

    public ClusterController(IClusterManagementService clusterService, INodeStateService nodeState)
    {
        _clusterService = clusterService;
        _nodeState = nodeState;
    }

    [HttpGet("state")]
    public IActionResult GetState()
    {
        var state = _nodeState.GetClusterState();
        return Ok(state);
    }

    [HttpPost("start")]
    public async Task<IActionResult> Start()
    {
        var state = await _clusterService.StartClusterAsync();
        return Ok(state);
    }

    [HttpPost("add-node")]
    public async Task<IActionResult> AddNode([FromBody] AddNodeRequest request)
    {
        var state = await _clusterService.AddNodeAsync(request.BaseUrl);
        return Ok(state);
    }

    [HttpPost("remove-node")]
    public async Task<IActionResult> RemoveNode([FromBody] RemoveNodeRequest request)
    {
        var state = await _clusterService.RemoveNodeAsync(request.NodeId);
        return Ok(state);
    }

    [HttpPost("restart-node")]
    public async Task<IActionResult> RestartNode([FromBody] RestartNodeRequest request)
    {
        var state = await _clusterService.RestartNodeAsync(request.NodeId);
        return Ok(state);
    }

    [HttpPost("set-replication-factor")]
    public async Task<IActionResult> SetReplicationFactor([FromBody] SetReplicationFactorRequest request)
    {
        await _clusterService.SetReplicationFactorAsync(request.ReplicationFactor);
        return Ok();
    }

    [HttpPost("shutdown")]
    public async Task<IActionResult> Shutdown()
    {
        await _clusterService.ShutdownClusterAsync();
        return Ok();
    }
}
