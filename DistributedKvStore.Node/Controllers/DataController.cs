using DistributedKvStore.Node.Services.Implementation.State;
using DistributedKvStore.Node.Services.Interfaces;
using DistributedKvStore.Shared.DTOs;
using Microsoft.AspNetCore.Mvc;

namespace DistributedKvStore.Node.Controllers;

[ApiController]
[Route("api/data")]
public class DataController : ControllerBase
{
    private readonly IKeyValueService _kvService;
    private readonly INodeStateService _nodeState;

    public DataController(IKeyValueService kvService, INodeStateService nodeState)
    {
        _kvService = kvService;
        _nodeState = nodeState;
    }

    [HttpGet("{key}")]
    public async Task<IActionResult> Get(string key)
    {
        if (!_nodeState.IsInitialized)
            return StatusCode(StatusCodes.Status503ServiceUnavailable, "Cluster/Node has not been Initialized yet.");

        var result = await _kvService.GetAsync(key);
        if (result == null)
            return NotFound();

        return Ok(result);
    }

    [HttpPost]
    public async Task<IActionResult> Post([FromBody] KeyValueRequest request)
    {
        if (!_nodeState.IsInitialized)
            return StatusCode(StatusCodes.Status503ServiceUnavailable, "Cluster/Node has not been Initialized yet.");

        await _kvService.PutAsync(request.Key, request.Value);
        return Ok();
    }

    [HttpPut]
    public async Task<IActionResult> Put([FromBody] KeyValueRequest request)
    {
        if (!_nodeState.IsInitialized)
            return StatusCode(StatusCodes.Status503ServiceUnavailable, "Cluster/Node has not been Initialized yet.");

        await _kvService.UpdateAsync(request.Key, request.Value);
        return Ok();
    }

    [HttpDelete("{key}")]
    public async Task<IActionResult> Delete(string key)
    {
        if (!_nodeState.IsInitialized)
            return StatusCode(StatusCodes.Status503ServiceUnavailable, "Cluster/Node has not been Initialized yet.");

        await _kvService.DeleteAsync(key);
        return Ok();
    }
}
