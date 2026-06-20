using DistributedKvStore.Node.Services;
using DistributedKvStore.Shared.DTOs;
using Microsoft.AspNetCore.Mvc;

namespace DistributedKvStore.Node.Controllers;

[ApiController]
[Route("api/data")]
public class DataController : ControllerBase
{
    private readonly IKeyValueService _kvService;

    public DataController(IKeyValueService kvService)
    {
        _kvService = kvService;
    }

    [HttpGet("{key}")]
    public async Task<IActionResult> Get(string key)
    {
        var result = await _kvService.GetAsync(key);
        if (result == null)
            return NotFound();

        return Ok(result);
    }

    [HttpPost]
    public async Task<IActionResult> Post([FromBody] KeyValueRequest request)
    {
        await _kvService.PutAsync(request.Key, request.Value);
        return Ok();
    }

    [HttpPut]
    public async Task<IActionResult> Put([FromBody] KeyValueRequest request)
    {
        await _kvService.UpdateAsync(request.Key, request.Value);
        return Ok();
    }

    [HttpDelete("{key}")]
    public async Task<IActionResult> Delete(string key)
    {
        await _kvService.DeleteAsync(key);
        return Ok();
    }
}
