namespace DistributedKvStore.Client;

public interface IDistributedClient : IDisposable
{
    Task<string?> GetAsync(string key);
    Task PutAsync(string key, string value);
    Task UpdateAsync(string key, string value);
    Task DeleteAsync(string key);
}
