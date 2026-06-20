using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace DistributedKvStore.Client;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddDistributedKvClient(this IServiceCollection services, IEnumerable<string> seedNodes)
    {
        services.AddSingleton<IDistributedClient>(sp =>
        {
            var httpClientFactory = sp.GetService<IHttpClientFactory>();
            var logger = sp.GetService<ILogger<DistributedClient>>();
            var httpClient = httpClientFactory?.CreateClient("DistributedKvStore") ?? new HttpClient();
            return new DistributedClient(seedNodes, httpClient, logger);
        });

        return services;
    }
}
