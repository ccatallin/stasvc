using FalxGroup.Finance.Service;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

var host = new HostBuilder()
    .ConfigureFunctionsWorkerDefaults()
    .ConfigureServices(services =>
    {
        // Register the TransactionLoggerService for dependency injection.
        // It will be created once and reused, which is efficient.
        services.AddSingleton<TransactionLoggerService>(sp =>
        {
            string? connectionString = Environment.GetEnvironmentVariable("SqlConnectionString");
            if (string.IsNullOrEmpty(connectionString))
            {
                throw new InvalidOperationException("The 'SqlConnectionString' environment variable is not set.");
            }
            return new TransactionLoggerService(connectionString!);
        });
    })
    .Build();

host.Run();