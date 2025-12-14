using System.Text;
using System.Net;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Logging;
// --
using FalxGroup.Finance.Service;
using Newtonsoft.Json;

namespace FalxGroup.Finance.Function
{
    public class Ticker
    {
        private const string version = "2.0.2-isolated";
        private readonly TickerService _processor;
        private readonly ILogger _logger;

        public Ticker(ILoggerFactory loggerFactory, TickerService processor)
        {
            _logger = loggerFactory.CreateLogger<Ticker>();
            _processor = processor;
        }

        [Function("Ticker")]
        public async Task<HttpResponseData> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", /* "post", */ Route = "finance/v1/ticker/{symbol?}/{market:alpha?}")] HttpRequestData req,
            string? symbol,
            string? market)
        {
            try
            {
                var httpResponse = req.CreateResponse(HttpStatusCode.OK);
                httpResponse.Headers.Add("Content-Type", "application/json; charset=utf-8");

                // Handle multiple symbols, passed as a comma-separated string
                if (symbol != null && symbol.Contains(','))
                {
                    var symbols = symbol.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
                    var tickerTasks = symbols.Select(s => _processor.Run(nameof(Ticker), version, s, market));
                    var results = await Task.WhenAll(tickerTasks);

                    var successfulResults = results.Where(r => r.StatusCode == 200 || r.StatusCode == 201).ToList();
                    var failedSymbols = results.Where(r => r.StatusCode != 200 && r.StatusCode != 201).Select(r => r.Symbol).ToList();

                    var responseObject = new
                    {
                        StatusCode = successfulResults.Any() ? (int)HttpStatusCode.OK : (int)HttpStatusCode.NotFound,
                        Message = $"Processed {symbols.Length} symbols. {successfulResults.Count} succeeded, {failedSymbols.Count} failed.",
                        Tickers = successfulResults.ToDictionary(r => r.Symbol!, r => r.Value),
                        FailedSymbols = failedSymbols
                    };

                    await httpResponse.WriteStringAsync(JsonConvert.SerializeObject(responseObject, Formatting.Indented));
                }
                else // Handle a single symbol
                {
                    var response = await _processor.Run(nameof(Ticker), version, symbol, market);

                    StringBuilder responseBuilder = new StringBuilder();

                    responseBuilder.Append("{")
                        .Append("\"StatusCode\":").Append($"{response.StatusCode}")
                        .Append(", \"Message\": \"").Append(response.Message).Append("\"");

                    if ((200 == response.StatusCode) || (201 == response.StatusCode))
                    {
                        responseBuilder.Append(", \"").Append(response.Symbol).Append("\":").Append(response.Value);
                    }

                    responseBuilder.Append("}");
                    await httpResponse.WriteStringAsync(responseBuilder.ToString());
                }

                return httpResponse;
            }
            catch (Exception exception)
            {
                _logger.LogError(exception, exception.Message);
                var errorResponse = req.CreateResponse(HttpStatusCode.InternalServerError);
                errorResponse.Headers.Add("Content-Type", "application/json; charset=utf-8");
                await errorResponse.WriteStringAsync(JsonConvert.SerializeObject(new
                {
                    StatusCode = (int)HttpStatusCode.InternalServerError,
                    Message = $"{nameof(Ticker)} version {version} ERROR: {exception.Message}"
                }));
                return errorResponse;
            }
        }

    }
}
