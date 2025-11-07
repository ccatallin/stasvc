using System.Text;
using System.Threading.Tasks;
using System.Net;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Logging;
// --
using FalxGroup.Finance.Service;

namespace FalxGroup.Finance.Function
{
    public class Ticker
    {
        private const string version = "2.0.0-isolated";
        private readonly TickerService _processor;
        private readonly ILogger _logger;

        public Ticker(ILoggerFactory loggerFactory, TickerService processor)
        {
            _logger = loggerFactory.CreateLogger<Ticker>();
            _processor = processor;
        }

        [Function("Ticker")]
        public async Task<HttpResponseData> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", /* "post", */ Route = "finance/v1/ticker/{symbol:alpha?}/{market:alpha?}")] HttpRequestData req,
            string? symbol,
            string? market)
        {
            var response = await _processor.Run(_logger, nameof(Ticker), version, symbol, market);

            StringBuilder responseBuilder = new StringBuilder();

            responseBuilder.Append("{")
                .Append("\"StatusCode\":").Append($"{response.StatusCode}")
                .Append(", \"Message\": \"").Append(response.Message).Append("\"");

            if ((200 == response.StatusCode) || (201 == response.StatusCode))
            {
                responseBuilder.Append(", \"").Append(response.Symbol).Append("\":").Append(response.Value);
            }

            responseBuilder.Append("}");

            var httpResponse = req.CreateResponse(HttpStatusCode.OK);
            httpResponse.Headers.Add("Content-Type", "application/json; charset=utf-8");
            await httpResponse.WriteStringAsync(responseBuilder.ToString());
            return httpResponse;
        }

    }
}
