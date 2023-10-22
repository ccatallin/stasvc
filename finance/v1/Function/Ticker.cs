using System.Text;
using System.Threading.Tasks;
// --
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
// --
using Microsoft.Extensions.Logging;
// --
using FalxGroup.Finance.Service;

namespace FalxGroup.Finance.Function
{
    public static class Ticker
    {
        private static string version = "1.0.3";
        private static TickerService processor = new TickerService(10);

        [FunctionName("Ticker")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", /* "post", */ Route = "finance/v1/ticker/{symbol:alpha?}/{market:alpha?}")] HttpRequest req,
            ExecutionContext executionContext,
            ILogger log,
            string symbol,
            string market)
        {
            var response = await Ticker.processor.Run(log, executionContext.FunctionName, version, symbol, market);

            StringBuilder responseBuilder = new StringBuilder();

            responseBuilder.Append("{")
                .Append("\"StatusCode\":").Append($"{response.StatusCode}")
                .Append(", \"Message\": \"").Append(response.Message).Append("\"");

            if ((200 == response.StatusCode) || (201 == response.StatusCode))
            {
                responseBuilder.Append(", \"").Append(response.Symbol).Append("\":").Append(response.Value);
            }

            responseBuilder.Append("}");

            return new OkObjectResult(responseBuilder.ToString());
        }

    }
}
