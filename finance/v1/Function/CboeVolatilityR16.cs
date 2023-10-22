using System;
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
using System.Linq;

namespace FalxGroup.Finance.Function
{
    public static class CboeVolatilityR16
    {
        private static string version = "1.0.0";

        private const string cboeIndexesMarketTicker = "INDEXCBOE";
        /*
            This function is used to apply the rule of 16 for the following CBOE indexes VIX, VVIX, VXN, VXD, RVX, MOVE, GVZ and OVX
         */
        private static readonly string[] cboeIndexes = { "VIX", "VVIX", "VXN", "VXD", "RVX", "GVZ", "OVX" };

        private static TickerService processor = new TickerService(10);

        [FunctionName("CboeVolatility")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", /* "post", */ Route = "finance/v1/cboe_volatility_r16/{symbol:alpha?}")] HttpRequest req,
            ExecutionContext executionContext,
            ILogger log, 
            string symbol)
        {
            StringBuilder responseBuilder = new StringBuilder("");

            try
            {
                var indexSymbol = (string.IsNullOrEmpty(symbol) ? "VIX" : symbol.ToUpper());

                var response = await CboeVolatilityR16.processor.Run(log, 
                    executionContext.FunctionName, version, 
                    (cboeIndexes.Any(validSymbol => validSymbol == indexSymbol) ? indexSymbol : "VIX"), 
                    cboeIndexesMarketTicker);

                responseBuilder.Append("{")
                    .Append("\"StatusCode\":").Append($"{response.StatusCode}")
                    .Append(", \"Message\": \"").Append(response.Message).Append("\"");

                if ((200 == response.StatusCode) || (201 == response.StatusCode))
                {
                    responseBuilder.Append(", \"").Append(response.Symbol).Append("\": ").Append(response.Value);

                    responseBuilder.Append(", \"daily expected volatility +/- (%)\": ")
                        .Append((Double.Parse(response.Value) / Math.Sqrt(252)).ToString("0.00"));
                    responseBuilder.Append(", \"weekly expected volatility +/- (%)\": ")
                        .Append((Double.Parse(response.Value) / Math.Sqrt(52)).ToString("0.00"));
                    responseBuilder.Append(", \"monthly expected volatility +/- (%)\": ")
                        .Append((Double.Parse(response.Value) / Math.Sqrt(12)).ToString("0.00"));
                    responseBuilder.Append(", \"yearly expected volatility +/- (%)\": ")
                        .Append(response.Value);
                }

                responseBuilder.Append("}");
            }
            catch (Exception exception)
            {
                log.LogError(exception: exception, exception.Message);
            }

            return new OkObjectResult(responseBuilder.ToString());
        }
    }
}
