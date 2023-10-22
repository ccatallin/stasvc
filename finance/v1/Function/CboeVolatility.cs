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

namespace FalxGroup.Finance.Function
{
    public static class CboeVolatility
    {
        private const string market = "INDEXCBOE";
        private static string version = "1.0.0";
        private static TickerService processor = new TickerService(10);

        /*
            This function is used to return the information for VVIX, VIX, VXN, VXD, RVX, MOVE, GVZ and OVX
         */

        [FunctionName("CboeVolatility")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", /* "post", */ Route = "finance/v1/cboe_volatility/{symbol:alpha?}")] HttpRequest req,
            ExecutionContext executionContext,
            ILogger log, 
            string symbol)
        {
            StringBuilder responseBuilder = new StringBuilder("");

            try
            {
                var response = await CboeVolatility.processor.Run(log, 
                    executionContext.FunctionName, version, 
                    (string.IsNullOrEmpty(symbol) ? "VIX" : symbol.ToUpper()), market);

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
