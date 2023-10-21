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
    public static class VxnCboeFunction
    {
        private static string version = "1.0.0";
        private static TickerService processor = new TickerService(10);       

        [FunctionName("VXN")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", /* "post", */ Route = "finance/v1/vxn")] HttpRequest req,
            ExecutionContext executionContext,
            ILogger log)
        {
            var response = await VxnCboeFunction.processor.Run(log, executionContext.FunctionName, version, "VXN", "INDEXCBOE");
            
            StringBuilder responseBuilder = new StringBuilder("");

            try 
            {
                responseBuilder.Append("{")
                    .Append("\"StatusCode\":").Append($"{response.StatusCode}")
                    .Append(", \"Message\": \"").Append(response.Message).Append("\"");

                if ((200 == response.StatusCode) || (201 == response.StatusCode))
                {
                    responseBuilder.Append(", \"").Append(response.Symbol).Append("\": ").Append(response.Value);

                    responseBuilder.Append(", \"S&P index daily expected volatility +/- (%)\": ").Append((Double.Parse(response.Value) / Math.Sqrt(252)).ToString("0.00"));
                    responseBuilder.Append(", \"S&P index weekly expected volatility +/- (%)\": ").Append((Double.Parse(response.Value) / Math.Sqrt(52)).ToString("0.00"));
                    responseBuilder.Append(", \"S&P index monthly expected volatility +/- (%)\": ").Append((Double.Parse(response.Value) / Math.Sqrt(12)).ToString("0.00"));
                    responseBuilder.Append(", \"S&P index yearly expected volatility +/- (%)\": ").Append(response.Value);
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
