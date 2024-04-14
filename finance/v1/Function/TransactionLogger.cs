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
    public static class TransactionLogger
    {
        private static string version = "1.0.0";
        private static TransactionLoggerService processor = new TransactionLoggerService();

        [FunctionName("TransactionLogger")]
        public static async Task<IActionResult> Run([HttpTrigger(AuthorizationLevel.Anonymous, "post", 
            Route = "finance/v1/transaction-logger/{beginDate:alpha?}/{endDate:alpha?}")] HttpRequest req,
            ExecutionContext executionContext,
            ILogger log,
            string symbol,
            string market)
        {
            var response = await TransactionLogger.processor.Run(log, executionContext.FunctionName, version);

            StringBuilder responseBuilder = new StringBuilder();

            responseBuilder.Append("{")
                .Append("\"StatusCode\":").Append($"{response.StatusCode}")
                .Append(", \"Message\": \"").Append(response.Message).Append("\"");

            return new OkObjectResult(responseBuilder.ToString());
        }
    }
}
