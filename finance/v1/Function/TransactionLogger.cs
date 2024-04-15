using System;
using System.IO;
using System.Text;
// using System.Text.Json;
using System.Threading.Tasks;
// --
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
// --
using Microsoft.Extensions.Logging;
// --
using Newtonsoft.Json;
// --
using FalxGroup.Finance.Service;
using FalxGroup.Finance.Model;

namespace FalxGroup.Finance.Function
{
    public static class TransactionLogger
    {
        private static string version = "1.0.0";
        private static TransactionLoggerService processor = new TransactionLoggerService(Environment.GetEnvironmentVariable("SqlConnectionString"));

        [FunctionName("LogTransaction")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post", Route = "finance/v1/log_transaction")] HttpRequest req,
            ExecutionContext executionContext,
            ILogger log)
        {
            StringBuilder responseBuilder = new StringBuilder();

            if (req.Method.Equals("POST"))
            {
                try
                {
                    var jsonData = await new StreamReader(req.Body).ReadToEndAsync();
                    TransactionLog record = JsonConvert.DeserializeObject<TransactionLog>(jsonData);
                    
                    var response = await TransactionLogger.processor.LogTransaction(record);
                    
                    if (1 == response) 
                    {
                        responseBuilder.Append("{").Append("\"StatusCode\": 200").Append(", \"Message\": \"\"}");
                    } 
                    else
                    {
                        responseBuilder.Append("{").Append("\"StatusCode\": 500").Append($", \"Message\": \"Records inserted {response}\"").Append("}");
                    }
                }
                catch (Exception exception)
                {
                    responseBuilder.Append("{").Append("\"StatusCode\": 500").Append(", \"Message\": \"").Append(exception.Message).Append("\"}");
                }
            }
            else
            {
                responseBuilder.Append("{").Append("\"StatusCode\": 204")
                    .Append(", \"Message\": \"").Append($"{executionContext.FunctionName} version {version}\"")
                    .Append("}");
            }

            return new OkObjectResult(responseBuilder.ToString());
        }
    }
}
