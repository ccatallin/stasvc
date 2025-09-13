using System;
using System.IO;
using System.Text;
// using System.Text.Json;
using System.Threading.Tasks;
using System.Net.Http;
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
        private static string version = "1.0.9";
        private static TransactionLoggerService processor = new TransactionLoggerService(Environment.GetEnvironmentVariable("SqlConnectionString"));

        [FunctionName("LogTransaction")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post", "put", "delete", Route = "finance/v1/log_transaction")] HttpRequest req,
            ExecutionContext executionContext,
            ILogger log)
        {
            StringBuilder responseBuilder = new StringBuilder();

            if (req.Method.Equals("POST") || req.Method.Equals("PUT") || req.Method.Equals("DELETE") || req.Method.Equals("GET"))
            {
                try
                {
                    var jsonData = await new StreamReader(req.Body).ReadToEndAsync();
                    TransactionLog record = JsonConvert.DeserializeObject<TransactionLog>(jsonData);

                    if ((null != record) && record.ApplicationKey.Equals("e0e06109-0b3a-4e64-8fe9-1e1e23db0f5e"))
                    {
                        switch (req.Method)
                        {
                            case "POST":
                            {
                                var response = await TransactionLogger.processor.LogTransaction(record);

                                if (1 == response.Item1)
                                {
                                    responseBuilder.Append("{")
                                            .Append("\"StatusCode\": 200")
                                            .Append($", \"TransactionId\": \"{response.Item2}\"")
                                        .Append("}");
                                }
                                else
                                {
                                    responseBuilder.Append("{")
                                            .Append("\"StatusCode\": 500")
                                            .Append($", \"Message\": METHOD {req.Method.ToString()} \"Records inserted {response.Item1}\"")
                                        .Append("}");
                                }

                                break;
                            }
                            case "PUT":
                            {
                                var response = await TransactionLogger.processor.UpdateTransaction(record);

                                if (1 == response.Item1)
                                {
                                    responseBuilder.Append("{")
                                            .Append("\"StatusCode\": 200")
                                            .Append($", \"TransactionId\": \"{response.Item2}\"")
                                        .Append("}");
                                }
                                else
                                {
                                    responseBuilder.Append("{")
                                            .Append("\"StatusCode\": 500")
                                            .Append($", \"Message\": METHOD {req.Method.ToString()} \"Records updated {response.Item1}\"")
                                        .Append("}");
                                }

                                break;
                            }
                            case "DELETE":
                            {
                                var response = await TransactionLogger.processor.DeleteTransaction(record);

                                if (1 == response.Item1)
                                {
                                    responseBuilder.Append("{")
                                            .Append("\"StatusCode\": 200")
                                            .Append($", \"TransactionId\": \"{response.Item2}\"")
                                        .Append("}");
                                }
                                else
                                {
                                    responseBuilder.Append("{")
                                            .Append("\"StatusCode\": 500")
                                            .Append($", \"Message\": \"Records deleted {response.Item1}\"")
                                        .Append("}");
                                }

                                break;
                            }
                            case "GET":
                            {
                                responseBuilder.Append("{")
                                        .Append("\"StatusCode\": 204")
                                        .Append(", \"Message\": \"").Append($"{executionContext.FunctionName} METHOD {req.Method.ToString()} version {version}\"")
                                    .Append("}");
                                break;
                            }
                            default:
                            {
                                responseBuilder.Append("{")
                                        .Append("\"StatusCode\": 405")
                                        .Append($", \"Message\": METHOD {req.Method.ToString()} \"Method Not Allowed\"")
                                    .Append("}");
                                log.LogError("405 Method Not Allowed");
                                break;
                            }
                        } // end switch
                    } 
                    else
                    {
                        responseBuilder.Append("{")
                                .Append("\"StatusCode\": 204")
                                .Append(", \"Message\": \"").Append($"{executionContext.FunctionName} METHOD {req.Method.ToString()} version {version}\"")
                            .Append("}");
                    } // end valid application key
                }
                catch (Exception exception)
                {
                    responseBuilder.Append("{")
                            .Append("\"StatusCode\": 500")
                            .Append(", \"Message\": \"").Append(" METHOD {req.Method.ToString()} ").Append(exception.Message)
                        .Append("\"}");
                    log.LogError(exception.Message);
                }
            }
            else
            {
                responseBuilder.Append("{")
                        .Append("\"StatusCode\": 204")
                        .Append(", \"Message\": \"").Append($"{executionContext.FunctionName} METHOD {req.Method.ToString()} version {version}\"")
                    .Append("}");
            }

            return new OkObjectResult(responseBuilder.ToString());
        }
    }
}
