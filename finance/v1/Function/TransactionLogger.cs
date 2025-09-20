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
using Microsoft.IdentityModel.Tokens;

namespace FalxGroup.Finance.Function
{
    public static class TransactionLogger
    {
        private static string version = "1.0.20";
        private static TransactionLoggerService processor = new TransactionLoggerService(Environment.GetEnvironmentVariable("SqlConnectionString"));

        [FunctionName("LogTransaction")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post", "put", "delete", Route = "finance/v1/log_transaction")] HttpRequest req,
            ExecutionContext executionContext,
            ILogger log)
        {
            string responseMessage = "";
            int statusCode = 500;

            try
            {
                TransactionLog record = null;

                if (req.Method.Equals("GET"))
                {
                    if (0 != req.Query.Count)
                    {
                        record = new TransactionLog
                        {
                            ApplicationKey = req.Query["application_key"],
                            ClientId = long.TryParse(req.Query["client_id"], out var clientId) ? clientId : 0,
                            UserId = long.TryParse(req.Query["user_id"], out var userId) ? userId : 0,
                            UserAccountId = long.TryParse(req.Query["user_account_id"], out var userAccount) ? userAccount : 0,
                            GetProcessType = int.TryParse(req.Query["get_process_type"], out var processType) ? processType : 0
                        };
                    }
                }
                else if (req.Method.Equals("POST") || req.Method.Equals("PUT") || req.Method.Equals("DELETE"))
                {
                    var jsonData = await new StreamReader(req.Body).ReadToEndAsync();
                    record = JsonConvert.DeserializeObject<TransactionLog>(jsonData);
                }
                else
                {
                    statusCode = 405;
                    responseMessage = JsonConvert.SerializeObject(new { StatusCode = statusCode, Message = $"{executionContext.FunctionName} version {version} METHOD {req.Method} NOT ALLOWED" });
                    return new ObjectResult(responseMessage) { StatusCode = statusCode };
                }

                if ((null != record) && record.ApplicationKey.Equals("e0e06109-0b3a-4e64-8fe9-1e1e23db0f5e"))
                {
                    switch (req.Method)
                    {
                        case "POST":
                        {
                            var response = await TransactionLogger.processor.LogTransaction(record);

                            if (1 == response.Item1)
                            {
                                statusCode = 201; // Created
                                responseMessage = JsonConvert.SerializeObject(new { StatusCode = statusCode, TransactionId = response.Item2 });
                            }
                            else
                            {
                                statusCode = 500;
                                responseMessage = JsonConvert.SerializeObject(new { StatusCode = statusCode, Message = $"METHOD {req.Method} Records inserted {response.Item1}" });
                            }
                            break;
                        }
                        case "PUT":
                        {
                            var response = await TransactionLogger.processor.UpdateTransaction(record);
                            if (1 == response.Item1)
                            {
                                statusCode = 200; // OK
                                responseMessage = JsonConvert.SerializeObject(new { StatusCode = statusCode, TransactionId = response.Item2 });
                            }
                            else
                            {
                                statusCode = 500;
                                responseMessage = JsonConvert.SerializeObject(new { StatusCode = statusCode, Message = $"METHOD {req.Method} Records updated {response.Item1}" });
                            }
                            break;
                        }
                        case "DELETE":
                        {
                            var response = await TransactionLogger.processor.DeleteTransaction(record);
                            if (1 == response.Item1)
                            {
                                statusCode = 200; // OK
                                responseMessage = JsonConvert.SerializeObject(new { StatusCode = statusCode, TransactionId = response.Item2 });
                            }
                            else
                            {
                                statusCode = 500;
                                responseMessage = JsonConvert.SerializeObject(new { StatusCode = statusCode, Message = $"Records deleted {response.Item1}" });
                            }
                            break;
                        }
                        case "GET":
                        {
                            if (2 == record.GetProcessType)
                            {
                                string jsonOpenPositions = await TransactionLogger.processor.GetOpenPositions(record);
                                if (jsonOpenPositions.IsNullOrEmpty() || jsonOpenPositions == "[]")
                                {
                                    statusCode = 204; // No Content
                                    responseMessage = JsonConvert.SerializeObject(new { StatusCode = statusCode });
                                }
                                else
                                {
                                    statusCode = 200; // OK
                                    responseMessage = $"{{\"StatusCode\": {statusCode}, \"OpenPositions\": {jsonOpenPositions}}}";
                                }
                            }
                            else
                            {
                                statusCode = 200; // OK, but different message
                                responseMessage = JsonConvert.SerializeObject(new { StatusCode = statusCode, Message = $"{executionContext.FunctionName} METHOD {req.Method} version {version}" });
                            }
                            break;
                        }
                    }
                }
                else
                {
                    if (req.Method.Equals("GET"))
                    {
                        statusCode = 200; // OK print version
                        responseMessage = JsonConvert.SerializeObject(new { StatusCode = statusCode, Message = $"{executionContext.FunctionName} version {version}" });
                    }
                    else
                    {
                        statusCode = 401; // Unauthorized
                        responseMessage = JsonConvert.SerializeObject(new { StatusCode = statusCode, Message = $"{executionContext.FunctionName} version {version} METHOD {req.Method} INVALID KEY" });
                    }
                }
                
                statusCode = 200; // because it is an application error not a infra system error
            }
            catch (Exception exception)
            {
                statusCode = 500;
                responseMessage = JsonConvert.SerializeObject(new
                {
                    StatusCode = statusCode,
                    Message = $"{executionContext.FunctionName} version {version} METHOD {req.Method} ERROR: {exception.Message}"
                });
                log.LogError(exception, exception.Message);
            }
            
            return new ContentResult { Content = responseMessage, ContentType = "application/json", StatusCode = statusCode };
        }
    }
}
