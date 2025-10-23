using System;
using System.IO;
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
using Microsoft.IdentityModel.Tokens;
using System.Globalization;

namespace FalxGroup.Finance.Function
{
    public static class TransactionLogger
    {
        private static string version = "1.0.30";
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
                            GetRequestId = int.TryParse(req.Query["get_request_id"], out var getRequestId) ? getRequestId : 0,
                            ProductCategoryId = int.TryParse(req.Query["product_category_id"], out var productCategoryId) ? productCategoryId : 0,
                            ProductId = int.TryParse(req.Query["product_id"], out var productId) ? productId : 0,
                            ProductSymbol = req.Query["product_symbol"],
                            StartDate = !String.IsNullOrEmpty(req.Query["start_date"]) ? DateTime.Parse(req.Query["start_date"], CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind) : (DateTime?)null,
                            EndDate = !String.IsNullOrEmpty(req.Query["end_date"]) ? DateTime.Parse(req.Query["end_date"], CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind) : (DateTime?)null
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
                            string postMode = string.Empty;
                            
                            try
                            {
                                postMode = req.Query["mode"].ToString()?.ToLower() ?? "normal";
                            }
                            catch
                            {
                                // ignore error and use whatever value was sent
                            }
                            
                            log.LogTrace($"POST mode: {postMode}");
                            var response = await TransactionLogger.processor.LogTransaction(record, postMode);

                            if (1 == response.Item1)
                            {
                                statusCode = 201; // Created
                                responseMessage = JsonConvert.SerializeObject(new { StatusCode = statusCode, Id = response.Item2 });
                            }
                            else if (-2 == response.Item1)
                            {
                                statusCode = 409; // Conflict
                                responseMessage = JsonConvert.SerializeObject(new { StatusCode = statusCode, Message = $"A product named '{record.ProductSymbol}' already exists with a different category or type." });
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
                            var response = await TransactionLogger.processor.UpdateTransactionLog(record);
                            if (1 == response.Item1)
                            {
                                statusCode = 200; // OK
                                responseMessage = JsonConvert.SerializeObject(new { StatusCode = statusCode, Id = response.Item2 });
                            }
                            else if (-2 == response.Item1)
                            {
                                statusCode = 409; // Conflict
                                responseMessage = JsonConvert.SerializeObject(new { StatusCode = statusCode, Message = $"Updating this transaction for product '{record.ProductSymbol}' would create a conflict with an existing product's category or type." });
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
                            var response = await TransactionLogger.processor.DeleteTransactionLog(record);
                            if (1 == response.Item1)
                            {
                                statusCode = 200; // OK
                                responseMessage = JsonConvert.SerializeObject(new { StatusCode = statusCode, Id = response.Item2 });
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
                            switch (record.GetRequestId)
                            {
                                case 1: // get raw transaction logs
                                {
                                    string jsonTransactionLogs = await TransactionLogger.processor.GetTransactionLogs(record);
                                    if (jsonTransactionLogs.IsNullOrEmpty() || jsonTransactionLogs == "[]")
                                    {
                                        statusCode = 204; // No Content
                                        responseMessage = JsonConvert.SerializeObject(new { StatusCode = statusCode });
                                    }
                                    else
                                    {
                                        statusCode = 200; // OK
                                        responseMessage = $"{{\"StatusCode\": {statusCode}, \"ReportData\": {jsonTransactionLogs}}}";
                                    }

                                    break;
                                }
                                case 2: // get open positions
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

                                    break;
                                }
                                case 3: // get product transaction logs
                                { // GetOpenPositionTransactionLogs
                                    string jsonProductTransactionLogs = await TransactionLogger.processor.GetProductTransactionLogs(record);
                                    if (jsonProductTransactionLogs.IsNullOrEmpty() || jsonProductTransactionLogs == "[]")
                                    {
                                        statusCode = 204; // No Content
                                        responseMessage = JsonConvert.SerializeObject(new { StatusCode = statusCode });
                                    }
                                    else
                                    {
                                        statusCode = 200; // OK
                                        responseMessage = $"{{\"StatusCode\": {statusCode}, \"ProductTransactionLogs\": {jsonProductTransactionLogs}}}";
                                    }

                                    break;
                                }
                                case 4: // get realized profit and loss
                                {
                                    string jsonReportData = await TransactionLogger.processor.GetRealizedProfitAndLoss(record);
                                    if (jsonReportData.IsNullOrEmpty() || jsonReportData == "[]")
                                    {
                                        statusCode = 204; // No Content
                                        responseMessage = JsonConvert.SerializeObject(new { StatusCode = statusCode });
                                    }
                                    else
                                    {
                                        statusCode = 200; // OK
                                        responseMessage = $"{{\"StatusCode\": {statusCode}, \"ReportData\": {jsonReportData}}}";
                                    }

                                    break;
                                }
                                case 5: // get open position transaction logs
                                { 
                                    string jsonOpenPositionTransactionLogs = await TransactionLogger.processor.GetOpenPositionTransactionLogs(record);
                                    if (jsonOpenPositionTransactionLogs.IsNullOrEmpty() || jsonOpenPositionTransactionLogs == "[]")
                                    {
                                        statusCode = 204; // No Content
                                        responseMessage = JsonConvert.SerializeObject(new { StatusCode = statusCode });
                                    }
                                    else
                                    {
                                        statusCode = 200; // OK
                                        responseMessage = $"{{\"StatusCode\": {statusCode}, \"OpenPositionTransactionLogs\": {jsonOpenPositionTransactionLogs}}}";
                                    }

                                    break;
                                }
                                default:
                                {
                                    statusCode = 200; // OK, but different message
                                    responseMessage = JsonConvert.SerializeObject(new { StatusCode = statusCode, Message = $"{executionContext.FunctionName} METHOD {req.Method} version {version}" });

                                    break;
                                }
                            } // end switch on GET method 

                            break;
                        } // end switch GET method option
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
