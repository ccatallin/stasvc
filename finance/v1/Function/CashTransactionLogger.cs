using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using FalxGroup.Finance.Service;
using System.Globalization;
using FalxGroup.Finance.Model;

namespace FalxGroup.Finance.Function
{
    public static class CashTransactionLogger
    {
        private static readonly string version = "1.0.1";
        private static readonly TransactionLoggerService processor = new TransactionLoggerService(Environment.GetEnvironmentVariable("SqlConnectionString"));

        [FunctionName("CashTransactionLogger")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post", "put", "delete", Route = "finance/v1/transactions/cash")] HttpRequest req,
            ExecutionContext executionContext,
            ILogger log)
        {
            string responseMessage = "";
            int statusCode = 500;

            try
            {
                CashTransactionLog record = null;

                if (req.Method.Equals("GET"))
                {
                    record = new CashTransactionLog
                    {
                        Id = req.Query["id"],
                        GetRequestId = int.TryParse(req.Query["get_request_id"], out var getRequestId) ? getRequestId : 0,
                        ClientId = long.TryParse(req.Query["client_id"], out var clientId) ? clientId : 0,
                        UserId = long.TryParse(req.Query["user_id"], out var userId) ? userId : 0,
                        StartDate = !string.IsNullOrEmpty(req.Query["start_date"]) ? DateTime.Parse(req.Query["start_date"], CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind) : (DateTime?)null,
                        EndDate = !string.IsNullOrEmpty(req.Query["end_date"]) ? DateTime.Parse(req.Query["end_date"], CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind) : (DateTime?)null
                    };
                }
                else
                {
                    var jsonData = await new StreamReader(req.Body).ReadToEndAsync();
                    record = JsonConvert.DeserializeObject<CashTransactionLog>(jsonData);
                }

                // Basic validation and authorization check
                // In a real app, you'd get the app key from headers, not the body.
                // For now, we'll assume a placeholder check.
                if (record == null)
                {
                    statusCode = 400;
                    responseMessage = JsonConvert.SerializeObject(new { StatusCode = statusCode, Message = "Invalid request body or query parameters." });
                    return new ContentResult { Content = responseMessage, ContentType = "application/json", StatusCode = statusCode };
                }

                // This is a placeholder for a real authorization check.
                // if (!IsValidRequest(req)) 
                // {
                //     return new UnauthorizedResult();
                // }

                switch (req.Method)
                {
                    case "POST":
                    {
                        var response = await processor.LogCashTransaction(record);

                        if (response.Item1 == 1)
                        {
                            statusCode = 201; // Created
                            responseMessage = JsonConvert.SerializeObject(new { StatusCode = statusCode, id = response.Item2 });
                        }
                        else
                        {
                            statusCode = 500;
                            responseMessage = JsonConvert.SerializeObject(new { StatusCode = statusCode, Message = $"Failed to create record. Records inserted: {response.Item1}" });
                        }
                        break;
                    }
                    case "PUT":
                    {
                        var response = await processor.UpdateCashTransaction(record);
                        if (response.Item1 == 1)
                        {
                            statusCode = 200; // OK
                            responseMessage = JsonConvert.SerializeObject(new { StatusCode = statusCode, id = response.Item2 });
                        }
                        else
                        {
                            statusCode = 404; // Not Found, or use 500 if it's a general failure
                            responseMessage = JsonConvert.SerializeObject(new { StatusCode = statusCode, Message = $"Record not found or failed to update. Records updated: {response.Item1}" });
                        }
                        break;
                    }
                    case "DELETE":
                    {
                        var response = await processor.DeleteCashTransaction(record);
                        if (response.Item1 == 1)
                        {
                            statusCode = 200; // OK
                            responseMessage = JsonConvert.SerializeObject(new { StatusCode = statusCode, id = response.Item2 });
                        }
                        else
                        {
                            statusCode = 404; // Not Found
                            responseMessage = JsonConvert.SerializeObject(new { StatusCode = statusCode, Message = $"Record not found or failed to delete. Records deleted: {response.Item1}" });
                        }
                        break;
                    }
                    case "GET":
                    {
                        switch (record.GetRequestId)
                        {
                            case 1: // get raw cash transaction logs
                            {
                                string jsonCashTransactionLogs = await processor.GetCashTransactionLogs(record);
                                if (string.IsNullOrEmpty(jsonCashTransactionLogs) || jsonCashTransactionLogs == "[]")
                                {
                                    statusCode = 204; // No Content
                                    responseMessage = JsonConvert.SerializeObject(new { StatusCode = statusCode });
                                }
                                else
                                {
                                    statusCode = 200; // OK
                                    responseMessage = $"{{\"StatusCode\": {statusCode}, \"ReportData\": {jsonCashTransactionLogs}}}";
                                }
                                
                                break;
                            }
                            case 2: // get cash transaction categories
                            {
                                string jsonCashTransactionCategories = await processor.GetCashTransactionCategories();
                                if (string.IsNullOrEmpty(jsonCashTransactionCategories) || jsonCashTransactionCategories == "[]")
                                {
                                    statusCode = 204; // No Content
                                    responseMessage = JsonConvert.SerializeObject(new { StatusCode = statusCode });
                                }
                                else
                                {
                                    statusCode = 200; // OK
                                    responseMessage = $"{{\"StatusCode\": {statusCode}, \"Categories\": {jsonCashTransactionCategories}}}";
                                }
                                break;
                            }
                            default:
                            {
                                statusCode = 200; // OK, but different message
                                responseMessage = JsonConvert.SerializeObject(new { StatusCode = statusCode, Message = $"{executionContext.FunctionName} METHOD {req.Method} version {version}" });

                                break;                             
                            }
                        }                

                        break;
                    }
                    default:
                    {
                        statusCode = 405; // Method Not Allowed
                        responseMessage = JsonConvert.SerializeObject(new { StatusCode = statusCode, Message = $"Method {req.Method} not allowed." });

                        break;
                    }
                }
            }
            catch (JsonException jsonEx)
            {
                statusCode = 400; // Bad Request
                responseMessage = JsonConvert.SerializeObject(new { StatusCode = statusCode, Message = $"Invalid JSON format: {jsonEx.Message}" });
                log.LogWarning(jsonEx, "JSON parsing error.");
            }
            catch (Exception exception)
            {
                statusCode = 500;
                responseMessage = JsonConvert.SerializeObject(new
                {
                    StatusCode = statusCode,
                    Message = $"{executionContext.FunctionName} v{version} ERROR: {exception.Message}"
                });
                log.LogError(exception, exception.Message);
            }

            return new ContentResult { Content = responseMessage, ContentType = "application/json", StatusCode = statusCode };
        }
    }
}