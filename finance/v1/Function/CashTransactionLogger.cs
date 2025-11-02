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
using FalxGroup.Finance.Model;

namespace FalxGroup.Finance.Function
{
    public static class CashTransactionLogger
    {
        private static readonly string version = "1.0.0";
        private static readonly TransactionLoggerService processor = new TransactionLoggerService(Environment.GetEnvironmentVariable("SqlConnectionString"));

        [FunctionName("CashTransactionLogger")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "post", "put", "delete", /* "get", */ Route = "finance/v1/transactions/cash")] HttpRequest req,
            ExecutionContext executionContext,
            ILogger log)
        {
            string responseMessage = "";
            int statusCode = 500;

            try
            {
                var jsonData = await new StreamReader(req.Body).ReadToEndAsync();
                var record = JsonConvert.DeserializeObject<CashTransactionLog>(jsonData);

                // Basic validation and authorization check
                // In a real app, you'd get the app key from headers, not the body.
                // For now, we'll assume a placeholder check.
                if (record == null)
                {
                    return new BadRequestObjectResult(new { StatusCode = 400, Message = "Invalid request body." });
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
                    default:
                        statusCode = 405; // Method Not Allowed
                        responseMessage = JsonConvert.SerializeObject(new { StatusCode = statusCode, Message = $"Method {req.Method} not allowed." });
                        break;
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