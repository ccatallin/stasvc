using System;
using System.IO;
using System.Threading.Tasks;
using System.Net;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using FalxGroup.Finance.Service;
using System.Globalization;
using Microsoft.IdentityModel.Tokens;
using FalxGroup.Finance.Model;

namespace FalxGroup.Finance.Function
{
    public class CashTransactionLogger
    {
        private const string version = "2.0.1-isolated";
        private readonly ILogger _logger;
        private readonly TransactionLoggerService _processor;

        public CashTransactionLogger(ILoggerFactory loggerFactory, TransactionLoggerService processor)
        {
            _logger = loggerFactory.CreateLogger<CashTransactionLogger>();
            _processor = processor;
        }

        [Function("CashTransactionLogger")]
        public async Task<HttpResponseData> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post", "put", "delete", Route = "finance/v1/transactions/cash")] HttpRequestData req)
        {
            string responseMessage = "";
            var statusCode = HttpStatusCode.InternalServerError;
            var functionName = nameof(CashTransactionLogger);

            try
            {
                CashTransactionLog? record = null;

                if (req.Method.Equals("GET"))
                {
                    record = new CashTransactionLog
                    {
                        Id = req.Query["id"],
                        GetRequestId = int.TryParse(req.Query["get_request_id"], out var getRequestId) ? getRequestId : 0,
                        ClientId = long.TryParse(req.Query["client_id"], out var clientId) ? clientId : 0,
                        UserId = long.TryParse(req.Query["user_id"], out var userId) ? userId : 0,
                        StartDate = !string.IsNullOrEmpty(req.Query["start_date"]) ? DateTime.Parse(req.Query["start_date"]!, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind) : (DateTime?)null,
                        EndDate = !string.IsNullOrEmpty(req.Query["end_date"]) ? DateTime.Parse(req.Query["end_date"]!, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind) : (DateTime?)null
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
                    statusCode = HttpStatusCode.BadRequest;
                    responseMessage = JsonConvert.SerializeObject(new { StatusCode = statusCode, Message = "Invalid request body or query parameters." });
                    return await CreateResponse(req, HttpStatusCode.BadRequest, responseMessage);
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
                        var response = await _processor.LogCashTransaction(record);

                        if (response.Item1 == 1)
                        {
                            statusCode = HttpStatusCode.Created;
                            responseMessage = JsonConvert.SerializeObject(new { Id = response.Item2, StatusCode = (int)statusCode, CashBalance = response.Item3 });
                        }
                        else
                        {
                            statusCode = HttpStatusCode.InternalServerError;
                            responseMessage = JsonConvert.SerializeObject(new { StatusCode = (int)statusCode, Message = $"Failed to create record. Records inserted: {response.Item1}" });
                        }
                        break;
                    }
                    case "PUT":
                    {
                        var response = await _processor.UpdateCashTransaction(record);
                        if (response.Item1 == 1)
                        {
                            statusCode = HttpStatusCode.OK;
                            responseMessage = JsonConvert.SerializeObject(new { Id = response.Item2, StatusCode = (int)statusCode, CashBalance = response.Item3 });
                        }
                        else
                        {
                            statusCode = HttpStatusCode.NotFound; // Not Found, or use 500 if it's a general failure
                            responseMessage = JsonConvert.SerializeObject(new { StatusCode = (int)statusCode, Message = $"Record not found or failed to update. Records updated: {response.Item1}" });
                        }
                        break;
                    }
                    case "DELETE":
                    {
                        var response = await _processor.DeleteCashTransaction(record);
                        if (response.Item1 == 1)
                        {
                            statusCode = HttpStatusCode.OK;
                            responseMessage = JsonConvert.SerializeObject(new { Id = response.Item2, StatusCode = (int)statusCode, CashBalance = response.Item3 });
                        }
                        else
                        {
                            statusCode = HttpStatusCode.NotFound;
                            responseMessage = JsonConvert.SerializeObject(new { StatusCode = (int)statusCode, Message = $"Record not found or failed to delete. Records deleted: {response.Item1}" });
                        }
                        break;
                    }
                    case "GET":
                    {
                        switch (record.GetRequestId)
                        {
                            case 1: // get raw cash transaction logs
                            {
                                string jsonCashTransactionLogs = await _processor.GetCashTransactionLogs(record);
                                if (string.IsNullOrEmpty(jsonCashTransactionLogs) || jsonCashTransactionLogs == "[]")
                                {
                                    statusCode = HttpStatusCode.NoContent;
                                    responseMessage = JsonConvert.SerializeObject(new { StatusCode = (int)statusCode });
                                }
                                else
                                {
                                    statusCode = HttpStatusCode.OK;
                                    responseMessage = $"{{\"StatusCode\": {(int)statusCode}, \"ReportData\": {jsonCashTransactionLogs}}}";
                                }
                                
                                break;
                            }
                            case 2: // get cash transaction categories
                            {
                                string jsonCashTransactionCategories = await _processor.GetCashTransactionCategories();
                                if (string.IsNullOrEmpty(jsonCashTransactionCategories) || jsonCashTransactionCategories == "[]")
                                {
                                    statusCode = HttpStatusCode.NoContent;
                                    responseMessage = JsonConvert.SerializeObject(new { StatusCode = (int)statusCode });
                                }
                                else
                                {
                                    statusCode = HttpStatusCode.OK;
                                    responseMessage = $"{{\"StatusCode\": {(int)statusCode}, \"Categories\": {jsonCashTransactionCategories}}}";
                                }
                                break;
                            }
                            case 6: // get transaction log by id (6 it's the same as for security transactions logs)
                            { 
                                string jsonCashTransactionLog = await _processor.GetCashTransactionLogById(record);
                                if (jsonCashTransactionLog.IsNullOrEmpty() || jsonCashTransactionLog == "[]")
                                {
                                    statusCode = HttpStatusCode.NoContent;
                                    responseMessage = JsonConvert.SerializeObject(new { StatusCode = (int)statusCode });
                                }
                                else
                                {
                                    statusCode = HttpStatusCode.OK;
                                    responseMessage = $"{{\"StatusCode\": {(int)statusCode}, \"CashTransactionLog\": {jsonCashTransactionLog}}}";
                                }

                                break;
                            }
                            default:
                            {
                                statusCode = HttpStatusCode.OK;
                                responseMessage = JsonConvert.SerializeObject(new { StatusCode = (int)statusCode, Message = $"{functionName} METHOD {req.Method} version {version}" });

                                break;                             
                            }
                        }                

                        break;
                    }
                    default:
                    {
                        statusCode = HttpStatusCode.MethodNotAllowed;
                        responseMessage = JsonConvert.SerializeObject(new { StatusCode = (int)statusCode, Message = $"Method {req.Method} not allowed." });

                        break;
                    }
                }
            }
            catch (JsonException jsonEx)
            {
                statusCode = HttpStatusCode.BadRequest;
                responseMessage = JsonConvert.SerializeObject(new { StatusCode = (int)statusCode, Message = $"Invalid JSON format: {jsonEx.Message}" });
                _logger.LogWarning(jsonEx, "JSON parsing error.");
            }
            catch (Exception exception)
            {
                statusCode = HttpStatusCode.InternalServerError;
                responseMessage = JsonConvert.SerializeObject(new
                {
                    StatusCode = (int)statusCode,
                    Message = $"{functionName} v{version} ERROR: {exception.Message}"
                });
                _logger.LogError(exception, exception.Message);
            }

            return await CreateResponse(req, statusCode, responseMessage);
        }

        private async Task<HttpResponseData> CreateResponse(HttpRequestData req, HttpStatusCode statusCode, string message)
        {
            var response = req.CreateResponse(statusCode);
            response.Headers.Add("Content-Type", "application/json; charset=utf-8");
            await response.WriteStringAsync(message);
            return response;
        }
    }
}