using System;
using System.IO;
using System.Threading.Tasks;
using System.Net;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
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
    public class SecurityTransactionLogger
    {
        private const string version = "2.0.4-isolated";
        private readonly ILogger _logger;
        private readonly TransactionLoggerService _processor;

        public SecurityTransactionLogger(ILoggerFactory loggerFactory, TransactionLoggerService processor)
        {
            _logger = loggerFactory.CreateLogger<SecurityTransactionLogger>();
            _processor = processor;
        }

        [Function("SecurityTransactionLogger")]
        public async Task<HttpResponseData> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post", "put", "delete", Route = "finance/v1/transactions/securities")] HttpRequestData req)
        {
            string responseMessage = "";
            var statusCode = HttpStatusCode.InternalServerError;

            var functionName = nameof(SecurityTransactionLogger);
            try
            {
                SecurityTransactionLog? record = null;

                if (req.Method.Equals("GET"))
                {
                    if (0 != req.Query.Count)
                    {
                        record = new SecurityTransactionLog
                        {
                            Id = req.Query["id"] ?? null,
                            ApplicationKey = req.Query["application_key"] ?? null,
                            ClientId = long.TryParse(req.Query["client_id"], out var clientId) ? clientId : 0,
                            UserId = long.TryParse(req.Query["user_id"], out var userId) ? userId : 0,
                            UserAccountId = long.TryParse(req.Query["user_account_id"], out var userAccount) ? userAccount : 0,
                            GetRequestId = int.TryParse(req.Query["get_request_id"], out var getRequestId) ? getRequestId : 0,
                            ProductCategoryId = int.TryParse(req.Query["product_category_id"], out var productCategoryId) ? productCategoryId : 0,
                            ProductId = int.TryParse(req.Query["product_id"], out var productId) ? productId : 0,
                            ProductSymbol = req.Query["product_symbol"] ?? null,
                            StartDate = !String.IsNullOrEmpty(req.Query["start_date"]) ? DateTime.Parse(req.Query["start_date"]!, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind) : (DateTime?)null,
                            EndDate = !String.IsNullOrEmpty(req.Query["end_date"]) ? DateTime.Parse(req.Query["end_date"]!, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind) : (DateTime?)null
                        };
                    }
                }
                else if (req.Method.Equals("POST") || req.Method.Equals("PUT") || req.Method.Equals("DELETE"))
                {
                    var jsonData = await new StreamReader(req.Body).ReadToEndAsync();
                    record = JsonConvert.DeserializeObject<SecurityTransactionLog>(jsonData);
                }
                else
                {
                    statusCode = HttpStatusCode.MethodNotAllowed;
                    responseMessage = JsonConvert.SerializeObject(new { StatusCode = statusCode, Message = $"{functionName} version {version} METHOD {req.Method} NOT ALLOWED" });
                    return await CreateResponse(req, statusCode, responseMessage);
                }

                if (record != null && "e0e06109-0b3a-4e64-8fe9-1e1e23db0f5e".Equals(record.ApplicationKey))
                {
                    switch (req.Method)
                    {
                        case "POST":
                        {
                            string postMode = req.Query["mode"]?.ToLower() ?? "normal";
                            
                            _logger.LogTrace($"POST mode: {postMode}");
                            var response = await _processor.LogTransaction(record, postMode);

                            if (1 == response.Item1)
                            {
                                statusCode = HttpStatusCode.Created;
                                responseMessage = JsonConvert.SerializeObject(new { Id = response.Item2, StatusCode = (int)statusCode, CashBalance = response.Item3 });
                            }
                            else if (-2 == response.Item1)
                            {
                                statusCode = HttpStatusCode.Conflict;
                                responseMessage = JsonConvert.SerializeObject(new { StatusCode = (int)statusCode, Message = $"A product named '{record.ProductSymbol ?? "unknown"}' already exists with a different category or type." });
                            }
                            else
                            {
                                statusCode = HttpStatusCode.InternalServerError;
                                responseMessage = JsonConvert.SerializeObject(new { StatusCode = (int)statusCode, Message = $"METHOD {req.Method} Records inserted {response.Item1}" });
                            }

                            break;
                        }
                        case "PUT":
                        {
                            var response = await _processor.UpdateTransactionLog(record);
                            if (1 == response.Item1)
                            {
                                statusCode = HttpStatusCode.OK;
                                responseMessage = JsonConvert.SerializeObject(new { Id = response.Item2, StatusCode = (int)statusCode, CashBalance = response.Item3 });
                            }
                            else if (-2 == response.Item1)
                            {
                                statusCode = HttpStatusCode.Conflict;
                                responseMessage = JsonConvert.SerializeObject(new { StatusCode = (int)statusCode, Message = $"Updating this transaction for product '{record.ProductSymbol ?? "unknown"}' would create a conflict with an existing product's category or type." });
                            }
                            else
                            {
                                statusCode = HttpStatusCode.InternalServerError;
                                responseMessage = JsonConvert.SerializeObject(new { StatusCode = (int)statusCode, Message = $"METHOD {req.Method} Records updated {response.Item1}" });
                            }

                            break;
                        }
                        case "DELETE":
                        {
                            var response = await _processor.DeleteTransactionLog(record);
                            if (1 == response.Item1)
                            {
                                statusCode = HttpStatusCode.OK;
                                responseMessage = JsonConvert.SerializeObject(new { Id = response.Item2, StatusCode = (int)statusCode, CashBalance = response.Item3 });
                            }
                            else
                            {
                                statusCode = HttpStatusCode.InternalServerError;
                                responseMessage = JsonConvert.SerializeObject(new { StatusCode = (int)statusCode, Message = $"Records deleted {response.Item1}" });
                            }

                            break;
                        }
                        case "GET":
                        {
                            switch (record.GetRequestId)
                            {
                                case 1: // get raw transaction logs
                                {
                                    string jsonTransactionLogs = await _processor.GetTransactionLogs(record);
                                    if (jsonTransactionLogs.IsNullOrEmpty() || jsonTransactionLogs == "[]")
                                    {
                                        statusCode = HttpStatusCode.NoContent;
                                        responseMessage = JsonConvert.SerializeObject(new { StatusCode = (int)statusCode });
                                    }
                                    else
                                    {
                                        statusCode = HttpStatusCode.OK;
                                        responseMessage = $"{{\"StatusCode\": {(int)statusCode}, \"ReportData\": {jsonTransactionLogs}}}";
                                    }

                                    break;
                                }
                                case 2: // get open positions
                                {
                                    string jsonOpenPositions = await _processor.GetOpenPositions(record);
                                    if (jsonOpenPositions.IsNullOrEmpty() || jsonOpenPositions == "[]")
                                    {
                                        statusCode = HttpStatusCode.NoContent;
                                        responseMessage = JsonConvert.SerializeObject(new { StatusCode = (int)statusCode });
                                    }
                                    else
                                    {
                                        statusCode = HttpStatusCode.OK; // This is 200
                                        responseMessage = $"{{\"StatusCode\": {(int)statusCode}, \"OpenPositions\": {jsonOpenPositions}}}";
                                    }

                                    break;
                                }
                                case 3: // get product transaction logs
                                { // GetOpenPositionTransactionLogs
                                    string jsonProductTransactionLogs = await _processor.GetProductTransactionLogs(record);
                                    if (jsonProductTransactionLogs.IsNullOrEmpty() || jsonProductTransactionLogs == "[]")
                                    {
                                        statusCode = HttpStatusCode.NoContent;
                                        responseMessage = JsonConvert.SerializeObject(new { StatusCode = (int)statusCode });
                                    }
                                    else
                                    {
                                        statusCode = HttpStatusCode.OK;
                                        responseMessage = $"{{\"StatusCode\": {(int)statusCode}, \"ProductTransactionLogs\": {jsonProductTransactionLogs}}}";
                                    }

                                    break;
                                }
                                case 4: // get realized profit and loss
                                {
                                    string jsonReportData = await _processor.GetRealizedProfitAndLoss(record);
                                    if (jsonReportData.IsNullOrEmpty() || jsonReportData == "[]")
                                    {
                                        statusCode = HttpStatusCode.NoContent;
                                        responseMessage = JsonConvert.SerializeObject(new { StatusCode = (int)statusCode });
                                    }
                                    else
                                    {
                                        statusCode = HttpStatusCode.OK;
                                        responseMessage = $"{{\"StatusCode\": {(int)statusCode}, \"ReportData\": {jsonReportData}}}";
                                    }

                                    break;
                                }
                                case 5: // get open position transaction logs
                                { 
                                    string jsonOpenPositionTransactionLogs = await _processor.GetOpenPositionTransactionLogs(record);
                                    if (jsonOpenPositionTransactionLogs.IsNullOrEmpty() || jsonOpenPositionTransactionLogs == "[]")
                                    {
                                        statusCode = HttpStatusCode.NoContent;
                                        responseMessage = JsonConvert.SerializeObject(new { StatusCode = (int)statusCode });
                                    }
                                    else
                                    {
                                        statusCode = HttpStatusCode.OK;
                                        responseMessage = $"{{\"StatusCode\": {(int)statusCode}, \"OpenPositionTransactionLogs\": {jsonOpenPositionTransactionLogs}}}";
                                    }

                                    break;
                                }
                                case 6: // get transaction log by id
                                { 
                                    string jsonTransactionLog = await _processor.GetTransactionLogById(record);
                                    if (jsonTransactionLog.IsNullOrEmpty() || jsonTransactionLog == "[]")
                                    {
                                        statusCode = HttpStatusCode.NoContent;
                                        responseMessage = JsonConvert.SerializeObject(new { StatusCode = (int)statusCode });
                                    }
                                    else
                                    {
                                        statusCode = HttpStatusCode.OK;
                                        responseMessage = $"{{\"StatusCode\": {(int)statusCode}, \"TransactionLog\": {jsonTransactionLog}}}";
                                    }

                                    break;
                                }
                                default:
                                {
                                    statusCode = HttpStatusCode.OK;
                                    responseMessage = JsonConvert.SerializeObject(new { StatusCode = (int)statusCode, Message = $"{functionName} METHOD {req.Method} version {version}" });

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
                        statusCode = HttpStatusCode.OK;
                        responseMessage = JsonConvert.SerializeObject(new { StatusCode = (int)statusCode, Message = $"{functionName} version {version}" });
                    }
                    else
                    {
                        statusCode = HttpStatusCode.Unauthorized;
                        responseMessage = JsonConvert.SerializeObject(new { StatusCode = (int)statusCode, Message = $"{functionName} version {version} METHOD {req.Method} INVALID KEY" });
                    }
                }
            }
            catch (Exception exception)
            {
                statusCode = HttpStatusCode.InternalServerError;
                responseMessage = JsonConvert.SerializeObject(new
                {
                    StatusCode = (int)statusCode,
                    Message = $"{functionName} version {version} METHOD {req.Method} ERROR: {exception.Message}"
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
