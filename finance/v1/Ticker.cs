using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System.Text;

namespace FalxGroup.Finance.v1
{
    public static class Ticker
    {
        private static cc.net.HttpQuery googleFinanceHttpQuery = new cc.net.HttpQuery("https://www.google.com/finance/quote/");
        private static cc.net.HttpQuery yahooFinanceHttpQuery = new cc.net.HttpQuery("https://finance.yahoo.com/quote/");

        [FunctionName("Ticker")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", /* "post", */ Route = "finance/v1/ticker/{symbol:alpha?}/{market:alpha?}")] HttpRequest req,
            ExecutionContext executionContext,
            ILogger log,
            string symbol,
            string market)
        {

            StringBuilder responseBuilder = new StringBuilder();

            var upperSymbol = string.Empty;
            string symbolPrice = string.Empty;
            
            var startIndex = -1;
            var endIndex  = -1;

            int statusCode = 200;
            string statusMessage = string.Empty;
            var result = new OkObjectResult(string.Empty);

            try 
            {
                if (string.IsNullOrEmpty(symbol))
                {
                    statusCode = 204;
                    statusMessage = $"HTTP {req.Method} {executionContext.FunctionName} version 1.0.1";
                }
                else
                {
                    string bodyResponse = string.Empty;
                    upperSymbol = symbol.ToUpperInvariant();

                    if (string.IsNullOrEmpty(market))
                    {
                        bodyResponse = await yahooFinanceHttpQuery.GetStringAsync($"{upperSymbol}");

                        startIndex = bodyResponse.IndexOf($"data-symbol=\"{upperSymbol}\"");

                        if (-1 == startIndex)
                        {
                            statusCode = 404;
                            statusMessage = $"{upperSymbol} informations not found";
                        }
                        else
                        {
                            endIndex = bodyResponse.IndexOf("</fin-streamer>", startIndex);
                            startIndex = bodyResponse.IndexOf(">", startIndex) + 1;

                            symbolPrice = bodyResponse.Substring(startIndex, endIndex - startIndex);

                            if ((string.IsNullOrEmpty(symbolPrice)) || (0 == symbolPrice.Length))
                            {
                                statusCode = 406;
                                statusMessage = $"{upperSymbol} informations not valid";
                            }
                        }
                    }
                    else
                    {
                        var upperMarket = market.ToUpperInvariant();

                        bodyResponse = await googleFinanceHttpQuery.GetStringAsync($"{upperSymbol}:{upperMarket}");
                    }                    
                }
            }
            catch (Exception exception) 
            {
                log.LogError(exception: exception, exception.Message);

                statusCode = 500;
                statusMessage = exception.Message;
            }
            finally
            {
                if (200 == statusCode)
                {
                    responseBuilder.Append("{\"StatusCode\":").Append(statusCode.ToString())
                        .Append(", \"Message\": \"\"")
                        .Append(", \"").Append(upperSymbol).Append("\":").Append(symbolPrice)
                        .Append("}");
                }
                else
                {
                    responseBuilder.Append("{\"StatusCode\":").Append(statusCode.ToString())
                        .Append(", \"Message\": \"")
                        .Append(statusMessage)
                        .Append("\"}");
                }
                
                result.Value = responseBuilder.ToString();
            }

            return result; // always OK status and inside we pass the correct application error code
        }
    }
}
