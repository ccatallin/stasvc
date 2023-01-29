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
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post",  Route = "finance/v1/ticker/{symbol:alpha?}/{market:alpha?}")] HttpRequest req,
            ILogger log,
            string symbol,
            string market)
        {
            ObjectResult result = null;

            try 
            {
                if (string.IsNullOrEmpty(symbol))
                {
                    result = new BadRequestObjectResult($"This HTTP {req.Method} triggered. Function executed successfully and said no symbol was provided.");
                }
                else
                {
                    string bodyResponse = string.Empty;
                    StringBuilder responseBuilder = new StringBuilder();
                    var upperSymbol = symbol.ToUpperInvariant();

                    if (string.IsNullOrEmpty(market))
                    {

                        bodyResponse = await yahooFinanceHttpQuery.GetStringAsync($"{upperSymbol}");

                        var startIndex = bodyResponse.IndexOf($"data-symbol=\"{upperSymbol}\"");
                        var endIndex = bodyResponse.IndexOf("</fin-streamer>", startIndex);
                        startIndex = bodyResponse.IndexOf(">", startIndex) + 1;

                        responseBuilder.Append("{\"").Append(upperSymbol).Append("\":")
                            .Append(bodyResponse.Substring(startIndex, endIndex - startIndex)).Append("}");
                    }
                    else
                    {
                        bodyResponse = await googleFinanceHttpQuery.GetStringAsync($"{upperSymbol}:{market.ToUpperInvariant()}");
                    }

                    // string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
                    // dynamic data = JsonConvert.DeserializeObject(requestBody);
                    // name = name ?? data?.name;

                    result = new OkObjectResult(responseBuilder.ToString());
                }
            }
            catch (Exception exception) 
            {
                log.LogError(exception: exception, exception.Message);
                result = new ObjectResult(500);
            }

            return  result;
        }
    }
}
