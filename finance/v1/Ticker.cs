using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace FalxGroup.Finance.v1
{
    public static class Ticker
    {
        [FunctionName("Ticker")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post",  Route = "finance/v1/ticker/{symbol:alpha}")] HttpRequest req,
            ILogger log,
            String symbol)
        {
            log.LogInformation("C# HTTP trigger function processed a request.");

            string name = req.Query["name"];

            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            dynamic data = JsonConvert.DeserializeObject(requestBody);
            name = name ?? data?.name;

            string responseMessage = string.IsNullOrEmpty(name)
                ? $"This HTTP {req.Method} triggered. Function for {symbol.ToUpperInvariant()} executed successfully. Pass a name in the query string or in the request body for a personalized response."
                : $"Hello, {name}. This HTTP {req.Method} triggered. Function for {symbol.ToUpperInvariant()} executed successfully.";

            return new OkObjectResult(responseMessage);
        }
    }
}
