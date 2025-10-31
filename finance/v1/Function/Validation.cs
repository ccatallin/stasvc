using System;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Logging;
using FalxGroup.Finance.Service;
using System.Text;
using Newtonsoft.Json.Linq;

#if DEBUG
namespace FalxGroup.Finance.Function
{
    public static class Validation
    {
        private static readonly TransactionLoggerService processor = new TransactionLoggerService(Environment.GetEnvironmentVariable("SqlConnectionString"));

        [FunctionName("ValidateSnapshots")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "finance/v1/validate_snapshots")] HttpRequest req,
            ILogger log)
        {
            log.LogInformation("C# HTTP trigger function 'ValidateSnapshots' processed a request.");

            if (!long.TryParse(req.Query["userId"], out long userId) ||
                !long.TryParse(req.Query["clientId"], out long clientId) ||
                string.IsNullOrEmpty(req.Query["productSymbol"]))
            {
                return new BadRequestObjectResult("Please provide 'userId' (long), 'clientId' (long), and 'productSymbol' (string) in the query string.");
            }

            string productSymbol = req.Query["productSymbol"];

            try
            {
                var (expectedJson, actualJson) = await processor.ValidateSnapshotCalculationAsync(userId, clientId, productSymbol);

                bool areEqual = JToken.DeepEquals(JToken.Parse(expectedJson), JToken.Parse(actualJson));

                var htmlBuilder = new StringBuilder();
                htmlBuilder.Append("<html><head><title>Snapshot Validation Report</title>");
                htmlBuilder.Append("<style>");
                htmlBuilder.Append("body { font-family: sans-serif; }");
                htmlBuilder.Append(".container { display: flex; }");
                htmlBuilder.Append(".column { flex: 50%; padding: 10px; }");
                htmlBuilder.Append("h1.match { color: green; } h1.mismatch { color: red; }");
                htmlBuilder.Append("pre { background-color: #f4f4f4; padding: 10px; border: 1px solid #ddd; border-radius: 5px; white-space: pre-wrap; }");
                htmlBuilder.Append("</style></head><body>");

                if (areEqual)
                {
                    htmlBuilder.Append($"<h1 class='match'>MATCH</h1>");
                }
                else
                {
                    htmlBuilder.Append($"<h1 class='mismatch'>MISMATCH</h1>");
                }
                htmlBuilder.Append($"<p>Validation for User: {userId}, Client: {clientId}, Symbol: {productSymbol}</p>");

                htmlBuilder.Append("<div class='container'>");
                htmlBuilder.Append("<div class='column'><h2>Expected (from T-SQL)</h2><pre>");
                htmlBuilder.Append(JToken.Parse(expectedJson).ToString(Newtonsoft.Json.Formatting.Indented));
                htmlBuilder.Append("</pre></div>");

                htmlBuilder.Append("<div class='column'><h2>Actual (from C#)</h2><pre>");
                htmlBuilder.Append(JToken.Parse(actualJson).ToString(Newtonsoft.Json.Formatting.Indented));
                htmlBuilder.Append("</pre></div>");
                htmlBuilder.Append("</div>");

                htmlBuilder.Append("</body></html>");

                return new ContentResult { Content = htmlBuilder.ToString(), ContentType = "text/html" };
            }
            catch (Exception ex)
            {
                log.LogError(ex, "Error during snapshot validation.");
                return new ObjectResult($"An error occurred: {ex.Message}") { StatusCode = 500 };
            }
        }
    }
}
#endif
