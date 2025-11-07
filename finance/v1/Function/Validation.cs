using System;
using System.Threading.Tasks;
using System.Net;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Logging;
using FalxGroup.Finance.Service;
using System.Text;
using Newtonsoft.Json.Linq;

#if DEBUG
namespace FalxGroup.Finance.Function
{
    public class Validation
    {
        private readonly TransactionLoggerService _processor;
        private readonly ILogger _logger;

        public Validation(TransactionLoggerService processor, ILoggerFactory loggerFactory)
        {
            _processor = processor;
            _logger = loggerFactory.CreateLogger<Validation>();
        }

        [Function("ValidateSnapshots")]
        public async Task<HttpResponseData> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "finance/v1/validate_snapshots")] HttpRequestData req)
        {
            _logger.LogInformation("C# HTTP trigger function 'ValidateSnapshots' processed a request.");

            if (!long.TryParse(req.Query["userId"], out long userId) ||
                !long.TryParse(req.Query["clientId"], out long clientId) ||
                string.IsNullOrEmpty(req.Query["productSymbol"])) // This will be null if not present
            {   
                var badReqResponse = req.CreateResponse(HttpStatusCode.BadRequest);
                await badReqResponse.WriteStringAsync("Please provide 'userId' (long), 'clientId' (long), and 'productSymbol' (string) in the query string.");
                return badReqResponse;
            }

            string productSymbol = req.Query["productSymbol"]!;

            try
            {
                var (expectedJson, actualJson) = await _processor.ValidateSnapshotCalculationAsync(userId, clientId, productSymbol);

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

                var response = req.CreateResponse(HttpStatusCode.OK);
                response.Headers.Add("Content-Type", "text/html; charset=utf-8");
                await response.WriteStringAsync(htmlBuilder.ToString());
                return response;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error during snapshot validation.");
                var errorResponse = req.CreateResponse(HttpStatusCode.InternalServerError);
                await errorResponse.WriteStringAsync($"An error occurred: {ex.Message}");
                return errorResponse;
            }
        }
    }
}
#endif
