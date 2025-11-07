using System;
using System.Text;
using System.Threading.Tasks;
using System.Net;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Logging;
// --
using FalxGroup.Finance.Service;
using System.Linq;

namespace FalxGroup.Finance.Function
{
    public class CboeVolatilityR16
    {
        private const string version = "2.0.0-isolated";
        private readonly TickerService _processor;
        private readonly ILogger _logger;

        public CboeVolatilityR16(ILoggerFactory loggerFactory, TickerService processor)
        {
            _logger = loggerFactory.CreateLogger<CboeVolatilityR16>();
            _processor = processor;
        }
        
        private const string cboeIndexesMarketTicker = "INDEXCBOE";
        /*
            This function is used to apply the rule of 16 for the following CBOE indexes VIX, VVIX, VXN, VXD, RVX, MOVE, GVZ and OVX
         */
        private static readonly string[] cboeIndexes = { "VIX", "VVIX", "VXN", "VXD", "RVX", "GVZ", "OVX" };

        [Function("CboeVolatilityR16")]
        public async Task<HttpResponseData> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", /* "post", */ Route = "finance/v1/cboe_volatility_r16/{symbol:alpha?}")] HttpRequestData req,
            string? symbol)
        {
            StringBuilder responseBuilder = new StringBuilder("");
            var functionName = nameof(CboeVolatilityR16);

            try
            {
                var indexSymbol = (string.IsNullOrEmpty(symbol) ? "VIX" : symbol.ToUpper());

                var response = await _processor.Run(_logger, 
                    functionName, version, 
                    (cboeIndexes.Any(validSymbol => validSymbol == indexSymbol) ? indexSymbol : "VIX"), 
                    cboeIndexesMarketTicker);

                responseBuilder.Append("{")
                    .Append("\"StatusCode\":").Append($"{response.StatusCode}")
                    .Append(", \"Message\": \"").Append(response.Message).Append("\"");

                if ((200 == response.StatusCode) || (201 == response.StatusCode))
                {
                    responseBuilder.Append(", \"").Append(response.Symbol).Append("\": ").Append(response.Value ?? "0");

                    responseBuilder.Append(", \"daily expected volatility +/- (%)\": ")
                        .Append((Double.Parse(response.Value ?? "0") / Math.Sqrt(252)).ToString("0.00"));
                    responseBuilder.Append(", \"weekly expected volatility +/- (%)\": ")
                        .Append((Double.Parse(response.Value ?? "0") / Math.Sqrt(52)).ToString("0.00"));
                    responseBuilder.Append(", \"monthly expected volatility +/- (%)\": ")
                        .Append((Double.Parse(response.Value ?? "0") / Math.Sqrt(12)).ToString("0.00"));
                    responseBuilder.Append(", \"yearly expected volatility +/- (%)\": ")
                        .Append(response.Value);
                }

                responseBuilder.Append("}");
            }
            catch (Exception exception)
            {
                _logger.LogError(exception: exception, exception.Message);
            }

            var httpResponse = req.CreateResponse(HttpStatusCode.OK);
            httpResponse.Headers.Add("Content-Type", "application/json; charset=utf-8");
            await httpResponse.WriteStringAsync(responseBuilder.ToString());
            return httpResponse;
        }
    }
}
