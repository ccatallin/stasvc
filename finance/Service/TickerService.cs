
using System;
using System.Text;
using System.Threading.Tasks;
// --
using Microsoft.Extensions.Logging;
// --
using FalxGroup.Finance.Model;
using FalxGroup.Finance.Util;

namespace FalxGroup.Finance.Service
{

public class TickerService
{
    private TickerCache symbolsCache;        
    private cc.net.HttpQuery yahooFinanceHttpQuery;
    private cc.net.HttpQuery googleFinanceHttpQuery;

    public TickerService(int cacheTimeout = 15)
    {
        symbolsCache = new TickerCache(cacheTimeout);
        yahooFinanceHttpQuery = new cc.net.HttpQuery("https://finance.yahoo.com/quote/");
        googleFinanceHttpQuery = new cc.net.HttpQuery("https://www.google.com/finance/quote/");
    }

    public async Task<QueryResponse> Run(ILogger log, string functionName, string version, string symbol, string market = null)
    {
        bool symbolInCache = false;

        QueryResponse queryResponse = null;

        try 
        {
            if (string.IsNullOrEmpty(symbol))
            {
                queryResponse = new QueryResponse(204, 
                    $"{functionName} version {version}", 
                    string.Empty);
            }
            else
            {
                var upperSymbol = symbol.ToUpperInvariant();

                if (symbolsCache.ContainsKey(upperSymbol))
                {
                    symbolInCache = true;
                    
                    var cacheValue = this.symbolsCache.GetValueOrDefault(upperSymbol);

                    if (null != cacheValue)
                    {
                        queryResponse = new QueryResponse(201, message: string.Empty, symbol: upperSymbol, value: cacheValue.Item2);                            
                    }
                }

                if ((null == queryResponse) || (string.IsNullOrEmpty(queryResponse.Value)))
                {
                    // data not in cache or data to old
                    if (string.IsNullOrEmpty(market))
                    {
                        queryResponse = await this.QueryYahooFinance(upperSymbol, symbolInCache);
                    }
                    else
                    {
                        var upperMarket = market.ToUpperInvariant();
                        queryResponse = await this.QueryGoogleFinance(upperSymbol, upperMarket, symbolInCache);
                    }  
                }                  
            }
        }
        catch (Exception exception) 
        {
            log.LogError(exception: exception, exception.Message);
            queryResponse = new QueryResponse(500, exception.Message, string.Empty);
        }

        return queryResponse;
    }

        private async Task<QueryResponse> QueryGoogleFinance(string symbol, string market, bool symbolInCache)
        {
            QueryResponse response = new QueryResponse(200, message: string.Empty, symbol: symbol, market: market);

            string bodyResponse = await googleFinanceHttpQuery.GetStringAsync($"{symbol}%3A{market}");

            int startIndex = bodyResponse.IndexOf($"data-last-price=");

            if (-1 == startIndex)
            {
                response.StatusCode = 404;
                response.Message = $"{symbol}:{market} informations not found";
            }
            else
            {
                int endIndex = bodyResponse.IndexOf(" data-last-normal-market-timestamp=", startIndex) - 1;
                response.Value = bodyResponse.Substring(startIndex + 17, endIndex - (startIndex + 17));

                if ((string.IsNullOrEmpty(symbol)) || (0 == symbol.Length))
                {
                    response.StatusCode = 406;
                    response.Message = $"{symbol}:{market} informations not valid";
                }
                else
                {
                    if (!symbolInCache)
                    {
                        // symbol info does not exists, it's  first
                        this.symbolsCache.Add(symbol, response.Value);
                    }
                    else
                    {   // symbol info exists but it's expired
                        this.symbolsCache.Update(symbol, response.Value);
                    }
                }
            }

            return response;
        }

    private async Task<QueryResponse> QueryYahooFinance(string symbol, bool symbolInCache)
    {
        QueryResponse response = new QueryResponse(200, message: string.Empty, symbol: symbol);

        string bodyResponse = await yahooFinanceHttpQuery.GetStringAsync($"{symbol}");

        int startIndex = bodyResponse.IndexOf($"data-symbol=\"{symbol}\"");

        if (-1 == startIndex)
        {
            response.StatusCode = 404;
            response.Message = $"{symbol} informations not found";
        }
        else
        {
            int endIndex = bodyResponse.IndexOf("</fin-streamer>", startIndex);
            startIndex = bodyResponse.IndexOf(">", startIndex) + 1;

            response.Value = bodyResponse.Substring(startIndex, endIndex - startIndex);

            if ((string.IsNullOrEmpty(response.Value)) || (0 == response.Value.Length))
            {
                response.StatusCode = 406;
                response.Message = $"{symbol} informations not valid";
            }
            else
            {
                if (!symbolInCache)
                {
                    // symbol info does not exists, it's  first
                    this.symbolsCache.Add(symbol, response.Value);
                }
                else
                {   // symbol info exists but it's expired
                    this.symbolsCache.Update(symbol, response.Value);
                }
            }
        }

        return response;
    }

} /* end class TickerService */

} /* end FalxGroup.Finance.Service namespace */