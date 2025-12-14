
using FalxGroup.Finance.Model;
using FalxGroup.Finance.Util;
using cc.net;

namespace FalxGroup.Finance.Service
{
    public class TickerService
    {
        private TickerCache symbolsCache;        
        private HttpQuery yahooFinanceHttpQuery;
        private HttpQuery googleFinanceHttpQuery;

        public TickerService(int cacheTimeout = 15)
        {
            symbolsCache = new TickerCache(cacheTimeout);
            // yahooFinanceHttpQuery = new HttpQuery("https://finance.yahoo.com/quote/");
            // Yahoo's internal JSON API endpoint for chart data
            // https://query1.finance.yahoo.com/v8/finance/chart/BG?interval=1d&range=1d
            yahooFinanceHttpQuery = new HttpQuery("https://query1.finance.yahoo.com/v8/finance/chart/");
            googleFinanceHttpQuery = new HttpQuery("https://www.google.com/finance/quote/");
        }

        public async Task<QueryResponse> Run(string functionName, string version, string? symbol, string? market = null)
        {
            QueryResponse? queryResponse = null;

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
                        var cacheValue = this.symbolsCache.GetValueOrDefault(upperSymbol);

                        if (null != cacheValue)
                        {
                            queryResponse = new QueryResponse(201, message: string.Empty, symbol: upperSymbol, value: cacheValue.Value.Item2);                            
                        }
                    }

                    if ((null == queryResponse) || (string.IsNullOrEmpty(queryResponse.Value)))
                    {
                        // data not in cache or data too old
                        if (string.IsNullOrEmpty(market))
                        {
                            queryResponse = await this.QueryYahooFinance(upperSymbol);
                        }
                        else
                        {
                            var upperMarket = market.ToUpperInvariant();
                            queryResponse = await this.QueryGoogleFinance(upperSymbol, upperMarket);
                        }  
                    }                  
                }
            }
            catch (Exception exception) 
            {
                queryResponse = new QueryResponse(500, exception.Message, string.Empty);
            }

            return queryResponse;
        }
        
        private async Task<QueryResponse> QueryGoogleFinance(string symbol, string market)
        {
            string value = await googleFinanceHttpQuery.GetValueAsync($"{symbol}%3A{market}", "data-last-price=\"", "\" data-last-normal-market-timestamp=");
            return CreateQueryResponse(symbol, market, value);
        }

        private async Task<QueryResponse> QueryYahooFinance(string symbol)
        {
            // string value = await yahooFinanceHttpQuery.GetValueAsync($"{symbol}/", "data-testid=\"qsp-price\">", "</");
            string value = await yahooFinanceHttpQuery.GetValueAsync($"{symbol}?interval=1d&range=1d", "regularMarketPrice");
            return CreateQueryResponse(symbol, null, value);
        }

        private QueryResponse CreateQueryResponse(string symbol, string? market, string value)
        {
            if (string.IsNullOrEmpty(value))
            {
                return new QueryResponse(404, $"{symbol} informations not found", symbol, market);
            }

            // symbol info exists but it's expired or it's first time
            // Add and Update are identical in TickerCache
            this.symbolsCache.Update(symbol, value);

            return new QueryResponse(200, message: string.Empty, symbol: symbol, market: market, value: value);
        }
    } /* end class TickerService */
} /* end FalxGroup.Finance.Service namespace */