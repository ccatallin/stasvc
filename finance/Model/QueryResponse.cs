namespace FalxGroup.Finance.Model
{

public class QueryResponse
{
    public QueryResponse()
    {

    }

    public QueryResponse(int statusCode, string message, string? symbol = null, string? market = null, string? value = null)
    {
        this.StatusCode = statusCode;
        this.Message = message;

        this.Symbol = symbol;
        this.MarketSymbol = market ?? string.Empty;

        this.Value = value ?? string.Empty;
    }

    public int StatusCode { get; set; }
    public string? Message { get; set; }
    
    public string? Symbol { get; set; }
    public string? MarketSymbol { get; set; }

    public string? Value { get; set; }

} /* end class QueryResponse */

} /* end FalxGroup.Finance.Model namespace */