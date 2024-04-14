namespace FalxGroup.Finance.Model
{

public class TransactionLogResponse
{

    public TransactionLogResponse()
    {

    }

    public TransactionLogResponse(int statusCode, string message)
    {
        this.StatusCode = statusCode;
        this.Message = message;
    }

    public int StatusCode { get; set; }
    public string Message { get; set; }

} /* end class TransactionLogResponse */

} /* end FalxGroup.Finance.Model namespace */