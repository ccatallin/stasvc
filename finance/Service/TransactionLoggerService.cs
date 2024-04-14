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

public class TransactionLoggerService
{
    public TransactionLoggerService()
    {

    }

    public static async Task<TransactionLogResponse> Run(ILogger log, string functionName, string version, TransactionLog transactionRecord)
    {
        TransactionLogResponse reponse = null;
        return reponse;
    }

} /* end class TransactionLoggerService */

} /* end FalxGroup.Finance.Service namespace */