using System;
using System.Runtime.Serialization;

namespace FalxGroup.Finance.Model
{

    [DataContract]
    public class CashTransactionLog : BaseTransactionLog
    {
        [DataMember(Name = "cash_category_id")]
        public int CashCategoryId { get; set; } // e.g., Deposit, Withdrawal, Dividend
        [DataMember(Name = "amount")]
        public decimal Amount { get; set; }
    }
}