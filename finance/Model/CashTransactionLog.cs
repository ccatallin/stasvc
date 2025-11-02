using System;
using System.Runtime.Serialization;

namespace FalxGroup.Finance.Model
{
    [DataContract]
    public class CashTransactionLog
    {
        [DataMember(Name = "id")]
        public string Id { get; set; }
        [DataMember(Name = "date")]
        public DateTime Date { get; set; }
        [DataMember(Name = "operation_id")]
        public int OperationId { get; set; } // e.g., Add, Remove
        [DataMember(Name = "cash_category_id")]
        public int CashCategoryId { get; set; } // e.g., Deposit, Withdrawal, Dividend
        [DataMember(Name = "amount")]
        public decimal Amount { get; set; }
        [DataMember(Name = "notes")]
        public string Notes { get; set; }
        [DataMember(Name = "user_id")]
        public long UserId { get; set; } // Maps to CreatedById or ModifiedById
        [DataMember(Name = "client_id")]
        public long ClientId { get; set; }

        // Optional properties for filtering or other operations
        [DataMember(Name = "start_date")]
        public DateTime? StartDate { get; set; }
        [DataMember(Name = "end_date")]
        public DateTime? EndDate { get; set; }
    }
}