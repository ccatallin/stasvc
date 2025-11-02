using System;
using System.Runtime.Serialization;

namespace FalxGroup.Finance.Model
{
    [DataContract]
    public abstract class BaseTransactionLog
    {
        [DataMember(Name = "id")]
        public string Id { get; set; }

        [DataMember(Name = "date")]
        public DateTime Date { get; set; }

        [DataMember(Name = "operation_id")]
        public int OperationId { get; set; }

        [DataMember(Name = "notes")]
        public string Notes { get; set; }

        [DataMember(Name = "user_id")]
        public long UserId { get; set; }

        [DataMember(Name = "client_id")]
        public long ClientId { get; set; }

        [DataMember(Name = "user_account_id")]
        public long UserAccountId { get; set; }

        [DataMember(Name = "application_key")]
        public string ApplicationKey { get; set; }

        [DataMember(Name = "get_request_id")]
        public int GetRequestId { get; set; }

        [DataMember(Name = "start_date")]
        public DateTime? StartDate { get; set; }

        [DataMember(Name = "end_date")]
        public DateTime? EndDate { get; set; }
    }
}