using System;
using System.Runtime.Serialization;

namespace FalxGroup.Finance.Model
{

    [DataContract]
    public class SecurityTransactionLog : BaseTransactionLog
    {

        public SecurityTransactionLog()
        {

        }

        public SecurityTransactionLog(string id, DateTime date, int operationId, int productCategoryId, int productId, string productName,
            int quantity, decimal price, decimal fees, string notes, long userId, long clientId, long userAccountId, string applicationKey,
            int getProcessTypeId = 2)
        {
            this.Id = id;

            this.Date = date;
            this.OperationId = operationId;
            this.ProductCategoryId = productCategoryId;
            this.ProductId = productId;
            this.ProductSymbol = productName;

            this.Quantity = quantity;
            this.Price = price;
            this.Fees = fees;

            this.Notes = notes;

            this.UserId = userId;
            this.ClientId = clientId;
            this.UserAccountId = userAccountId;

            this.ApplicationKey = applicationKey;
            this.GetRequestId = getProcessTypeId;
        }

        [DataMember(Name = "product_category_id")]
        public int ProductCategoryId { get; set; }

        [DataMember(Name = "product_id")]
        public int ProductId { get; set; }

        [DataMember(Name = "product_symbol")]
        public string? ProductSymbol { get; set; }

        [DataMember(Name = "quantity")]
        public int Quantity { get; set; }

        [DataMember(Name = "price")]
        public decimal Price { get; set; }

        [DataMember(Name = "fees")]
        public decimal Fees { get; set; }

        // These properties are not mapped to the DB table directly.
        // They should be populated by the data access layer by joining with
        // the Products and ProductCategories tables before being passed to the calculator.
        public decimal ContractMultiplier { get; set; } = 1;
        public decimal CategoryMultiplier { get; set; } = 1;

        // Safe accessors: treat 0 as "no multiplier" (same as 1) to guard against bad DB data.
        public decimal EffectiveContractMultiplier => ContractMultiplier <= 0 ? 1m : ContractMultiplier;
        public decimal EffectiveCategoryMultiplier => CategoryMultiplier <= 0 ? 1m : CategoryMultiplier;

        public bool IsEmpty => string.IsNullOrWhiteSpace(this.ProductSymbol) || (0 == this.Quantity);
    }
} /* end FalxGroup.Finance.Model namespace */