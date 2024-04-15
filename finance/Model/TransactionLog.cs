using System;
using System.Runtime.Serialization;

namespace FalxGroup.Finance.Model
{

[DataContract]
public class TransactionLog
{

    public TransactionLog()
    {

    }

    public TransactionLog(int transactionType, int productType, string productName, int noContracts, 
        double contractPrice, DateTime transactionDate, long createdById, long clientId, 
        long userAccountId /* not used yet */, string notes /* not used yet */)
    {
        this.TransactionType = transactionType;
        this.ProductType = productType;
        this.ProductName = productName;
        this.NoContracts = noContracts;
        this.ContractPrice = contractPrice;
        this.TransactionDate = transactionDate;
        this.CreatedById = createdById;
        this.ClientId = clientId;
        this.UserAccountId = userAccountId;
        this.Notes = notes;
    }

    [DataMember(Name = "transaction_type")]
    public int TransactionType { get; set; }

    [DataMember(Name = "product_type")]
    public int ProductType { get; set; }
    
    [DataMember(Name = "product_name")]
    public string ProductName { get; set; }

    [DataMember(Name = "product_quantity")]
    public int NoContracts { get; set; }

    [DataMember(Name = "product_price")]
    public double ContractPrice { get; set; }

    [DataMember(Name = "transaction_date")]
    public DateTime TransactionDate { get; set; }

    [DataMember(Name = "user_id")]
    public long CreatedById { get; set; }

    [DataMember(Name = "client_id")]
    public long ClientId { get; set; }

    [DataMember(Name = "user_account_id")]
    public long UserAccountId { get; set; }

    [DataMember(Name = "notes")]
    public string Notes { get; set; }

    public bool IsEmpty => (((0 == this.ProductName.Trim().Length) || (0 == this.NoContracts) || (0 == this.ContractPrice)) ? true : false);

} /* end class TransactionLog */

} /* end FalxGroup.Finance.Model namespace */