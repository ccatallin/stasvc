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

    public TransactionLog(string id, DateTime date, int typeId, int productCategoryId, int productTypeId, string productName,
        int quantity, decimal price, decimal fees, string notes, long userId, long clientId, long userAccountId, string applicationKey,
        int getProcessType = 2)
    {
        this.Id = id;
        
        this.Date = date;
        this.TypeId = typeId;
        this.ProductCategoryId = productCategoryId;
        this.ProductTypeId = productTypeId;
        this.ProductName = productName;
        
        this.Quantity = quantity;
        this.Price = price;
        this.Fees = fees;

        this.Notes = notes;
            
        this.UserId = userId;
        this.ClientId = clientId;
        this.UserAccountId = userAccountId;

        this.ApplicationKey = applicationKey;
        this.GetProcessTypeId = getProcessType;
    }

   [DataMember(Name = "id")]
    public string Id { get; set; }
    
    [DataMember(Name = "product_category_id")]
    public int ProductCategoryId { get; set; }

    [DataMember(Name = "type_id")] /* BUY/SELL */
    public int TypeId { get; set; }

    [DataMember(Name = "product_type_id")]
    public int ProductTypeId { get; set; }

    [DataMember(Name = "product_name")]
    public string ProductName { get; set; }

    [DataMember(Name = "quantity")]
    public int Quantity { get; set; }

    [DataMember(Name = "price")]
    public decimal Price { get; set; }

    [DataMember(Name = "fees")]
    public decimal Fees { get; set; }

    [DataMember(Name = "date")]
    public DateTime Date { get; set; }

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

    [DataMember(Name = "get_process_type_id")]
    public int GetProcessTypeId { get; set; }

    [DataMember(Name = "start_date")]
    public DateTime? StartDate { get; set; }
    
    [DataMember(Name = "end_date")]
    public DateTime? EndDate { get; set; }

    public bool IsEmpty => string.IsNullOrWhiteSpace(this.ProductName) || (0 == this.Quantity);
} /* end class TransactionLog */

} /* end FalxGroup.Finance.Model namespace */