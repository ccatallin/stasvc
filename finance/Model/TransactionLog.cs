using System;

namespace FalxGroup.Finance.Model
{

public class TransactionLog
{

    public TransactionLog()
    {

    }

    public TransactionLog(int transactionType, int productType, string productName, int quantity, 
        double price, DateTime transactionDate, string notes)
    {
        this.TransactionType = transactionType;
        this.ProductType = productType;
        this.ProductName = productName;
        this.Quantity = quantity;
        this.Price = price;
        this.TransactionDate = transactionDate;
        this.Notes = notes;
    }

    public int TransactionType { get; set; }
    public int ProductType { get; set; }
    public string ProductName { get; set; }
    public int Quantity { get; set; }
    public double Price { get; set; }
    public DateTime TransactionDate { get; set; }
    public string Notes { get; set; } 

} /* end class TransactionLog */

} /* end FalxGroup.Finance.Model namespace */