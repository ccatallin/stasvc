using System;

namespace FalxGroup.Finance.Model
{
    public class PositionSnapshot
    {
        public long UserId { get; set; }
        public long ClientId { get; set; }
        public int ProductCategoryId { get; set; }
        public int ProductId { get; set; }
        public string ProductSymbol { get; set; } = string.Empty;
        public DateTime SnapshotDate { get; set; }
        public int Quantity { get; set; }
        public decimal Cost { get; set; }
        public decimal Commission { get; set; }
        public decimal AveragePrice { get; set; }
    }
}