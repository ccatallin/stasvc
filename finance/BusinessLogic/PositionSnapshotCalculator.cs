using System;
using System.Collections.Generic;
using System.Linq;
using FalxGroup.Finance.Model;

namespace FalxGroup.Finance.BusinessLogic
{
    public class PositionSnapshotCalculator
    {
        public List<PositionSnapshot> Calculate(long userId, long clientId, string productSymbol, IEnumerable<SecurityTransactionLog> transactions)
        {
            if (transactions == null || !transactions.Any())
            {
                return new List<PositionSnapshot>();
            }

            // Step 1 & 2: Order transactions, calculate intermediate values, and assign LotGroupIds
            var tradesWithLots = AssignLotGroups(transactions);

            // Step 3: Calculate per-transaction running state
            var perTransactionState = CalculatePerTransactionState(tradesWithLots);

            // Step 4: Select the last state for each day and calculate final snapshot values
            var dailySnapshots = perTransactionState
                .GroupBy(s => s.Tx.Date.Date)
                .Select(dayGroup =>
                {
                    // Find the last transaction of the day
                    var lastStateOfDay = dayGroup.OrderByDescending(s => s.Tx.Date).ThenByDescending(s => s.Tx.Id).First();

                    decimal averagePrice = 0;
                    if (lastStateOfDay.OpenQuantity > 0)
                    {
                        averagePrice = lastStateOfDay.TotalBuyQuantity > 0 ? lastStateOfDay.TotalBuyValue / lastStateOfDay.TotalBuyQuantity : 0;
                    }
                    else if (lastStateOfDay.OpenQuantity < 0)
                    {
                        // For shorts, use the average price from the start of the lot.
                        // We find the first transaction that initiated the short position within the lot.
                        var lotTransactions = perTransactionState.Where(t => t.LotGroupId == lastStateOfDay.LotGroupId);
                        var firstSellState = lotTransactions.OrderBy(t => t.Tx.Date).ThenBy(t => t.Tx.Id).First();

                        if (firstSellState.TotalSellQuantity > 0)
                        {
                            averagePrice = firstSellState.TotalSellValue / firstSellState.TotalSellQuantity;
                        }
                    }

                    // For ZB futures, the multiplier is effectively 1 because the UnitValue already accounts for it.
                    decimal multiplier = (lastStateOfDay.Tx.ProductCategoryId == 3 && lastStateOfDay.Tx.ProductId == 5) ? 1 : lastStateOfDay.Tx.CategoryMultiplier;
                    decimal cost = (decimal)lastStateOfDay.OpenQuantity * averagePrice * multiplier;

                    return new PositionSnapshot
                    {
                        UserId = userId,
                        ClientId = clientId,
                        ProductCategoryId = lastStateOfDay.Tx.ProductCategoryId,
                        ProductId = lastStateOfDay.Tx.ProductId,
                        ProductSymbol = productSymbol,
                        SnapshotDate = lastStateOfDay.Tx.Date.Date,
                        Quantity = (int)lastStateOfDay.OpenQuantity,
                        AveragePrice = averagePrice,
                        Commission = lastStateOfDay.TotalCommission,
                        Cost = cost
                    };
                })
                .ToList();

            return dailySnapshots;
        }

        private static List<TradeWithLot> AssignLotGroups(IEnumerable<SecurityTransactionLog> transactions)
        {
            var orderedTrades = transactions
                .OrderBy(t => t.Date)
                .ThenBy(t => t.Id)
                .Select(t =>
                {
                    // BUY is -1 in the old SP, but let's assume a more standard BUY = 1, SELL = -1.
                    // The SP has OperationId = -1 for BUY. Let's stick to that for consistency.
                    var signedQuantity = t.OperationId == -1 ? t.Quantity : -t.Quantity;

                    decimal unitValue = (t.ProductCategoryId == 3 && t.ProductId == 5)
                        // For bond futures (like ZB), price is in points and 32nds (e.g., 117.18 is 117 and 18/32).
                        // We need to extract the integer part of the 32nds.
                        // Example: 117.18 -> floor = 117. frac = 0.18. 32nds = (0.18 * 100) = 18.
                        ? (Math.Floor(t.Price) + ((t.Price - Math.Floor(t.Price)) * 100m) / 32m) * 1000m
                        : t.Price;

                    return new
                    {
                        Tx = t,
                        SignedQuantity = signedQuantity,
                        BuyValue = t.OperationId == -1 ? t.Quantity * unitValue : 0,
                        BuyQuantity = t.OperationId == -1 ? t.Quantity : 0,
                        SellValue = t.OperationId == 1 ? t.Quantity * unitValue : 0,
                        SellQuantity = t.OperationId == 1 ? t.Quantity : 0,
                    };
                });

            var tradesWithLots = new List<TradeWithLot>();
            int lotGroupId = 1;
            decimal runningQuantity = 0;

            foreach (var t in orderedTrades)
            {
                // A new lot starts when the position is flat, or when it flips from long to short (or vice-versa).
                if (runningQuantity == 0 || Math.Sign(runningQuantity) * Math.Sign(runningQuantity + t.SignedQuantity) == -1)
                {
                    if (tradesWithLots.Any()) // Increment LotGroupId for subsequent lots
                    {
                        lotGroupId++;
                    }
                }

                tradesWithLots.Add(new TradeWithLot
                {
                    Tx = t.Tx,
                    SignedQuantity = t.SignedQuantity,
                    BuyValue = t.BuyValue,
                    BuyQuantity = t.BuyQuantity,
                    SellValue = t.SellValue,
                    SellQuantity = t.SellQuantity,
                    LotGroupId = lotGroupId
                });

                runningQuantity += t.SignedQuantity;
            }
            return tradesWithLots;
        }

        private static List<PerTransactionState> CalculatePerTransactionState(IEnumerable<TradeWithLot> tradesWithLots)
        {
            return tradesWithLots
                .GroupBy(t => t.LotGroupId)
                .SelectMany(lot =>
                {
                    decimal runningQty = 0, runningBuyValue = 0, runningBuyQty = 0, runningSellValue = 0, runningSellQty = 0, runningFees = 0;
                    return lot.Select(t =>
                    {
                        runningQty += t.SignedQuantity;
                        runningBuyValue += t.BuyValue;
                        runningBuyQty += t.BuyQuantity;
                        runningSellValue += t.SellValue;
                        runningSellQty += t.SellQuantity;
                        runningFees += t.Tx.Fees;

                        return new PerTransactionState
                        {
                            Tx = t.Tx,
                            LotGroupId = t.LotGroupId,
                            OpenQuantity = runningQty,
                            TotalBuyValue = runningBuyValue,
                            TotalBuyQuantity = runningBuyQty,
                            TotalSellValue = runningSellValue,
                            TotalSellQuantity = runningSellQty,
                            TotalCommission = runningFees
                        };
                    });
                })
                .ToList();
        }

        // Private helper classes to improve readability over anonymous types
        private class TradeWithLot
        {
            public SecurityTransactionLog Tx { get; set; } = new();
            public decimal SignedQuantity { get; set; }
            public decimal BuyValue { get; set; }
            public decimal BuyQuantity { get; set; }
            public decimal SellValue { get; set; }
            public decimal SellQuantity { get; set; }
            public int LotGroupId { get; set; }
        }

        private class PerTransactionState
        {
            public SecurityTransactionLog Tx { get; set; } = new();
            public int LotGroupId { get; set; }
            public decimal OpenQuantity { get; set; }
            public decimal TotalBuyValue { get; set; }
            public decimal TotalBuyQuantity { get; set; }
            public decimal TotalSellValue { get; set; }
            public decimal TotalSellQuantity { get; set; }
            public decimal TotalCommission { get; set; }
        }
    }
}