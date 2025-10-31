SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		Catalin Calugaroiu
-- Create date: 2025-10-15
-- Description:	Calculates realized and unrealized profit and loss for a client.
--              This procedure leverages the pre-calculated lot data from
--              the transaction log recalculation logic, making it both
--              accurate and performant. It replaces the older, less
--              efficient getProfitAndLoss procedure.
-- =============================================
CREATE OR ALTER PROCEDURE [Klondike].[getProfitAndLoss]

@UserId AS bigint,
@ClientId AS bigint,
@ProductCategoryId AS int = NULL,
@ProductId AS int = NULL,
@ProductSymbol AS varchar(255) = NULL,
@StartDate as datetime = NULL,
@EndDate as datetime = NULL,
@realized AS smallint = 1 -- 1 for Realized, 0 for Unrealized

AS
BEGIN
    SET NOCOUNT ON;

    WITH OrderedTrades AS (
        SELECT -- Added a LEFT JOIN to ProductCategories to get the multiplier dynamically
            tl.[Date] AS TransactionDate,
            tl.[Id] AS TransactionId,
            tl.[OperationId],
            tl.[ProductCategoryId],
            tl.[ProductId],
            tl.[ProductSymbol],
            tl.[Quantity],
            tl.[Price],
            tl.[Fees],
            IIF(tl.[OperationId] = -1, tl.[Quantity], -tl.[Quantity]) AS SignedQuantity,
            -- Calculate cost basis for buys and proceeds for sells.
            -- This CASE statement correctly calculates the gross value for all products,
            -- including the special price conversion for ZB futures (ProductId=5).
            CASE
                -- For ZB futures, convert the fractional price (e.g., 117.18) to its full dollar value.
                WHEN tl.ProductCategoryId = 3 AND tl.ProductId = 5 THEN
                    tl.Quantity * ((FLOOR(tl.Price) + (tl.Price - FLOOR(tl.Price)) * 100 / 32) * 1000)
                -- For other futures, use the standard contract multiplier.
                WHEN tl.ProductCategoryId = 3 THEN (tl.[Quantity] * tl.[Price] * COALESCE(p.ContractMultiplier, 1))
                ELSE (tl.[Quantity] * tl.[Price] * COALESCE(pc.Multiplier, 1))
            END AS GrossValue
        FROM Klondike.TransactionLogs AS tl
        LEFT JOIN [Klondike].[ProductCategories] AS pc 
            ON tl.ProductCategoryId = pc.Id
        LEFT JOIN [Klondike].[Products] AS p
            ON tl.ProductId = p.Id AND tl.ProductCategoryId = p.ProductCategoryId -- Join on ProductId and CategoryId
        WHERE
            (ClientId = @ClientId)
            AND (@ProductCategoryId IS NULL OR tl.[ProductCategoryId] = @ProductCategoryId)
            AND (@ProductId IS NULL OR tl.[ProductId] = @ProductId)
            AND (@ProductSymbol IS NULL OR [ProductSymbol] = @ProductSymbol)
            AND (@StartDate IS NULL OR [Date] >= @StartDate)
            AND (@EndDate IS NULL OR [Date] < @EndDate)
            AND [IsDeleted] = 0
    ),
    RunningTotals AS (
        SELECT
            *,
            ISNULL(SUM(SignedQuantity) OVER (
                PARTITION BY ProductSymbol
                ORDER BY TransactionDate, TransactionId
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ), 0) AS PreviousRunningQuantity
        FROM OrderedTrades
    ),
    LotIdentifier AS (
        SELECT
            *,
            CASE
                WHEN PreviousRunningQuantity = 0 THEN 1
                WHEN SIGN(PreviousRunningQuantity) * SIGN(PreviousRunningQuantity + SignedQuantity) = -1 THEN 1
                ELSE 0
            END AS IsNewLot
        FROM RunningTotals
    ),
    LotGroups AS (
        SELECT
            *,
            SUM(IsNewLot) OVER (
                PARTITION BY ProductSymbol
                ORDER BY TransactionDate, TransactionId
            ) AS LotGroupID
        FROM LotIdentifier
    ),
    LotSummary AS (
        SELECT
            l.ProductSymbol,
            l.ProductCategoryId,
            l.ProductId,
            l.LotGroupID,
            MIN(l.TransactionDate) AS FirstTransactionDate,
            MAX(l.TransactionDate) AS LastTransactionDate,
            SUM(l.SignedQuantity) AS NetQuantity,
            -- Gross P/L (before fees): Proceeds (SELL=1) - Cost (BUY=-1)
            SUM(IIF(l.OperationId = 1, l.GrossValue, -l.GrossValue)) AS RealizedPL,
            SUM(l.Fees) AS TotalFees
        FROM LotGroups l
        GROUP BY l.ProductSymbol, l.ProductCategoryId, l.ProductId, l.LotGroupID
    )
    SELECT
        l.ProductSymbol,
        l.ProductCategoryId,
        l.ProductId,
        l.FirstTransactionDate,
        l.LastTransactionDate,
        l.NetQuantity AS OpenContracts,
        l.RealizedPL AS Profit,
        l.TotalFees AS Fees,
        (l.RealizedPL - l.TotalFees) AS Total -- Net Profit (after fees)
    FROM LotSummary l
    WHERE
        -- Realized lots are those that are now closed (NetQuantity = 0)
        (@realized = 1 AND l.NetQuantity = 0)
        OR
        -- Unrealized lots are the most recent lots that are still open
        (@realized = 0 AND l.NetQuantity <> 0 AND l.LotGroupID = (
            SELECT MAX(s.LotGroupID)
            FROM LotGroups s
            WHERE s.ProductSymbol = l.ProductSymbol
        ))
    ORDER BY
        l.ProductCategoryId, l.ProductSymbol, l.LastTransactionDate ASC;

END
GO