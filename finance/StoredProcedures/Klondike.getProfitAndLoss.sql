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
CREATE PROCEDURE [Klondike].[getProfitAndLoss]
    @UserId AS BIGINT,
    @ClientId AS BIGINT,
    @StartDate AS DATETIME = NULL,
    @EndDate AS DATETIME = NULL,
    @realized AS SMALLINT = 1 -- 1 for Realized, 0 for Unrealized
AS
BEGIN
    SET NOCOUNT ON;

    WITH OrderedTrades AS (
        SELECT
            [Date] AS TransactionDate,
            [Id] AS TransactionId,
            [OperationId],
            [ProductCategoryId],
            [ProductId],
            [ProductSymbol],
            [Quantity],
            [Price],
            [Fees],
            IIF([OperationId] = -1, [Quantity], -[Quantity]) AS SignedQuantity,
            -- Calculate cost basis for buys and proceeds for sells
            -- Note: This assumes a simple multiplier for stocks/ETFs vs. other instruments.
            -- A product-specific multiplier would be more robust.
            ([Quantity] * [Price] * IIF([ProductCategoryId] = 2, 100, 1)) AS GrossValue
        FROM Klondike.TransactionLogs
        WHERE
            ClientId = @ClientId
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
            ProductSymbol,
            LotGroupID,
            MIN(TransactionDate) AS FirstTransactionDate,
            MAX(TransactionDate) AS LastTransactionDate,
            SUM(SignedQuantity) AS NetQuantity,
            -- Realized P/L is the sum of all transaction values within the lot
            SUM(IIF(OperationId = 1, GrossValue, -GrossValue)) - SUM(Fees) AS RealizedPL,
            SUM(Fees) AS TotalFees
        FROM LotGroups
        GROUP BY ProductSymbol, LotGroupID
    )
    SELECT
        l.ProductSymbol,
        l.FirstTransactionDate,
        l.LastTransactionDate,
        l.NetQuantity AS OpenContracts,
        l.RealizedPL AS Profit,
        l.TotalFees AS Fees,
        l.RealizedPL AS Total -- Profit already includes fees
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
        l.LastTransactionDate ASC;

END
GO
