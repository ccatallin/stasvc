SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:      Catalin Calugaroiu
-- Create date: 2025-10-09
-- Description: Recalculates and updates the daily snapshots for a specific product.
--              This is the "heavy" part of the calculation, intended to be
--              run when a transaction is created, updated, or deleted.
-- =============================================
CREATE PROCEDURE [Klondike].[updatePositionSnapshots]
    @UserId AS BIGINT,
    @ClientId AS BIGINT,
    @ProductSymbol AS NVARCHAR(255)
AS
BEGIN
    SET NOCOUNT ON;

    -- Step 1: Delete all existing snapshots for this specific product and user.
    DELETE FROM Klondike.PositionSnapshots
    WHERE
        ClientId = @ClientId
        AND ProductSymbol = @ProductSymbol;

    -- Step 2: Use the full recalculation logic to generate the history of lots and daily states.
    WITH OrderedTrades AS (
        SELECT
            [Date] AS TransactionDate,
            [Id] AS TransactionId,
            [OperationId] AS TransactionType,
            [ProductCategoryId],
            [ProductId],
            [ProductSymbol],
            [Quantity],
            [Price],
            [Fees] AS TransactionFees,
            IIF([OperationId] = -1, [Quantity], -[Quantity]) AS SignedQuantity,
            IIF([OperationId] = -1, ([Quantity] * [Price]) + [Fees], 0) AS BuyCost,
            IIF([OperationId] = -1, [Quantity], 0) AS BuyQuantity,
            IIF([OperationId] = 1, ([Quantity] * [Price]) - [Fees], 0) AS SellValue,
            IIF([OperationId] = 1, [Quantity], 0) AS SellQuantity
        FROM Klondike.TransactionLogs WITH (NOLOCK)
        WHERE
            ClientId = @ClientId
            AND ProductSymbol = @ProductSymbol
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
            IIF((TransactionType = -1 AND PreviousRunningQuantity <= 0) OR (TransactionType = 1 AND PreviousRunningQuantity >= 0), 1, 0) AS IsNewLot
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
    DailyState AS (
        SELECT
            CAST(TransactionDate AS DATE) AS SnapshotDate,
            LotGroupID,
            MAX(ProductCategoryId) AS ProductCategoryId,
            MAX(ProductId) AS ProductId,
            SUM(SignedQuantity) AS NetQuantityChange,
            SUM(BuyCost) AS NetBuyCost,
            SUM(BuyQuantity) AS NetBuyQuantity,
            SUM(SellValue) AS NetSellValue,
            SUM(SellQuantity) AS NetSellQuantity,
            SUM(TransactionFees) AS NetCommission
        FROM LotGroups
        GROUP BY CAST(TransactionDate AS DATE), LotGroupID
    ),
    RunningDailyState AS (
        SELECT
            SnapshotDate,
            LotGroupID,
            ProductCategoryId,
            ProductId,
            SUM(NetQuantityChange) OVER (PARTITION BY LotGroupID ORDER BY SnapshotDate) AS OpenQuantity,
            SUM(NetBuyCost) OVER (PARTITION BY LotGroupID ORDER BY SnapshotDate) AS TotalBuyCost,
            SUM(NetBuyQuantity) OVER (PARTITION BY LotGroupID ORDER BY SnapshotDate) AS TotalBuyQuantity,
            SUM(NetSellValue) OVER (PARTITION BY LotGroupID ORDER BY SnapshotDate) AS TotalSellValue,
            SUM(NetSellQuantity) OVER (PARTITION BY LotGroupID ORDER BY SnapshotDate) AS TotalSellQuantity,
            SUM(NetCommission) OVER (PARTITION BY LotGroupID ORDER BY SnapshotDate) AS TotalCommission
        FROM DailyState
    ),
    FinalCalculation AS (
        -- This CTE makes OpenQuantity available for the IIF statement below.
        SELECT
            *,
            (TotalBuyCost - TotalSellValue) AS Cost,
            TotalCommission AS Commission
        FROM RunningDailyState
    )
    -- Step 5: Insert the calculated daily snapshots into the summary table.
    INSERT INTO Klondike.PositionSnapshots (
        UserId,
        ClientId,
        ProductCategoryId,
        ProductId,
        ProductSymbol,
        SnapshotDate,
        Quantity,
        Cost,
        Commission,
        AveragePrice
    )
    SELECT
        @UserId,
        @ClientId,
        ProductCategoryId,
        ProductId,
        @ProductSymbol,
        SnapshotDate,
        OpenQuantity,
        Cost,
        Commission,
        -- Calculate the average price based on whether it's a long or short position
        IIF(
            OpenQuantity > 0,
            TotalBuyCost / NULLIF(TotalBuyQuantity, 0),
            TotalSellValue / NULLIF(TotalSellQuantity, 0)
        ) AS AveragePrice
    FROM FinalCalculation;

END
GO
