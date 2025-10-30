DROP PROCEDURE [Klondike].[updatePositionSnapshots];
GO

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
            tl.[Date] AS TransactionDate,
            tl.[Id] AS TransactionId,
            tl.[OperationId] AS TransactionType,
            tl.[ProductCategoryId],
            tl.[ProductId],
            tl.[ProductSymbol],
            tl.[Quantity],
            tl.[Price],
            tl.[Fees] AS TransactionFees,
            IIF(tl.[OperationId] = -1, tl.[Quantity], -tl.[Quantity]) AS SignedQuantity,
            IIF(tl.[OperationId] = -1, (tl.[Quantity] * tl.[Price] * ISNULL(pc.Multiplier, 1)) + tl.[Fees], 0) AS BuyCost,
            IIF(tl.[OperationId] = -1, tl.[Quantity], 0) AS BuyQuantity,
            IIF(tl.[OperationId] = 1, (tl.[Quantity] * tl.[Price] * ISNULL(pc.Multiplier, 1)) - tl.[Fees], 0) AS SellValue,
            IIF(tl.[OperationId] = 1, tl.[Quantity], 0) AS SellQuantity
        FROM Klondike.TransactionLogs AS tl
        LEFT JOIN Klondike.ProductCategories AS pc
            ON tl.ProductCategoryId = pc.Id
        WHERE
            tl.ClientId = @ClientId
            AND tl.ProductSymbol = @ProductSymbol
    ),
    RunningTotals AS (
        SELECT
            *,
            -- Running quantity is used to determine the start of a new lot.
            ISNULL(SUM(SignedQuantity) OVER (
                PARTITION BY ProductSymbol
                ORDER BY TransactionDate, TransactionId
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ), 0) AS PreviousRunningQuantity,
            -- These running totals are for the entire history of the product,
            -- which will be used later to calculate lot-specific values.
            SUM(BuyQuantity) OVER (
                PARTITION BY ProductSymbol ORDER BY TransactionDate, TransactionId
            ) AS CumulativeBuyQuantity,
            SUM(BuyCost) OVER (
                PARTITION BY ProductSymbol ORDER BY TransactionDate, TransactionId
            ) AS CumulativeBuyCost,
            SUM(SellQuantity) OVER (
                PARTITION BY ProductSymbol ORDER BY TransactionDate, TransactionId
            ) AS CumulativeSellQuantity,
            SUM(SellValue) OVER (
                PARTITION BY ProductSymbol ORDER BY TransactionDate, TransactionId
            ) AS CumulativeSellValue
        FROM OrderedTrades
    ),
    LotIdentifier AS (
        SELECT
            *,
            -- This simplified CASE statement correctly identifies a new lot only when
            -- starting from zero or when the position *crosses* zero (flips sign).
            -- It correctly keeps closing trades within the same lot.
            CASE
                WHEN PreviousRunningQuantity = 0 THEN 1
                -- A new lot is only created if the sign of the position *flips*.
                -- A closing trade that results in zero should NOT start a new lot.
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
    DailyState AS (
        SELECT
            CAST(TransactionDate AS DATE) AS SnapshotDate,
            LotGroupID,
            MAX(ProductCategoryId) AS ProductCategoryId,
            MAX(ProductId) AS ProductId,
            SUM(SignedQuantity) AS NetQuantityChange, -- Daily change is fine
            SUM(BuyCost) AS NetBuyCost,
            SUM(BuyQuantity) AS NetBuyQuantity,
            SUM(SellValue) AS NetSellValue,
            SUM(SellQuantity) AS NetSellQuantity,
            SUM(TransactionFees) AS NetCommission -- Daily change
        FROM LotGroups -- Reverting to use LotGroups directly
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
            SUM(NetCommission) OVER (PARTITION BY LotGroupID ORDER BY SnapshotDate) AS TotalCommission -- Commission is still a simple running sum
        FROM DailyState
    ),
    FinalDailyState AS (
        -- This CTE handles the case where multiple lots are opened and closed on the same day.
        -- It ensures that only one snapshot is created per day by selecting the final state
        -- of the day, which corresponds to the highest LotGroupID for that day.
        SELECT
            *,
            ROW_NUMBER() OVER(PARTITION BY SnapshotDate ORDER BY LotGroupID DESC) as rn
        FROM RunningDailyState
    )
    --
    -- Step 5: Insert the calculated daily snapshots into the summary table.
    -- The final SELECT now filters to only the last state of each day (rn = 1)
    -- to prevent duplicate key errors when a position is closed and reopened on the same day.
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
        (TotalBuyCost - TotalSellValue) AS Cost,
        TotalCommission AS Commission,
        -- Calculate the average price based on whether it's a long or short position
        IIF(
            OpenQuantity = 0, 0, -- Avoid division by zero for closed positions
            IIF(
                OpenQuantity > 0,
                TotalBuyCost / NULLIF(TotalBuyQuantity, 0),
                TotalSellValue / NULLIF(TotalSellQuantity, 0))
        ) AS AveragePrice
    FROM FinalDailyState
    WHERE rn = 1;

END
GO
