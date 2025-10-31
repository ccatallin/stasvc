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
CREATE OR ALTER PROCEDURE [Klondike].[updatePositionSnapshots]
    @UserId AS BIGINT,
    @ClientId AS BIGINT,
    @ProductSymbol AS NVARCHAR(255)
AS
BEGIN
    SET NOCOUNT ON;

    -- Step 1: Delete all existing snapshots for this specific product and user.
    -- This ensures we are starting with a clean slate for the recalculation.
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
            -- This is the total value of the transaction, excluding fees.
            -- It correctly multiplies Quantity * Price * Multiplier for all asset types.
            CASE
                WHEN tl.ProductCategoryId = 3 AND p.ContractMultiplier IS NOT NULL THEN tl.Quantity * tl.Price * p.ContractMultiplier -- Futures
                ELSE tl.Quantity * tl.Price * ISNULL(pc.Multiplier, 1) -- Options/Equities
            END AS GrossValue
        FROM Klondike.TransactionLogs AS tl
        LEFT JOIN Klondike.ProductCategories AS pc
            ON tl.ProductCategoryId = pc.Id
        LEFT JOIN Klondike.Products AS p
            ON tl.ProductId = p.Id AND tl.ProductCategoryId = p.ProductCategoryId
        WHERE
            tl.ClientId = @ClientId
            AND tl.ProductSymbol = @ProductSymbol
    ),
    RunningTotals AS (
        SELECT
            *,
            -- Calculate cost/value fields for the current transaction
            IIF(TransactionType = -1, GrossValue, 0) AS BuyGrossValue, -- Gross value of buys (for avg price)
            IIF(TransactionType = -1, Quantity * Price, 0) AS BuyValue, -- Value without multiplier (for per-unit avg price)
            IIF(TransactionType = -1, GrossValue + TransactionFees, 0) AS BuyCost, -- Total cost of buys (incl. fees), used for P/L
            IIF(TransactionType = -1, Quantity, 0) AS BuyQuantity,
            IIF(TransactionType = 1, GrossValue, 0) AS SellGrossValue, -- Gross value of sells (for avg price)
            IIF(TransactionType = 1, Quantity * Price, 0) AS SellValue, -- Value without multiplier (for per-unit avg price)
            IIF(TransactionType = 1, GrossValue - TransactionFees, 0) AS SellProceeds, -- Net proceeds from sells (after fees), used for P/L
            IIF(TransactionType = 1, Quantity, 0) AS SellQuantity,
            -- Running quantity is used to determine the start of a new lot.
            ISNULL(SUM(SignedQuantity) OVER (
                PARTITION BY ProductSymbol
                ORDER BY TransactionDate, TransactionId
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ), 0) AS PreviousRunningQuantity
            -- No need for separate cumulative columns here; they will be calculated in the DailyState CTEs.
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
            SUM(SignedQuantity) AS NetQuantityChange,
            SUM(BuyGrossValue) AS NetBuyGrossValue, -- Daily total
            SUM(BuyValue) AS NetBuyValue,
            SUM(BuyCost) AS NetBuyCost, -- Daily total
            SUM(BuyQuantity) AS NetBuyQuantity, -- Daily total
            SUM(SellProceeds) AS NetSellProceeds, -- Daily total
            SUM(SellValue) AS NetSellValue,
            SUM(SellGrossValue) AS NetSellGrossValue, -- Daily total
            SUM(SellQuantity) AS NetSellQuantity, -- Daily total
            SUM(TransactionFees) AS NetCommission
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
            SUM(NetBuyGrossValue) OVER (PARTITION BY LotGroupID ORDER BY SnapshotDate) AS TotalBuyGrossValue, -- Cumulative per lot
            SUM(NetBuyValue) OVER (PARTITION BY LotGroupID ORDER BY SnapshotDate) AS TotalBuyValue,
            SUM(NetBuyCost) OVER (PARTITION BY LotGroupID ORDER BY SnapshotDate) AS TotalBuyCost, -- Cumulative per lot
            SUM(NetBuyQuantity) OVER (PARTITION BY LotGroupID ORDER BY SnapshotDate) AS TotalBuyQuantity, -- Cumulative per lot
            SUM(NetSellProceeds) OVER (PARTITION BY LotGroupID ORDER BY SnapshotDate) AS TotalSellProceeds, -- Cumulative per lot
            SUM(NetSellGrossValue) OVER (PARTITION BY LotGroupID ORDER BY SnapshotDate) AS TotalSellGrossValue, -- Cumulative per lot
            SUM(NetSellValue) OVER (PARTITION BY LotGroupID ORDER BY SnapshotDate) AS TotalSellValue,
            SUM(NetSellQuantity) OVER (PARTITION BY LotGroupID ORDER BY SnapshotDate) AS TotalSellQuantity, -- Cumulative per lot
            SUM(NetCommission) OVER (PARTITION BY LotGroupID ORDER BY SnapshotDate) AS TotalCommission
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
    -- Step 3: Materialize the final daily states into a temporary table.
    -- This can improve performance by preventing re-calculation and providing better statistics to the optimizer.
    SELECT
        ProductCategoryId,
        ProductId,
        SnapshotDate,
        OpenQuantity,
        TotalBuyGrossValue,
        TotalBuyValue,
        TotalBuyCost,
        TotalBuyQuantity,
        TotalSellProceeds,
        TotalSellValue,
        TotalSellGrossValue,
        TotalSellQuantity,
        TotalCommission
    INTO #FinalSnapshots
    FROM FinalDailyState
    WHERE rn = 1;

    -- Step 3.5: Calculate the final AveragePrice and Cost in a new CTE
    -- This makes the logic clearer by separating the calculation from the final insert.
    WITH FinalSnapshotsWithAvgPrice AS (
        SELECT fs.*,
            ISNULL(pc.Multiplier, 1) AS Multiplier,
            IIF(
                OpenQuantity = 0, 0,
                IIF(
                    OpenQuantity > 0, TotalBuyValue / NULLIF(TotalBuyQuantity, 0), -- Avg Price for Longs (per unit)
                    TotalSellValue / NULLIF(TotalSellQuantity, 0)  -- Avg Price for Shorts (per unit)
                )
            ) AS CalculatedAveragePrice
        FROM #FinalSnapshots fs
        LEFT JOIN Klondike.ProductCategories pc ON fs.ProductCategoryId = pc.Id)
    -- Step 4: Insert the calculated daily snapshots from the temporary table into the summary table.
    -- This separation makes the logic clearer and can be more performant.
    -- The logic correctly handles:
    --   - Cost basis for long and short positions.
    --   - Average price calculation, including for short positions (using gross sell value).
    --   - Avoidance of divide-by-zero errors.
    --   - Prevention of duplicate key errors for same-day close/reopen scenarios (handled by rn=1 filter).
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
        -- Cost is now correctly calculated from the per-unit AveragePrice and Multiplier.
        (OpenQuantity * CalculatedAveragePrice * Multiplier) AS Cost,
        TotalCommission AS Commission,
        CalculatedAveragePrice AS AveragePrice
    FROM FinalSnapshotsWithAvgPrice;

END
GO
