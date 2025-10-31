SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:      Catalin Calugaroiu & Gemini Code Assist
-- Create date: 2025-10-16
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

    -- Step 2: Use a simplified recalculation logic to generate the history of lots and per-transaction states.
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
            ,
            -- Calculate the per-unit value, applying special logic for ZB futures (ProductId=5).
            -- For ZB, the price (e.g., 117.18) is converted from points and 32nds to a full dollar value.
            -- For all other products, it's simply the price.
            CASE
                WHEN tl.ProductCategoryId = 3 AND tl.ProductId = 5 THEN
                    (FLOOR(tl.Price) + (tl.Price - FLOOR(tl.Price)) * 100 / 32) * 1000
                ELSE tl.Price
            END AS UnitValue
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
            IIF(TransactionType = -1, Quantity * UnitValue, 0) AS BuyValue, -- Value based on UnitValue (for per-unit avg price)
            IIF(TransactionType = -1, GrossValue + TransactionFees, 0) AS BuyCost, -- Total cost of buys (incl. fees), used for P/L
            IIF(TransactionType = -1, Quantity, 0) AS BuyQuantity,
            IIF(TransactionType = 1, GrossValue, 0) AS SellGrossValue, -- Gross value of sells (for avg price)
            IIF(TransactionType = 1, Quantity * UnitValue, 0) AS SellValue, -- Value based on UnitValue (for per-unit avg price)
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
    ), PerTransactionState AS (
        -- Step 3a: Calculate the cumulative state of the position *after* each transaction within its lot.
        SELECT
            lg.ProductCategoryId,
            lg.ProductId,
            lg.LotGroupID,
            CAST(lg.TransactionDate AS DATE) AS SnapshotDate,
            lg.TransactionDate,
            lg.TransactionId,
            ISNULL(pc.Multiplier, 1) AS Multiplier,
            SUM(lg.SignedQuantity) OVER (PARTITION BY lg.LotGroupID ORDER BY lg.TransactionDate, lg.TransactionId) AS OpenQuantity,
            SUM(lg.BuyValue) OVER (PARTITION BY lg.LotGroupID ORDER BY lg.TransactionDate, lg.TransactionId) AS TotalBuyValue,
            SUM(lg.BuyQuantity) OVER (PARTITION BY lg.LotGroupID ORDER BY lg.TransactionDate, lg.TransactionId) AS TotalBuyQuantity,
            SUM(lg.SellValue) OVER (PARTITION BY lg.LotGroupID ORDER BY lg.TransactionDate, lg.TransactionId) AS TotalSellValue,
            SUM(lg.SellQuantity) OVER (PARTITION BY lg.LotGroupID ORDER BY lg.TransactionDate, lg.TransactionId) AS TotalSellQuantity,
            SUM(lg.TransactionFees) OVER (PARTITION BY lg.LotGroupID ORDER BY lg.TransactionDate, lg.TransactionId) AS TotalCommission
        FROM LotGroups lg
        LEFT JOIN Klondike.ProductCategories AS pc ON lg.ProductCategoryId = pc.Id
    ),
    DailySnapshots AS (
        -- Step 3b: For each day, select the state from the *last* transaction of that day.
        SELECT
            pts.ProductCategoryId,
            pts.ProductId,
            pts.SnapshotDate,
            pts.OpenQuantity,
            pts.TotalCommission,
            pts.Multiplier,
            -- Calculate the average price based on the position's state at the end of the day.
            ISNULL(
                IIF(
                    pts.OpenQuantity > 0,
                    pts.TotalBuyValue / NULLIF(pts.TotalBuyQuantity, 0), -- Avg price for long positions
                    -- For short positions, use the avg price from the first sell(s) of the lot.
                    FIRST_VALUE(pts.TotalSellValue / NULLIF(pts.TotalSellQuantity, 0)) OVER (PARTITION BY pts.LotGroupID ORDER BY pts.SnapshotDate)
                ), 0
            ) AS AveragePrice,
            -- This identifies the last record for each day to ensure we only insert one snapshot per day.
            ROW_NUMBER() OVER(PARTITION BY pts.SnapshotDate ORDER BY pts.TransactionDate DESC, pts.TransactionId DESC) as rn
        FROM PerTransactionState pts
    )
    -- Step 3: Insert the calculated daily snapshots into the summary table.
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
    -- Step 3c: Final SELECT and INSERT into the snapshot table.
    SELECT
        @UserId,
        @ClientId,
        ds.ProductCategoryId,
        ds.ProductId,
        @ProductSymbol,
        ds.SnapshotDate,
        ds.OpenQuantity,
        -- Cost is calculated from the final state: OpenQuantity * AveragePrice * Multiplier
        IIF(ds.OpenQuantity = 0, 0, ds.OpenQuantity * ds.AveragePrice * IIF(ds.ProductId = 5 AND ds.ProductCategoryId = 3, 1, ds.Multiplier)) AS Cost,
        ds.TotalCommission,
        ds.AveragePrice
    FROM DailySnapshots ds
    WHERE ds.rn = 1; -- Insert only the last state for each day.

END
GO
