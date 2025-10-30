SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		Catalin Calugaroiu
-- Create date: 2025-10-08 20:21:34 PM
-- Description:	Get open positions
-- =============================================
CREATE PROCEDURE [Klondike].[getOpenPositions]

@UserId as bigint,
@ClientId as bigint

AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
    SET NOCOUNT ON;
   -- This new version reads from the pre-calculated snapshots table, which is much faster.
    -- The heavy calculation is now handled by a separate procedure that updates the snapshots
    -- only when a transaction is added, updated, or deleted.

    WITH LatestSnapshots AS (
        -- 1. Find the most recent snapshot for each product for the user.
        SELECT  ps.[ProductCategoryId],
                ps.[ProductId],
                ps.[ProductSymbol],
                ps.[Quantity],
                ps.[Cost],
                ps.[Commission],
                ps.[AveragePrice],
                ISNULL(pc.Multiplier, 1) AS Multiplier,
                -- We need to get some details from the first transaction of the lot,
                -- which requires joining back to the transaction table.
                -- This is a small lookup and still very fast.
                ROW_NUMBER() OVER(PARTITION BY ps.ProductSymbol ORDER BY ps.SnapshotDate DESC) as rn

            FROM [Klondike].[PositionSnapshots] AS ps WITH (NOLOCK)
            LEFT JOIN [Klondike].[ProductCategories] AS pc
                ON ps.ProductCategoryId = pc.Id
            WHERE (ps.[ClientId] = @ClientId)
    )
    -- 2. Select the latest snapshots that have a non-zero quantity.
    SELECT  [ProductCategoryId],
            [ProductId],
            [ProductSymbol],
            [Quantity],
            -- For products with a multiplier (like options), the stored AveragePrice is per contract.
            -- We divide by the multiplier to get the per-unit/per-share price for display.
            IIF(Multiplier > 1, [AveragePrice] / Multiplier, [AveragePrice]) AS [AveragePrice],
            [Cost],
            [Commission],
            ([Cost] + [Commission]) AS TotalCost

        FROM LatestSnapshots
            WHERE   ((rn = 1) AND 
                     ([Quantity] <> 0));
END
GO
