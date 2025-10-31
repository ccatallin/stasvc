SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:      Catalin Calugaroiu & Gemini Code Assist
-- Create date: 2025-10-17
-- Description: Fetches all transaction data required by the C# 
--              PositionSnapshotCalculator for a specific product.
-- =============================================
CREATE OR ALTER PROCEDURE [Klondike].[getTransactionsForSnapshot]

@UserId AS BIGINT, -- just in case I will do the calculation per user and per client
@ClientId AS BIGINT,
@ProductSymbol AS NVARCHAR(255)

AS
BEGIN
    SET NOCOUNT ON;

    SELECT
        tl.*,
        ISNULL(p.ContractMultiplier, 1) AS ContractMultiplier,
        ISNULL(pc.Multiplier, 1) AS CategoryMultiplier
    FROM 
        [Klondike].[TransactionLogs] AS tl
    LEFT JOIN 
        [Klondike].[ProductCategories] AS pc ON tl.ProductCategoryId = pc.Id
    LEFT JOIN 
        [Klondike].[Products] AS p ON tl.ProductId = p.Id AND tl.ProductCategoryId = p.ProductCategoryId
    WHERE 
        tl.ClientId = @ClientId AND tl.ProductSymbol = @ProductSymbol
    ORDER BY 
        tl.Date ASC;
END
GO