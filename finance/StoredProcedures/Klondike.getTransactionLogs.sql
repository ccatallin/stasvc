SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		Catalin Calugaroiu
-- Create date: 2024-03-28 23:12:04 PM
-- Create date: 2025-10-13 20:06:05 PM
-- Description:	Get all transactions from TransactionLog
-- =============================================
CREATE PROCEDURE [Klondike].[getTransactionLogs] 

@UserId as bigint,
@ClientId bigint,
@ProductCategoryId AS int = NULL,
@ProductId AS int = NULL,
@ProductSymbol AS varchar(255) = NULL,
@StartDate as datetime = NULL,
@EndDate as datetime = NULL

AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

    SELECT [Id], [Date], [OperationId], [ProductCategoryId], [ProductId], 
           [ProductSymbol], [Quantity], [Price], [Fees], [Notes]

	    FROM [Klondike].[TransactionLogs] AS TL 
		
            WHERE ((TL.[ClientId] = @ClientId) AND
                   (TL.[CreatedById] = @UserId) AND
                   (@ProductCategoryId IS NULL OR TL.[ProductCategoryId] = @ProductCategoryId) AND
                   (@ProductId IS NULL OR TL.[ProductId] = @ProductId) AND
                   (@ProductSymbol IS NULL OR TL.[ProductSymbol] = @ProductSymbol) AND
                   (@StartDate IS NULL OR TL.[Date] >= @StartDate) AND
                   (@EndDate IS NULL OR TL.[Date] < @EndDate))
	
        ORDER BY [ProductCategoryId], [ProductId], [ProductSymbol], [Date];
END
GO
