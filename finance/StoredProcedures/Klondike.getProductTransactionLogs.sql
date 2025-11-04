SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		Catalin Calugaroiu
-- Create date: 2025-10-08 13:25:10 AM
-- Description:	Get all transactions from TransactionLog for a given product and client
-- NOT USED ANYMORE KEEP 4 HISTORY
-- =============================================
CREATE PROCEDURE [Klondike].[getProductTransactionLogs] 

@ProductSymbol as VARCHAR(255),
@UserId as bigint,
@ClientId as bigint

AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

    IF (-1001 <> @ClientId)
        SELECT  [Id],
                CONVERT(VARCHAR(19), [Date], 126) AS [Date],
                [OperationId],
                [ProductCategoryId],
                [ProductId],
                [ProductSymbol],
                [Quantity],
                [Price],
                [Fees]

        FROM [Klondike].[TransactionLogs] AS T 
            -- INNER JOIN [Klondike].[ProductTypes] AS PT 
                -- ON PT.[ProductTypeID] = T.[ProductTypeID]
            WHERE  ((T.[ClientID] = @ClientId) AND
                    (T.[ProductSymbol] = @ProductSymbol))

        ORDER BY [Date] DESC;
    ELSE
        SELECT  [Id],
                CONVERT(VARCHAR(19), [Date], 126) AS [Date],
                [OperationId],
                [ProductCategoryId],
                [ProductId],
                [ProductSymbol],
                [Quantity],
                [Price],
                [Fees]
        
        FROM [Klondike].[TransactionLogs] AS T 
            -- INNER JOIN [Klondike].[ProductTypes] AS PT 
                -- ON PT.[ProductTypeID] = T.[ProductTypeID]
            WHERE (T.[ProductSymbol] = @ProductSymbol)

        ORDER BY [Date] ASC;
END
GO
