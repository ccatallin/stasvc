SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		Catalin Calugaroiu
-- Create date: 2025-10-08 13:25:10 AM
-- Description:	Get all transactions from TransactionLog for a given product and client
-- =============================================
CREATE PROCEDURE [Klondike].[getProductTransactionLogs] 

@sProductName as VARCHAR(255),
@UserId as bigint,
@ClientId as bigint

AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

    IF (-1001 <> @ClientId)
        SELECT  [TransactionId],
                CONVERT(VARCHAR(19), [TransactionDate], 126) AS [TransactionDate],
                [TransactionType],
                [ProductName],
                [ProductTypeId],
                [NoContracts],
                [ContractPrice],
                [TransactionFees]
        
        FROM [Klondike].[Transactions] AS T 
            -- INNER JOIN [Klondike].[ProductTypes] AS PT 
                -- ON PT.[ProductTypeID] = T.[ProductTypeID]
            WHERE ((T.[ClientID] = @ClientId) AND
                (T.[ProductName] = @sProductName))

        ORDER BY [TransactionDate] DESC;
    ELSE
        SELECT  [TransactionId],
                CONVERT(VARCHAR(19), [TransactionDate], 126) AS [TransactionDate],
                [TransactionType],
                [ProductName],
                [ProductTypeId],
                [NoContracts],
                [ContractPrice],
                [TransactionFees]
        
        FROM [Klondike].[Transactions] AS T 
            -- INNER JOIN [Klondike].[ProductTypes] AS PT 
                -- ON PT.[ProductTypeID] = T.[ProductTypeID]
            WHERE (T.[ProductName] = @sProductName)

        ORDER BY [TransactionDate] ASC;
END
GO
