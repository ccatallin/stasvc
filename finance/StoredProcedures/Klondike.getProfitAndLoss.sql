SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		Catalin Calugaroiu
-- Create date: 2025-10-12 17:12:30 PM
-- Description:	Calculate the profit and loss per transactions
-- =============================================
CREATE PROCEDURE [Klondike].[getProfitAndLoss]

@UserId as bigint,
@ClientId bigint,
@StartDate as datetime = NULL,
@EndDate as datetime = NULL,
@realized as smallint = 1

AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
    SET NOCOUNT ON;

    IF (0 = @realized)
        WITH R AS (SELECT  T.[ProductName],
                           T.[ProductTypeID],
                           MIN(TransactionDate) AS FirstTransactionDate, 
                           MAX(TransactionDate) AS LastTransactionDate,
                           COUNT(TransactionID) AS NoTransactions,
                           (SUM(T.[TransactionType] * T.[NoContracts]) * -1) AS OpenContracts,
                           SUM((PT.[ProductContractSize] * [NoContracts] * [ContractPrice] * [TransactionType])) As Profit,
				           SUM(TransactionFees) * (-1) As Fees

                        FROM [Klondike].[Transactions] AS T 
                            INNER JOIN [Klondike].[ProductTypes] AS PT 
                                ON PT.[ProductTypeID] = T.[ProductTypeID]
                        
                            WHERE (T.[ClientID] = @ClientId) AND
                                  (@StartDate IS NULL OR T.[TransactionDate] >= @StartDate) AND
                                  (@EndDate IS NULL OR T.[TransactionDate] < @EndDate)
                        
                        GROUP BY [ProductName], T.[ProductTypeID])
            -- SELECT FROM R
            SELECT  ProductName, 
			        FirstTransactionDate, 
					LastTransactionDate, 
					OpenContracts, 
					Profit, 
					Fees, 
					(Profit + Fees) AS Total

                FROM R WHERE (0 <> OpenContracts)
                    ORDER BY LastTransactionDate ASC;

    ELSE
        -- realized (a trade was open with one or more transactions and closed with one or more transactions)
        -- the remaining open contracts = 0
        BEGIN
            PRINT '@StartDate IS NOT NULL';
            WITH R AS (SELECT  T.[ProductName],
                                    T.[ProductTypeID],
                                    MIN(TransactionDate) AS FirstTransactionDate, 
                                    MAX(TransactionDate) AS LastTransactionDate,
                                    COUNT(TransactionID) AS NoTransactions,
                                    (SUM(T.[TransactionType] * T.[NoContracts]) * -1) AS OpenContracts,
                                    SUM((PT.[ProductContractSize] * [NoContracts] * [ContractPrice] * [TransactionType])) As Profit,
                                    SUM(TransactionFees) * (-1) As Fees

                                    FROM [Klondike].[Transactions] AS T 
                                        INNER JOIN [Klondike].[ProductTypes] AS PT 
                                            ON PT.[ProductTypeID] = T.[ProductTypeID]

                                        WHERE (T.[ClientID] = @ClientId) AND
                                              (@StartDate IS NULL OR T.[TransactionDate] >= @StartDate) AND
                                              (@EndDate IS NULL OR T.[TransactionDate] < @EndDate)
                                    
                                    GROUP BY [ProductName], T.[ProductTypeID])
                -- SELECT FROM R
                SELECT  ProductName, 
                        ProductTypeID,
                        FirstTransactionDate, 
                        LastTransactionDate,
                        NoTransactions, 
                        OpenContracts, 
                        Profit, 
                        Fees, 
                        (Profit + Fees) AS Total

                    FROM R WHERE (0 = OpenContracts)
                        ORDER BY [ProductTypeID], LastTransactionDate ASC;
        END
END
GO
