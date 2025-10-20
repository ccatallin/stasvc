SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		Catalin Calugaroiu & Gemini
-- Create date: 2025-10-14
-- Description:	Gets the set of transactions that constitute the current
--              open position for a given product and client. It returns
--              only the transactions since the position was last opened.
-- =============================================
CREATE PROCEDURE [Klondike].[getOpenPositionTransactionLogs]

@ProductSymbol AS VARCHAR(255),
@UserId AS BIGINT,
@ClientId AS BIGINT

AS
BEGIN
    SET NOCOUNT ON;

    -- This procedure identifies the "lot" of transactions that make up the current
    -- open position. A new lot begins when a position is opened from zero or
    -- flips sign (e.g., from long to short).

    WITH OrderedTrades AS (
        -- Step 1: Get all transactions for the product and assign a signed quantity.
        SELECT
            *,
            IIF([OperationId] = -1, [Quantity], -[Quantity]) AS SignedQuantity -- BUY is positive, SELL is negative
        FROM Klondike.TransactionLogs
        WHERE
            ProductSymbol = @ProductSymbol
            AND (ClientId = @ClientId OR @ClientId = -1001) -- Handle special admin case
    ),
    RunningTotals AS (
        -- Step 2: Calculate the running quantity *before* the current trade.
        SELECT
            *,
            ISNULL(SUM(SignedQuantity) OVER (
                PARTITION BY ProductSymbol, ClientId -- Partition by ClientId for the admin case
                ORDER BY [Date], Id
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ), 0) AS PreviousRunningQuantity
        FROM OrderedTrades
    ),
    LotIdentifier AS (
        -- Step 3: Identify the start of a new lot.
        SELECT
            *,
            CASE
                WHEN PreviousRunningQuantity = 0 THEN 1 -- A new lot starts when the position was zero.
                WHEN SIGN(PreviousRunningQuantity) * SIGN(PreviousRunningQuantity + SignedQuantity) = -1 THEN 1 -- Or when it flips sign.
                ELSE 0
            END AS IsNewLot
        FROM RunningTotals
    ),
    LotGroups AS (
        -- Step 4: Assign a unique, incrementing ID to each lot.
        SELECT *, SUM(IsNewLot) OVER (PARTITION BY ProductSymbol, ClientId ORDER BY [Date], Id) AS LotGroupID
        FROM LotIdentifier
    )
    -- Step 5: Select all transactions from the most recent lot.
    SELECT [Id], CONVERT(VARCHAR(19), [Date], 126) AS [Date], [OperationId], [ProductCategoryId], [ProductId], [ProductSymbol], [Quantity], [Price], [Fees]
    FROM LotGroups
    WHERE LotGroupID = (SELECT MAX(LotGroupID) FROM LotGroups)
    ORDER BY [Date] ASC, [Id] ASC;

END
GO
