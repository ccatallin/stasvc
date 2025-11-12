SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:      Catalin Calugaroiu & Gemini Code Assist
-- Create date: 2025-10-18
-- Description: Updates the cash balance for a specific client and currency.
--              It uses a MERGE statement to perform an "UPSERT" operation:
--              - If a balance exists for the ClientId/Currency, it's updated.
--              - If not, a new balance record is created.
-- =============================================
CREATE OR ALTER PROCEDURE [Klondike].[updateCashBalance]

@UserId AS BIGINT,
@ClientId AS BIGINT,
@AmountChange AS DECIMAL(24, 5),
@Currency AS VARCHAR(3) = 'USD', -- Default to USD
@NewBalance AS DECIMAL(24, 5) OUTPUT

AS
BEGIN
    SET NOCOUNT ON;

    MERGE [Klondike].[CashBalances] AS target
    USING (SELECT @ClientId AS ClientId, @UserId AS UserId, @Currency AS Currency) AS source
    ON (target.ClientId = source.ClientId AND target.CreatedById = source.UserId AND target.Currency = source.Currency)
    WHEN MATCHED THEN
        UPDATE SET
            target.Balance = target.Balance + @AmountChange,
            target.ModifiedById = @UserId,
            target.Modified = GETUTCDATE()
    WHEN NOT MATCHED THEN
        INSERT (ClientId, Balance, Currency, CreatedById, Created, ModifiedById, Modified)
        VALUES (@ClientId, @AmountChange, @Currency, @UserId, GETUTCDATE(), @UserId, GETUTCDATE()); -- CreatedById is the user

    -- Select the new balance into the output parameter
    SELECT @NewBalance = Balance
    FROM [Klondike].[CashBalances]
    WHERE ClientId = @ClientId AND CreatedById = @UserId AND Currency = @Currency;
END
GO