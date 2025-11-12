SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:      Catalin Calugaroiu & Gemini Code Assist
-- Create date: 2025-10-19
-- Description: Creates a daily snapshot of all cash balances.
--              This procedure is designed to be run once daily via a scheduled job.
--              It uses a MERGE statement to be idempotent, meaning if it's run
--              multiple times for the same day, it will update the existing snapshot
--              rather than creating a duplicate.
-- =============================================
CREATE OR ALTER PROCEDURE [Klondike].[createDailyCashBalanceSnapshots]

@SnapshotDate DATE = NULL

AS
BEGIN
    SET NOCOUNT ON;

    -- Default to the current UTC date if no date is provided.
    DECLARE @TargetDate DATE = ISNULL(@SnapshotDate, CAST(GETUTCDATE() AS DATE));

    MERGE [Klondike].[CashBalanceSnapshots] AS Target
    USING [Klondike].[CashBalances] AS Source
    ON (Target.ClientId = Source.ClientId 
        AND Target.UserId = Source.CreatedById -- Note: UserId in snapshot maps to CreatedById in live balance
        AND Target.Currency = Source.Currency 
        AND Target.SnapshotDate = @TargetDate)
    WHEN MATCHED THEN
        -- If a snapshot for today already exists, update it with the latest balance.
        UPDATE SET Target.Balance = Source.Balance
    WHEN NOT MATCHED BY TARGET THEN
        -- If no snapshot exists for today, insert a new one.
        INSERT (UserId, ClientId, Balance, Currency, SnapshotDate)
        VALUES (Source.CreatedById, Source.ClientId, Source.Balance, Source.Currency, @TargetDate);
END
GO
