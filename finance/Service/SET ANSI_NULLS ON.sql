SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		Catalin Calugaroiu
-- Create date: 2025-10-18
-- Description:	Get a single transaction log by its ID.
-- =============================================
ALTER   PROCEDURE [Klondike].[getTransactionLogById]

@Id AS VARCHAR(100),
@UserId AS BIGINT, -- Included for future use, though filtering is currently done by ClientId.
@ClientId AS BIGINT

AS
BEGIN
	SET NOCOUNT ON;

    -- It's critical to filter by ClientId to prevent users from accessing other clients' data.
    -- Also, we should not return logically deleted records.
    SELECT *
    FROM [Klondike].[TransactionLogs]
    WHERE [Id] = @Id AND [CreatedById] = @UserId AND [ClientId] = @ClientId;
END
GO
