SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:      Catalin Calugaroiu & Gemini Code Assist
-- Create date: 2025-10-18
-- Description: Retrieves the cash balance for a specific client.
-- =============================================
CREATE OR ALTER PROCEDURE [Klondike].[getCashBalance]

@UserId AS BIGINT,
@ClientId AS BIGINT

AS
BEGIN
    SET NOCOUNT ON;

    SELECT [Balance] AS [CashBalance], [Currency]
    FROM [Klondike].[CashBalances]
    WHERE ([ClientId] = @ClientId) AND ([CreatedById] = @UserId);
END
GO
