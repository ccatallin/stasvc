SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:      Catalin Calugaroiu & Gemini Code Assist
-- Create date: 2025-10-18
-- Description: Retrieves cash transaction logs with optional filtering.
-- =============================================
CREATE OR ALTER PROCEDURE [Klondike].[getCashTransactionLogById]

@Id AS VARCHAR(100),
@UserId AS BIGINT, -- Included for future use, though filtering is currently done by ClientId.
@ClientId AS BIGINT

AS
BEGIN
    SET NOCOUNT ON;

    SELECT
        ctl.[Id],
        ctl.[Date],
        ctl.[OperationId],
        ctl.[CashCategoryId],
        cc.[Name] AS CashCategoryName,
        ctl.[Amount],
        ctl.[Notes]
    FROM [Klondike].[CashTransactionLogs] AS ctl
    LEFT JOIN [Klondike].[CashCategories] AS cc ON ctl.CashCategoryId = cc.Id
    WHERE ctl.[ClientId] = @ClientId
      AND ctl.[CreatedById] = @UserId
      AND ctl.[Id] = @Id;
END
GO
