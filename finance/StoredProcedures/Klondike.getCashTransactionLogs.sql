SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:      Catalin Calugaroiu & Gemini Code Assist
-- Create date: 2025-10-18
-- Description: Retrieves cash transaction logs with optional filtering.
-- =============================================
CREATE OR ALTER PROCEDURE [Klondike].[getCashTransactionLogs]

@UserId BIGINT,
@ClientId BIGINT,
@StartDate DATETIME = NULL,
@EndDate DATETIME = NULL

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
      AND (@StartDate IS NULL OR ctl.[Date] >= @StartDate)
      AND (@EndDate IS NULL OR ctl.[Date] <= @EndDate)
      AND ctl.[IsDeleted] = 0
    ORDER BY ctl.[Date] DESC;
END
GO
