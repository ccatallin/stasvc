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
        [Id],
        [Date],
        [OperationId],
        [CashCategoryId],
        [Amount],
        [Notes]
    FROM [Klondike].[CashTransactionLogs]
    WHERE [ClientId] = @ClientId
      AND [CreatedById] = @UserId
      AND (@StartDate IS NULL OR [Date] >= @StartDate)
      AND (@EndDate IS NULL OR [Date] <= @EndDate)
      AND [IsDeleted] = 0
    ORDER BY [Date] DESC;
END
GO
