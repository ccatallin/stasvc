SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:      Catalin Calugaroiu & Gemini Code Assist
-- Create date: 2025-10-18
-- Description: Retrieves all cash transaction categories.
-- =============================================
CREATE OR ALTER PROCEDURE [Klondike].[getCashTransactionCategories]
AS
BEGIN
    SET NOCOUNT ON;

    SELECT [Id], [OperationId], [Name] FROM [Klondike].[CashCategories] ORDER BY [Id] ASC;
END
GO