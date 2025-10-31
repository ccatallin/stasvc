SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		Catalin Calugaroiu & Gemini Code Assist
-- Create date: 2025-10-17
-- Description:	Deletes a transaction. Snapshot recalculation is handled by the application layer.
-- =============================================
CREATE OR ALTER PROCEDURE [Klondike].[deleteTransactionLogNew]

@Id AS VARCHAR(100),
@ModifiedById AS BIGINT,
@ClientId AS BIGINT,

@DeletedCount AS INT OUTPUT

AS
BEGIN
    SET NOCOUNT ON;
    SET @DeletedCount = 0;

    -- The C# service layer now handles transactions and snapshot updates.
    -- This procedure is simplified to only perform the delete.
    DELETE FROM [Klondike].[TransactionLogs]
    WHERE [Id] = @Id AND [ClientId] = @ClientId;

    SET @DeletedCount = @@ROWCOUNT;
END
GO
