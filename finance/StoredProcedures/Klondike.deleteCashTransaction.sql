SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:      Catalin Calugaroiu & Gemini Code Assist
-- Create date: 2025-10-18
-- Description: Marks a cash transaction record as deleted.
-- =============================================
CREATE OR ALTER PROCEDURE [Klondike].[deleteCashTransaction]

@Id VARCHAR(100),
@ModifiedById BIGINT,
@ClientId BIGINT,
@DeletedCount INT OUTPUT

AS
BEGIN
    SET NOCOUNT ON;
    SET @DeletedCount = 0;

    BEGIN TRY
        DELETE FROM [Klondike].[CashTransactionLogs] WITH (ROWLOCK)
            WHERE [Id] = @Id AND [ClientId] = @ClientId;

        SET @DeletedCount = @@ROWCOUNT;
    END TRY
    BEGIN CATCH
        -- Re-throw the error to the calling application
        THROW;
    END CATCH
END
GO