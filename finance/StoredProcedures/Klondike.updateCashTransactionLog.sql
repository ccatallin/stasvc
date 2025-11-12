SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:      Catalin Calugaroiu & Gemini Code Assist
-- Create date: 2025-10-18
-- Description: Updates an existing cash transaction record.
-- =============================================

CREATE OR ALTER PROCEDURE [Klondike].[updateCashTransactionLog]
@Id VARCHAR(100),
@Date DATETIME,
@OperationId INT,
@CashCategoryId INT,
@Amount DECIMAL(18, 5),
@Notes NVARCHAR(MAX),
@ModifiedById BIGINT,
@ClientId BIGINT,
@UpdatedCount INT OUTPUT

AS
BEGIN
    SET NOCOUNT ON;
    SET @UpdatedCount = 0;

    BEGIN TRY
        UPDATE [Klondike].[CashTransactionLogs]
        SET [Date] = @Date,
            [OperationId] = @OperationId,
            [CashCategoryId] = @CashCategoryId,
            [Amount] = @Amount,
            [Notes] = @Notes,
            [ModifiedById] = @ModifiedById,
            [Modified] = GETUTCDATE()
        WHERE [Id] = @Id AND [ClientId] = @ClientId AND [IsDeleted] = 0;

        SET @UpdatedCount = @@ROWCOUNT;
    END TRY
    BEGIN CATCH
        -- Re-throw the error to the calling application
        THROW;
    END CATCH
END
GO