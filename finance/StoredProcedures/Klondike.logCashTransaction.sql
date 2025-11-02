SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:      Catalin Calugaroiu & Gemini Code Assist
-- Create date: 2025-10-18
-- Description: Inserts a new cash transaction record.
-- =============================================
CREATE OR ALTER PROCEDURE [Klondike].[logCashTransaction]

@Date DATETIME,
@OperationId INT,
@CashCategoryId INT,
@Amount DECIMAL(18, 5),
@Notes NVARCHAR(MAX),
@CreatedById BIGINT,
@ClientId BIGINT,
@Id VARCHAR(100) OUTPUT,
@InsertedCount INT OUTPUT

AS
BEGIN
    SET NOCOUNT ON;
    SET @InsertedCount = 0;

    BEGIN TRY
        SET @Id = NEWID();

        INSERT INTO [Klondike].[CashTransactionLogs] 
            ([Id], [Date], [OperationId], [CashCategoryId], [Amount], [Notes], [CreatedById], [ClientId])
        VALUES 
            (@Id, @Date, @OperationId, @CashCategoryId, @Amount, @Notes, @CreatedById, @ClientId);

        SET @InsertedCount = @@ROWCOUNT;
    END TRY
    BEGIN CATCH
        -- Re-throw the error to the calling application
        THROW;
    END CATCH
END
GO