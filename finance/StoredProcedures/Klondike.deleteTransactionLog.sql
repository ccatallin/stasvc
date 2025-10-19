SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		Catalin Calugaroiu
-- Create date: 2024-04-15 22:50:06
-- Description:	Deletes a transaction and recalculates the relevant position snapshot.
-- =============================================
CREATE PROCEDURE [Klondike].[deleteTransactionLog]

@Id AS VARCHAR(100),
@ModifiedById AS BIGINT,
@ClientId AS BIGINT,

@DeletedCount AS INT OUTPUT

AS
BEGIN
    SET NOCOUNT ON;
    SET @DeletedCount = 0;

    DECLARE @ProductSymbol varchar(255);

    BEGIN TRY
        BEGIN TRANSACTION;

        -- First, get the product name before deleting for snapshot update
        SELECT @ProductSymbol = [ProductSymbol] 
        FROM [Klondike].[TransactionLogs] 
        WHERE [Id] = @Id AND [ClientId] = @ClientId;

        -- Perform the delete (or a soft delete, e.g., setting an IsDeleted flag)
        DELETE FROM [Klondike].[TransactionLogs]
        WHERE [Id] = @Id AND [ClientId] = @ClientId;

        SET @DeletedCount = @@ROWCOUNT;

        -- If the delete was successful and we found a product name, update snapshots
        IF @DeletedCount > 0 AND @ProductSymbol IS NOT NULL
        BEGIN
            EXEC [Klondike].[updatePositionSnapshots] @ModifiedById, @ClientId, @ProductSymbol;
        END

        COMMIT TRANSACTION;

    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        -- Re-throw the error to be caught by the application
        THROW;
    END CATCH
END
GO
