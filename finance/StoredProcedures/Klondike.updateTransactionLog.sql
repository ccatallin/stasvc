SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

-- Creates or alters the updateTransactionLog stored procedure with validation logic.
CREATE   PROCEDURE [Klondike].[updateTransactionLog]
    @Id varchar(100),
    @Date datetime,
    @OperationId int,
    @ProductCategoryId int,
    @ProductId int,
    @ProductSymbol varchar(255),
    @Quantity int,
    @Price decimal(18, 5),
    @Fees decimal(18, 5),
    @Notes varchar(max),
    @ModifiedById bigint,
    @ClientId bigint,
    @UpdatedCount int OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    SET @UpdatedCount = 0;

    DECLARE @OldProductSymbol varchar(255);

    -- Step 1: Validate product consistency before updating.
    -- Check if another record (with a different Id) exists with the same product name
    -- but has a different Category or Type ID.
    IF EXISTS (
        SELECT 1
        FROM [Klondike].[TransactionLogs]
        WHERE [ClientId] = @ClientId
          AND [ProductSymbol] = @ProductSymbol
          AND [Id] <> @Id -- Exclude the current record from the check
          AND ([ProductCategoryId] <> @ProductCategoryId OR [ProductId] <> @ProductId)
          AND [IsDeleted] = 0
    )
    BEGIN
        -- A conflict exists. Set a specific error code and do not update.
        SET @UpdatedCount = -2;
        RETURN;
    END

    -- Step 2: If validation passes, proceed with the transaction.
    BEGIN TRY
        BEGIN TRANSACTION;

        -- Get the original product symbol before the update.
        SELECT @OldProductSymbol = [ProductSymbol]
        FROM [Klondike].[TransactionLogs]
        WHERE [Id] = @Id AND [ClientId] = @ClientId;

        UPDATE [Klondike].[TransactionLogs]
        SET [Date] = @Date, [OperationId] = @OperationId, [ProductCategoryId] = @ProductCategoryId, [ProductId] = @ProductId,
            [ProductSymbol] = @ProductSymbol, [Quantity] = @Quantity, [Price] = @Price, [Fees] = @Fees, [Notes] = @Notes,
            [ModifiedById] = @ModifiedById, [Modified] = GETUTCDATE()
        WHERE [Id] = @Id AND [ClientId] = @ClientId;

        SET @UpdatedCount = @@ROWCOUNT;

        IF @UpdatedCount > 0
        BEGIN
            -- Recalculate snapshots for the NEW product symbol.
            EXEC [Klondike].[updatePositionSnapshots] @ModifiedById, @ClientId, @ProductSymbol;

            -- If the product symbol was changed, we must also recalculate the OLD one.
            IF @OldProductSymbol IS NOT NULL AND @OldProductSymbol <> @ProductSymbol
            BEGIN
                EXEC [Klondike].[updatePositionSnapshots] @ModifiedById, @ClientId, @OldProductSymbol;
            END
        END

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;
        THROW;
    END CATCH
END
GO
