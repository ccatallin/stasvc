SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

-- Creates or alters the logTransaction stored procedure with validation logic.
CREATE   PROCEDURE [Klondike].[logTransaction]
    @Date datetime,
    @OperationId int, /* BUY/SELL */
    @ProductCategoryId int,
    @ProductId int,
    @ProductSymbol varchar(255),
    @Quantity int,
    @Price decimal(18, 5),
    @Fees decimal(18, 5),
    @Notes nvarchar(max), /* varchar(4096) in some versions */
    @CreatedById bigint,
    @ClientId bigint,
    @Mode smallint, /* 1 = normal, 0 = import */
    @Id varchar(100) OUTPUT,
    @InsertedCount int OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    SET @InsertedCount = 0; -- Default to 0 insertions

    -- Step 1: Validate product consistency.
    -- Check if a product with the same name already exists but with a different Category or Type ID.
    IF EXISTS (
        SELECT 1
        FROM [Klondike].[TransactionLogs]
        WHERE [ClientId] = @ClientId
          AND [ProductSymbol] = @ProductSymbol
          AND ([ProductCategoryId] <> @ProductCategoryId OR [ProductId] <> @ProductId)
          AND [IsDeleted] = 0 -- Only check against active records
    )
    BEGIN
        -- A conflict exists. Set a specific error code and do not insert.
        SET @InsertedCount = -2;
        RETURN;
    END

    -- Step 2: If validation passes, proceed with the transaction.
    BEGIN TRY
        BEGIN TRANSACTION;

        SELECT @Id = NEWID();
    
        INSERT INTO [Klondike].[TransactionLogs] ([Id], [Date], [OperationId], [ProductCategoryId], [ProductId], [ProductSymbol], [Quantity], [Price], [Fees], [Notes], [CreatedById], [ClientId])  
        VALUES (@Id, @Date, @OperationId, @ProductCategoryId, @ProductId, @ProductSymbol, @Quantity, @Price, @Fees, @Notes, @CreatedById, @ClientId);

        SET @InsertedCount = @@ROWCOUNT;

        IF ((@InsertedCount > 0) AND (@Mode = 1)) /* Normal mode */
        BEGIN
            EXEC [Klondike].[updatePositionSnapshots] @CreatedById, @ClientId, @ProductSymbol;
        END
        
        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;
        THROW; -- Re-throw the original error to the client
    END CATCH
END
GO
