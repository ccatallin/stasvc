SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

-- Author:      Catalin Calugaroiu & Gemini Code Assist
-- Create date: 2025-10-17
-- Description: Updates a transaction. Snapshot recalculation is handled by the application layer.
CREATE OR ALTER  PROCEDURE [Klondike].[updateTransactionLog]

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
    -- The C# service layer now handles transactions and snapshot updates.
    -- This procedure is simplified to only perform the update.
    UPDATE [Klondike].[TransactionLogs]
    SET [Date] = @Date, [OperationId] = @OperationId, [ProductCategoryId] = @ProductCategoryId, [ProductId] = @ProductId,
        [ProductSymbol] = @ProductSymbol, [Quantity] = @Quantity, [Price] = @Price, [Fees] = @Fees, [Notes] = @Notes,
        [ModifiedById] = @ModifiedById, [Modified] = GETUTCDATE()
    WHERE [Id] = @Id AND [ClientId] = @ClientId;

    SET @UpdatedCount = @@ROWCOUNT;
END
GO
