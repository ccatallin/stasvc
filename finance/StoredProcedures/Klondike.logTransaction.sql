SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

-- Author:      Catalin Calugaroiu & Gemini Code Assist
-- Create date: 2025-10-17
-- Description: Inserts a transaction. Snapshot recalculation is handled by the application layer.
CREATE OR ALTER  PROCEDURE [Klondike].[logTransaction]

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
    -- The C# service layer now handles transactions and snapshot updates.
    -- This procedure is simplified to only perform the insert.

    SELECT @Id = NEWID();

    INSERT INTO [Klondike].[TransactionLogs] ([Id], [Date], [OperationId], [ProductCategoryId], [ProductId], [ProductSymbol], [Quantity], [Price], [Fees], [Notes], [CreatedById], [ClientId])  
    VALUES (@Id, @Date, @OperationId, @ProductCategoryId, @ProductId, @ProductSymbol, @Quantity, @Price, @Fees, @Notes, @CreatedById, @ClientId);

    SET @InsertedCount = @@ROWCOUNT;
END
GO
