SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		Catalin Calugaroiu
-- Create date: 2024-05-22
-- Description:	Rebuilds all position snapshots by iterating through
--              every unique product for each client in the
--              TransactionLogs table. This is useful after bulk data
--              imports or for data correction purposes.
-- =============================================
CREATE PROCEDURE [Klondike].[rebuildAllPositionSnapshots]
AS
BEGIN
    SET NOCOUNT ON;

    -- Declare variables to hold the data from the cursor
    DECLARE @UserId BIGINT;
    DECLARE @ClientId BIGINT;
    DECLARE @ProductSymbol VARCHAR(255);

    -- Declare the cursor to iterate through all unique product/client combinations
    DECLARE snapshot_cursor CURSOR FOR
        SELECT DISTINCT
            [CreatedById],
            [ClientId],
            [ProductSymbol]
        FROM
            [Klondike].[TransactionLogs]
        ORDER BY
            [ClientId], [CreatedById], [ProductSymbol];

    BEGIN TRY
        OPEN snapshot_cursor;

        FETCH NEXT FROM snapshot_cursor INTO @UserId, @ClientId, @ProductSymbol;

        WHILE @@FETCH_STATUS = 0
        BEGIN
            -- Execute the update procedure for the current combination.
            EXEC [Klondike].[updatePositionSnapshots] @UserId, @ClientId, @ProductSymbol;

            FETCH NEXT FROM snapshot_cursor INTO @UserId, @ClientId, @ProductSymbol;
        END;

        CLOSE snapshot_cursor;
        DEALLOCATE snapshot_cursor;

    END TRY
    BEGIN CATCH
        -- If an error occurs, make sure to clean up the cursor
        IF CURSOR_STATUS('global', 'snapshot_cursor') >= 0
        BEGIN
            CLOSE snapshot_cursor;
            DEALLOCATE snapshot_cursor;
        END

        -- Re-throw the error to the calling application
        -- The THROW; statement is only supported in SQL Server 2012 and later.
        -- Using the classic RAISERROR for broader compatibility.
        DECLARE @ErrorMessage NVARCHAR(4000);
        DECLARE @ErrorSeverity INT;
        DECLARE @ErrorState INT;

        SELECT 
            @ErrorMessage = ERROR_MESSAGE(),
            @ErrorSeverity = ERROR_SEVERITY(),
            @ErrorState = ERROR_STATE();

        RAISERROR (@ErrorMessage, @ErrorSeverity, @ErrorState);
    END CATCH
END
GO
