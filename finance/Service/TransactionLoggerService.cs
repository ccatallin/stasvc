using System;
using System.Text;
using System.Threading.Tasks;
// --
using Microsoft.Data.SqlClient;
using Dapper;
// --
using System.Linq;
using System.Collections.Generic;
using System.Data;
using Newtonsoft.Json;
using FalxGroup.Finance.Model;
using FalxGroup.Finance.BusinessLogic;

namespace FalxGroup.Finance.Service
{
    // A dedicated view model for open positions improves type safety and maintainability.
    public class OpenPositionViewModel
    {
        public int ProductCategoryId { get; set; }
        public int ProductId { get; set; }
        public string ProductSymbol { get; set; }
        public decimal Quantity { get; set; }
        public decimal AveragePrice { get; set; }
        public decimal Cost { get; set; }
        public decimal Commission { get; set; }
        public decimal TotalCost => Cost + Commission;
    }


public class TransactionLoggerService
{
    private readonly PositionSnapshotCalculator _snapshotCalculator;
    public TransactionLoggerService(string connectionString)
    {
        this.ConnectionString = connectionString;
        this._snapshotCalculator = new PositionSnapshotCalculator();
    }

    public async Task<Tuple<int, string>> LogTransaction(SecurityTransactionLog record, string mode)
    {
        if (record == null || record.IsEmpty)
        {
            return new Tuple<int, string>(-1, null);
        }

        using var connection = new SqlConnection(this.ConnectionString);
        await connection.OpenAsync();
        await using var transaction = await connection.BeginTransactionAsync();

        try
        {
            var sqlQuery = "EXEC [Klondike].[logTransactionNew] @Date, @OperationId, @ProductCategoryId, @ProductId, @ProductSymbol, @Quantity, @Price, @Fees, @Notes, @CreatedById, @ClientId, @Mode, @Id OUTPUT, @InsertedCount OUTPUT";

            using var command = new SqlCommand(sqlQuery, connection, (SqlTransaction)transaction);

            command.Parameters.AddWithValue("@Date", record.Date);
            command.Parameters.AddWithValue("@OperationId", record.OperationId);
            command.Parameters.AddWithValue("@ProductCategoryId", record.ProductCategoryId);
            command.Parameters.AddWithValue("@ProductId", record.ProductId);
            command.Parameters.AddWithValue("@ProductSymbol", record.ProductSymbol.Trim());
            command.Parameters.AddWithValue("@Quantity", record.Quantity);
            command.Parameters.AddWithValue("@Price", record.Price);
            command.Parameters.AddWithValue("@Fees", record.Fees);
            command.Parameters.AddWithValue("@Notes", record.Notes?.Trim());
            command.Parameters.AddWithValue("@CreatedById", record.UserId);
            command.Parameters.AddWithValue("@ClientId", record.ClientId);
            command.Parameters.AddWithValue("@Mode", mode.Equals("import") ? 0 : 1);

            command.Parameters.Add("@Id", System.Data.SqlDbType.VarChar, 100).Direction = System.Data.ParameterDirection.Output;
            var insertedCountParameter = command.Parameters.Add("@InsertedCount", System.Data.SqlDbType.Int);
            insertedCountParameter.Direction = System.Data.ParameterDirection.Output;

            await command.ExecuteNonQueryAsync();

            var id = Convert.ToString(command.Parameters["@Id"].Value);
            var result = (int)insertedCountParameter.Value;

            if (result == 1)
            {
                await UpdateSnapshotsForProductAsync(connection, (SqlTransaction)transaction, record.UserId, record.ClientId, record.ProductSymbol);
            }

            await transaction.CommitAsync();
            return new Tuple<int, string>(result, result == 1 ? id : null);
        }
        catch
        {
            await transaction.RollbackAsync();
            throw;
        }
    }

    public async Task<Tuple<int, string>> UpdateTransactionLog(SecurityTransactionLog record)
    {
        if (record == null || record.IsEmpty || string.IsNullOrWhiteSpace(record.Id))
        {
            return new Tuple<int, string>(-1, null);
        }

        using var connection = new SqlConnection(this.ConnectionString);
        await connection.OpenAsync();
        await using var transaction = await connection.BeginTransactionAsync();

        try
        {
            // First, get the original product symbol in case it changes.
            var getOldSymbolCmd = new SqlCommand("SELECT [ProductSymbol] FROM [Klondike].[TransactionLogs] WHERE [Id] = @Id AND [CreatedById] = @UserId  AND [ClientId] = @ClientId", connection, (SqlTransaction)transaction);
            getOldSymbolCmd.Parameters.AddWithValue("@Id", record.Id);
            getOldSymbolCmd.Parameters.AddWithValue("@UserId", record.UserId); // The parameter name is fine, the query text was wrong.
            getOldSymbolCmd.Parameters.AddWithValue("@ClientId", record.ClientId);
            var oldProductSymbol = await getOldSymbolCmd.ExecuteScalarAsync() as string;

            // Now, execute the update.
            var sqlQuery = "EXEC [Klondike].[updateTransactionLogNew] @Id, @Date, @OperationId, @ProductCategoryId, @ProductId, @ProductSymbol, @Quantity, @Price, @Fees, @Notes, @ModifiedById, @ClientId, @UpdatedCount OUTPUT";
            using var command = new SqlCommand(sqlQuery, connection, (SqlTransaction)transaction);

            command.Parameters.AddWithValue("@Id", record.Id);
            command.Parameters.AddWithValue("@Date", record.Date);
            command.Parameters.AddWithValue("@OperationId", record.OperationId);
            command.Parameters.AddWithValue("@ProductCategoryId", record.ProductCategoryId);
            command.Parameters.AddWithValue("@ProductId", record.ProductId);
            command.Parameters.AddWithValue("@ProductSymbol", record.ProductSymbol.Trim());
            command.Parameters.AddWithValue("@Quantity", record.Quantity);
            command.Parameters.AddWithValue("@Price", record.Price);
            command.Parameters.AddWithValue("@Fees", record.Fees);
            command.Parameters.AddWithValue("@Notes", record.Notes?.Trim());
            command.Parameters.AddWithValue("@ModifiedById", record.UserId);
            command.Parameters.AddWithValue("@ClientId", record.ClientId);

            var updatedCountParameter = command.Parameters.Add("UpdatedCount", System.Data.SqlDbType.Int);
            updatedCountParameter.Direction = System.Data.ParameterDirection.Output;

            await command.ExecuteNonQueryAsync();
            var result = (int)updatedCountParameter.Value;

            if (result == 1)
            {
                // Recalculate for the new/current product symbol.
                await UpdateSnapshotsForProductAsync(connection, (SqlTransaction)transaction, record.UserId, record.ClientId, record.ProductSymbol);

                // If the symbol changed, we must also recalculate the old one.
                if (!string.IsNullOrEmpty(oldProductSymbol) && oldProductSymbol != record.ProductSymbol)
                {
                    await UpdateSnapshotsForProductAsync(connection, (SqlTransaction)transaction, record.UserId, record.ClientId, oldProductSymbol);
                }
            }

            await transaction.CommitAsync();
            return new Tuple<int, string>(result, result == 1 ? record.Id : null);
        }
        catch
        {
            await transaction.RollbackAsync();
            throw;
        }
    }

    public async Task<Tuple<int, string>> DeleteTransactionLog(SecurityTransactionLog record)
    {
        if (string.IsNullOrWhiteSpace(record?.Id))
        {
            return new Tuple<int, string>(-1, null);
        }

        using var connection = new SqlConnection(this.ConnectionString);
        await connection.OpenAsync();
        await using var transaction = await connection.BeginTransactionAsync();

        try
        {
            // Get the product symbol before deleting.
            var getSymbolCmd = new SqlCommand("SELECT [ProductSymbol] FROM [Klondike].[TransactionLogs] WHERE [Id] = @Id AND [CreatedById] = @UserId AND [ClientId] = @ClientId", connection, (SqlTransaction)transaction);
            getSymbolCmd.Parameters.AddWithValue("@Id", record.Id);
            getSymbolCmd.Parameters.AddWithValue("@UserId", record.UserId); // The parameter name is fine, the query text was wrong.
            getSymbolCmd.Parameters.AddWithValue("@ClientId", record.ClientId);
            var productSymbol = await getSymbolCmd.ExecuteScalarAsync() as string;

            // Execute the delete.
            var sqlQuery = "EXEC [Klondike].[deleteTransactionLogNew] @Id, @ModifiedById, @ClientId, @DeletedCount OUTPUT";
            using var command = new SqlCommand(sqlQuery, connection, (SqlTransaction)transaction);
            command.Parameters.AddWithValue("@Id", record.Id);
            command.Parameters.AddWithValue("@ModifiedById", record.UserId); // user id
            command.Parameters.AddWithValue("@ClientId", record.ClientId); // client id

            var deletedCountParameter = command.Parameters.Add("@DeletedCount", System.Data.SqlDbType.Int);
            deletedCountParameter.Direction = System.Data.ParameterDirection.Output;

            await command.ExecuteNonQueryAsync();

            var result = (int)deletedCountParameter.Value;

            if (result > 0 && !string.IsNullOrEmpty(productSymbol))
            {
                await UpdateSnapshotsForProductAsync(connection, (SqlTransaction)transaction, record.UserId, record.ClientId, productSymbol);
            }

            await transaction.CommitAsync();
            return new Tuple<int, string>(result, record.Id);
        }
        catch
        {
            await transaction.RollbackAsync();
            throw;
        }
    }

    public async Task<Tuple<int, string>> LogCashTransaction(CashTransactionLog record)
    {
        if (record == null)
        {
            return new Tuple<int, string>(-1, null);
        }

        using var connection = new SqlConnection(this.ConnectionString);
        await connection.OpenAsync();

        var sqlQuery = "EXEC [Klondike].[logCashTransaction] @Date, @OperationId, @CashCategoryId, @Amount, @Notes, @CreatedById, @ClientId, @Id OUTPUT, @InsertedCount OUTPUT";

        using var command = new SqlCommand(sqlQuery, connection);

        command.Parameters.AddWithValue("@Date", record.Date);
        command.Parameters.AddWithValue("@OperationId", record.OperationId);
        command.Parameters.AddWithValue("@CashCategoryId", record.CashCategoryId);
        command.Parameters.AddWithValue("@Amount", record.Amount);
        command.Parameters.AddWithValue("@Notes", (object)record.Notes?.Trim() ?? DBNull.Value);
        command.Parameters.AddWithValue("@CreatedById", record.UserId);
        command.Parameters.AddWithValue("@ClientId", record.ClientId);

        command.Parameters.Add("@Id", System.Data.SqlDbType.VarChar, 100).Direction = System.Data.ParameterDirection.Output;
        var insertedCountParameter = command.Parameters.Add("@InsertedCount", System.Data.SqlDbType.Int);
        insertedCountParameter.Direction = System.Data.ParameterDirection.Output;

        await command.ExecuteNonQueryAsync();

        var id = Convert.ToString(command.Parameters["@Id"].Value);
        var result = (int)insertedCountParameter.Value;

        return new Tuple<int, string>(result, result == 1 ? id : null);
    }

    public async Task<Tuple<int, string>> UpdateCashTransaction(CashTransactionLog record)
    {
        if (record == null || string.IsNullOrWhiteSpace(record.Id))
        {
            return new Tuple<int, string>(-1, null);
        }

        using var connection = new SqlConnection(this.ConnectionString);
        await connection.OpenAsync();

        var sqlQuery = "EXEC [Klondike].[updateCashTransaction] @Id, @Date, @OperationId, @CashCategoryId, @Amount, @Notes, @ModifiedById, @ClientId, @UpdatedCount OUTPUT";
        using var command = new SqlCommand(sqlQuery, connection);

        command.Parameters.AddWithValue("@Id", record.Id);
        command.Parameters.AddWithValue("@Date", record.Date);
        command.Parameters.AddWithValue("@OperationId", record.OperationId);
        command.Parameters.AddWithValue("@CashCategoryId", record.CashCategoryId);
        command.Parameters.AddWithValue("@Amount", record.Amount);
        command.Parameters.AddWithValue("@Notes", (object)record.Notes?.Trim() ?? DBNull.Value);
        command.Parameters.AddWithValue("@ModifiedById", record.UserId);
        command.Parameters.AddWithValue("@ClientId", record.ClientId);

        var updatedCountParameter = command.Parameters.Add("@UpdatedCount", System.Data.SqlDbType.Int);
        updatedCountParameter.Direction = System.Data.ParameterDirection.Output;

        await command.ExecuteNonQueryAsync();
        var result = (int)updatedCountParameter.Value;

        return new Tuple<int, string>(result, result == 1 ? record.Id : null);
    }

    public async Task<Tuple<int, string>> DeleteCashTransaction(CashTransactionLog record)
    {
        if (string.IsNullOrWhiteSpace(record?.Id))
        {
            return new Tuple<int, string>(-1, null);
        }

        using var connection = new SqlConnection(this.ConnectionString);
        await connection.OpenAsync();

        try
        {
            var sqlQuery = "EXEC [Klondike].[deleteCashTransaction] @Id, @ModifiedById, @ClientId, @DeletedCount OUTPUT";
            using var command = new SqlCommand(sqlQuery, connection);
            command.Parameters.AddWithValue("@Id", record.Id);
            command.Parameters.AddWithValue("@ModifiedById", record.UserId);
            command.Parameters.AddWithValue("@ClientId", record.ClientId);

            var deletedCountParameter = command.Parameters.Add("@DeletedCount", System.Data.SqlDbType.Int);
            deletedCountParameter.Direction = System.Data.ParameterDirection.Output;

            await command.ExecuteNonQueryAsync();

            var result = (int)deletedCountParameter.Value;

            return new Tuple<int, string>(result, record.Id);
        }
        catch
        {
            throw;
        }
    }

    public async Task<string> GetCashTransactionLogs(CashTransactionLog record)
    {
        using SqlConnection connection = new SqlConnection(this.ConnectionString);
        await connection.OpenAsync();

        var sqlQuery = "EXEC [Klondike].[getCashTransactionLogs] @UserId, @ClientId, @StartDate, @EndDate";

        using SqlCommand command = new SqlCommand(sqlQuery, connection);
        command.Parameters.AddWithValue("@UserId", record.UserId);
        command.Parameters.AddWithValue("@ClientId", record.ClientId);
        command.Parameters.AddWithValue("@StartDate", record.StartDate.HasValue ? record.StartDate.Value : DBNull.Value);
        command.Parameters.AddWithValue("@EndDate", record.EndDate.HasValue ? record.EndDate.Value : DBNull.Value);

        return await ReadToJsonAsync(command);
    }

    public async Task<string> GetOpenPositions(SecurityTransactionLog record)
    {
        const string sqlQuery = @"
            WITH LatestSnapshots AS (
                SELECT  ps.[ProductCategoryId],
                        ps.[ProductId],
                        ps.[ProductSymbol],
                        ps.[Quantity],
                        ps.[Cost],
                        ps.[Commission],
                        ps.[AveragePrice],
                        ROW_NUMBER() OVER(PARTITION BY ps.ProductSymbol ORDER BY ps.SnapshotDate DESC) as rn
                    FROM [Klondike].[PositionSnapshots] AS ps WITH (NOLOCK)
                    WHERE (ps.[ClientId] = @ClientId)
            )
            SELECT  [ProductCategoryId],
                    [ProductId],
                    [ProductSymbol],
                    [Quantity],
                    [AveragePrice],
                    [Cost],
                    [Commission],
                    ([Cost] + [Commission]) AS TotalCost
                FROM LatestSnapshots
                    WHERE rn = 1 AND [Quantity] <> 0;";

        using var connection = new SqlConnection(this.ConnectionString);
        var openPositions = (await connection.QueryAsync<OpenPositionViewModel>(sqlQuery, new { record.ClientId })).ToList();

        // Apply presentation-layer transformations after fetching the data.
        foreach (var position in openPositions)
        {
            // Apply special formatting for ZB Bond price.
            if (position.ProductCategoryId == 3 && position.ProductId == 5)
            {
                position.AveragePrice = FormatZbBondPrice(position.AveragePrice);
            }
        }

        // The final JSON will have integer quantities and rounded prices.
        var result = openPositions.Select(p => new {
            p.ProductCategoryId,
            p.ProductId,
            p.ProductSymbol,
            Quantity = (int)p.Quantity,
            AveragePrice = Math.Round(p.AveragePrice, 2),
            p.Cost,
            p.Commission,
            p.TotalCost
        });

        return JsonConvert.SerializeObject(result);
    }
    
    public async Task<string> GetTransactionLogById(SecurityTransactionLog record)
    {
        using SqlConnection connection = new SqlConnection(this.ConnectionString);
        await connection.OpenAsync();

        var sqlQuery = "EXEC [Klondike].[getTransactionLogById] @Id, @UserId, @ClientId";

        using SqlCommand command = new SqlCommand(sqlQuery, connection);
        command.Parameters.AddWithValue("@Id", record.Id);
        command.Parameters.AddWithValue("@UserId", record.UserId);
        command.Parameters.AddWithValue("@ClientId", record.ClientId);

        return await ReadToJsonAsync(command);
    }

    public async Task<string> GetProductTransactionLogs(SecurityTransactionLog record)
    {
        // Logic moved from [Klondike].[getProductTransactionLogs] stored procedure.
        var sqlBuilder = new StringBuilder(@"
            SELECT [Id], 
                   [Date], 
                   [OperationId], 
                   [ProductCategoryId], 
                   [ProductId], 
                   [ProductSymbol], 
                   [Quantity], +                   [Price], 
                   [Fees]
            FROM [Klondike].[TransactionLogs]
            WHERE [ProductSymbol] = @ProductSymbol");

        var parameters = new DynamicParameters();
        parameters.Add("ProductSymbol", record.ProductSymbol);

        // The SP had a special case for ClientId = -1001 to show all clients.
        if (record.ClientId != -1001)
        {
            sqlBuilder.Append(" AND [ClientID] = @ClientId");
            parameters.Add("ClientId", record.ClientId);
        }

        sqlBuilder.Append(record.ClientId == -1001 ? " ORDER BY [Date] ASC;" : " ORDER BY [Date] DESC;");

        using var connection = new SqlConnection(this.ConnectionString);
        var logs = await connection.QueryAsync(sqlBuilder.ToString(), parameters);
        return JsonConvert.SerializeObject(logs);
    }
    
    public async Task<string> GetOpenPositionTransactionLogs(SecurityTransactionLog record)
    {
        // Logic moved from [Klondike].[getOpenPositionTransactionLogs] stored procedure.
        // This complex, stateful logic is much clearer and more testable in C#.

        // 1. Fetch all transactions for the product, ordered chronologically.
        var sqlBuilder = new StringBuilder(@"
            SELECT [Id], [Date], [OperationId], [ProductCategoryId], [ProductId], [ProductSymbol], [Quantity], [Price], [Fees]
            FROM [Klondike].[TransactionLogs]
            WHERE [ProductSymbol] = @ProductSymbol");

        var parameters = new DynamicParameters();
        parameters.Add("ProductSymbol", record.ProductSymbol);

        if (record.ClientId != -1001)
        {
            sqlBuilder.Append(" AND [ClientID] = @ClientId");
            parameters.Add("ClientId", record.ClientId);
        }

        sqlBuilder.Append(" ORDER BY [Date] ASC, [Id] ASC;");

        using var connection = new SqlConnection(this.ConnectionString);
        var allTransactions = (await connection.QueryAsync<SecurityTransactionLog>(sqlBuilder.ToString(), parameters)).ToList();

        if (!allTransactions.Any())
        {
            return "[]";
        }

        // 2. Group transactions into lots and return the last one.
        var lots = GroupTransactionsIntoLots(allTransactions);
        var lastLot = lots.LastOrDefault() ?? new List<SecurityTransactionLog>();

        return JsonConvert.SerializeObject(lastLot);
    }

    public async Task<string> GetRealizedProfitAndLoss(SecurityTransactionLog record)
    {
        // Logic moved from [Klondike].[getProfitAndLoss] stored procedure.
        // This centralizes complex P&L and lot identification logic in C#.

        // 1. Fetch all transactions matching the filter criteria.
        var sqlBuilder = new StringBuilder(@"
            SELECT tl.*,
                   ISNULL(p.ContractMultiplier, 1) AS ContractMultiplier,
                   ISNULL(pc.Multiplier, 1) AS CategoryMultiplier
            FROM [Klondike].[TransactionLogs] AS tl
            LEFT JOIN [Klondike].[ProductCategories] AS pc ON tl.ProductCategoryId = pc.Id
            LEFT JOIN [Klondike].[Products] AS p ON tl.ProductId = p.Id AND tl.ProductCategoryId = p.ProductCategoryId
            WHERE tl.ClientId = @ClientId AND tl.IsDeleted = 0");

        var parameters = new DynamicParameters();
        parameters.Add("ClientId", record.ClientId);

        if (record.ProductCategoryId > 0) { sqlBuilder.Append(" AND tl.ProductCategoryId = @ProductCategoryId"); parameters.Add("ProductCategoryId", record.ProductCategoryId); }
        if (record.ProductId > 0) { sqlBuilder.Append(" AND tl.ProductId = @ProductId"); parameters.Add("ProductId", record.ProductId); }
        if (!string.IsNullOrEmpty(record.ProductSymbol)) { sqlBuilder.Append(" AND tl.ProductSymbol = @ProductSymbol"); parameters.Add("ProductSymbol", record.ProductSymbol); }
        if (record.StartDate.HasValue) { sqlBuilder.Append(" AND tl.Date >= @StartDate"); parameters.Add("StartDate", record.StartDate.Value); }
        if (record.EndDate.HasValue) { sqlBuilder.Append(" AND tl.Date < @EndDate"); parameters.Add("EndDate", record.EndDate.Value); }

        sqlBuilder.Append(" ORDER BY tl.ProductSymbol, tl.Date ASC, tl.Id ASC;");

        using var connection = new SqlConnection(this.ConnectionString);
        var allTransactions = (await connection.QueryAsync<SecurityTransactionLog>(sqlBuilder.ToString(), parameters)).ToList();

        if (!allTransactions.Any())
        {
            return "[]";
        }

        // 2. Group all transactions by product symbol, then process each group.
        var results = new List<object>();
        var transactionsBySymbol = allTransactions.GroupBy(t => t.ProductSymbol);

        foreach (var group in transactionsBySymbol)
        {
            var lots = GroupTransactionsIntoLots(group.ToList());

            // 3. Summarize each lot.
            var lotSummaries = lots.Select(lot =>
            {
                decimal netQuantity = 0;
                decimal realizedPL = 0;
                decimal totalFees = 0;

                foreach (var t in lot)
                {
                    decimal signedQuantity = t.OperationId == -1 ? t.Quantity : -t.Quantity;
                    netQuantity += signedQuantity;
                    totalFees += t.Fees;

                    // GrossValue calculation, including ZB bond special logic.
                    decimal grossValue;
                    if (t.ProductCategoryId == 3 && t.ProductId == 5)
                    {
                        // Convert price like 117.18 to full dollar value
                        grossValue = t.Quantity * ((Math.Floor(t.Price) + (t.Price - Math.Floor(t.Price)) * 100m / 32m) * 1000m);
                    }
                    else
                    {
                        grossValue = t.Quantity * t.Price * t.ContractMultiplier * t.CategoryMultiplier;
                    }

                    realizedPL += (t.OperationId == 1 ? grossValue : -grossValue); // SELL is positive P/L, BUY is negative
                }

                return new
                {
                    ProductSymbol = lot.First().ProductSymbol,
                    ProductCategoryId = lot.First().ProductCategoryId,
                    ProductId = lot.First().ProductId,
                    FirstTransactionDate = lot.First().Date,
                    LastTransactionDate = lot.Last().Date,
                    NetQuantity = netQuantity,
                    Profit = realizedPL,
                    Fees = totalFees,
                    Total = realizedPL - totalFees,
                    IsClosed = netQuantity == 0
                };
            });

            // 4. Filter for realized (isClosed) or unrealized (most recent open lot).
            // Assuming @realized = 1 for this method.
            var realizedLots = lotSummaries.Where(s => s.IsClosed);
            results.AddRange(realizedLots);
        }

        return JsonConvert.SerializeObject(results);
    }

    public async Task<string> GetTransactionLogs(SecurityTransactionLog record)
    {
        using SqlConnection connection = new SqlConnection(this.ConnectionString);
        await connection.OpenAsync();

        var sqlQuery = "EXEC [Klondike].[getTransactionLogs] @UserId, @ClientId, @ProductCategoryId, @ProductId, @ProductSymbol, @StartDate, @EndDate";

        using SqlCommand command = new SqlCommand(sqlQuery, connection);
        command.Parameters.AddWithValue("@UserId", record.UserId);
        command.Parameters.AddWithValue("@ClientId", record.ClientId);
        command.Parameters.AddWithValue("@ProductCategoryId", (0 != record.ProductCategoryId) ? record.ProductCategoryId : DBNull.Value);
        command.Parameters.AddWithValue("@ProductId", (0 != record.ProductId) ? record.ProductId : DBNull.Value);
        command.Parameters.AddWithValue("@ProductSymbol", (0 != record.ProductSymbol.Length) ? record.ProductSymbol : DBNull.Value);
        command.Parameters.AddWithValue("@StartDate", record.StartDate.HasValue ? record.StartDate.Value : DBNull.Value);
        command.Parameters.AddWithValue("@EndDate", record.EndDate.HasValue ? record.EndDate.Value : DBNull.Value);

        return await ReadToJsonAsync(command);
    }

    /// <summary>
    /// Triggers a full rebuild of all position snapshots for all products and clients.
    /// This is the C# equivalent of the old `rebuildAllPositionSnapshots` stored procedure.
    /// </summary>
    public async Task RebuildAllSnapshotsAsync()
    {
        using var connection = new SqlConnection(this.ConnectionString);
        await connection.OpenAsync();

        // 1. Get all unique combinations of user/client/product that have transactions.
        var combos = new List<(long userId, long clientId, string productSymbol)>();
        var cmd = new SqlCommand("SELECT DISTINCT [CreatedById], [ClientId], [ProductSymbol] FROM [Klondike].[TransactionLogs] WHERE [IsDeleted] = 0", connection);
        using (var reader = await cmd.ExecuteReaderAsync())
        {
            while (await reader.ReadAsync())
            {
                combos.Add((reader.GetInt64(0), reader.GetInt64(1), reader.GetString(2)));
            }
        }

        // 2. Loop through each combination and update its snapshots.
        // Note: For a very large number of products, consider running this as a background job.
        foreach (var (userId, clientId, productSymbol) in combos)
        {
            // We wrap each update in its own transaction.
            await using var transaction = await connection.BeginTransactionAsync();
            try
            {
                await UpdateSnapshotsForProductAsync(connection, (SqlTransaction)transaction, userId, clientId, productSymbol);
                await transaction.CommitAsync();
            }
            catch
            {
                await transaction.RollbackAsync();
                // Optionally log the error for the specific product and continue.
                // For now, we'll let it throw to stop the process on failure.
                throw;
            }
        }
    }

    /// <summary>
    /// A utility method to test the C# snapshot calculation against the original T-SQL stored procedure for a given product.
    /// </summary>
    /// <returns>A tuple containing the JSON result from the T-SQL proc (expected) and the C# calculator (actual).</returns>
    public async Task<Tuple<string, string>> ValidateSnapshotCalculationAsync(long userId, long clientId, string productSymbol)
    {
        using var connection = new SqlConnection(this.ConnectionString);
        await connection.OpenAsync();

        // 1. Run the OLD T-SQL procedure and get the results
        var oldProcCmd = new SqlCommand("[Klondike].[updatePositionSnapshots]", connection);
        oldProcCmd.CommandType = CommandType.StoredProcedure;
        oldProcCmd.Parameters.AddWithValue("@UserId", userId);
        oldProcCmd.Parameters.AddWithValue("@ClientId", clientId);
        oldProcCmd.Parameters.AddWithValue("@ProductSymbol", productSymbol);
        await oldProcCmd.ExecuteNonQueryAsync();

        var getSqlResultsCmd = new SqlCommand("SELECT * FROM [Klondike].[PositionSnapshots] WHERE ClientId = @ClientId AND ProductSymbol = @ProductSymbol ORDER BY SnapshotDate", connection);
        getSqlResultsCmd.Parameters.AddWithValue("@ClientId", clientId);
        getSqlResultsCmd.Parameters.AddWithValue("@ProductSymbol", productSymbol);
        string expectedJson = await ReadToJsonAsync(getSqlResultsCmd);

        // 2. Run the NEW C# logic and get the results
        await using var transaction = await connection.BeginTransactionAsync();
        await UpdateSnapshotsForProductAsync(connection, (SqlTransaction)transaction, userId, clientId, productSymbol);
        // We can read the results before committing because we're in the same session.
        var getCSharpResultsCmd = new SqlCommand("SELECT * FROM [Klondike].[PositionSnapshots] WHERE ClientId = @ClientId AND ProductSymbol = @ProductSymbol ORDER BY SnapshotDate", connection, (SqlTransaction)transaction);
        getCSharpResultsCmd.Parameters.AddWithValue("@ClientId", clientId);
        getCSharpResultsCmd.Parameters.AddWithValue("@ProductSymbol", productSymbol);
        string actualJson = await ReadToJsonAsync(getCSharpResultsCmd);
        await transaction.RollbackAsync(); // Rollback to leave the database unchanged.

        return new Tuple<string, string>(expectedJson, actualJson);
    }

    private async Task UpdateSnapshotsForProductAsync(SqlConnection connection, SqlTransaction transaction, long userId, long clientId, string productSymbol)
    {
        // Step 1: Fetch all transactions for the product.
        var transactions = await GetTransactionsForSnapshotAsync(connection, transaction, userId, clientId, productSymbol);

        // Step 2: Calculate the new snapshots in C#.
        var snapshots = _snapshotCalculator.Calculate(userId, clientId, productSymbol, transactions);

        // Step 3: Delete the old snapshots for this product.
        var deleteCmd = new SqlCommand("DELETE FROM [Klondike].[PositionSnapshots] WHERE ClientId = @ClientId AND ProductSymbol = @ProductSymbol", connection, transaction);
        deleteCmd.Parameters.AddWithValue("@ClientId", clientId);
        deleteCmd.Parameters.AddWithValue("@ProductSymbol", productSymbol);
        await deleteCmd.ExecuteNonQueryAsync();

        // Step 4: Bulk insert the new snapshots.
        if (snapshots.Any())
        {
            using var bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, transaction);
            bulkCopy.DestinationTableName = "[Klondike].[PositionSnapshots]";

            // Column mappings
            bulkCopy.ColumnMappings.Add(nameof(PositionSnapshot.UserId), "UserId");
            bulkCopy.ColumnMappings.Add(nameof(PositionSnapshot.ClientId), "ClientId");
            bulkCopy.ColumnMappings.Add(nameof(PositionSnapshot.ProductCategoryId), "ProductCategoryId");
            bulkCopy.ColumnMappings.Add(nameof(PositionSnapshot.ProductId), "ProductId");
            bulkCopy.ColumnMappings.Add(nameof(PositionSnapshot.ProductSymbol), "ProductSymbol");
            bulkCopy.ColumnMappings.Add(nameof(PositionSnapshot.SnapshotDate), "SnapshotDate");
            bulkCopy.ColumnMappings.Add(nameof(PositionSnapshot.Quantity), "Quantity");
            bulkCopy.ColumnMappings.Add(nameof(PositionSnapshot.Cost), "Cost");
            bulkCopy.ColumnMappings.Add(nameof(PositionSnapshot.Commission), "Commission");
            bulkCopy.ColumnMappings.Add(nameof(PositionSnapshot.AveragePrice), "AveragePrice");

            await bulkCopy.WriteToServerAsync(ToDataTable(snapshots));
        }
    }

    private async Task<List<SecurityTransactionLog>> GetTransactionsForSnapshotAsync(SqlConnection connection, SqlTransaction transaction, long userId, long clientId, string productSymbol)
    {
        var transactions = new List<SecurityTransactionLog>();
        var cmd = new SqlCommand("[Klondike].[getTransactionsForSnapshot]", connection, transaction);
        cmd.CommandType = CommandType.StoredProcedure;
        cmd.Parameters.AddWithValue("@UserId", userId);
        cmd.Parameters.AddWithValue("@ClientId", clientId);
        cmd.Parameters.AddWithValue("@ProductSymbol", productSymbol);

        using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            transactions.Add(new SecurityTransactionLog
            {
                Id = reader["Id"].ToString(),
                Date = (DateTime)reader["Date"],
                OperationId = (int)reader["OperationId"],
                ProductCategoryId = (int)reader["ProductCategoryId"],
                ProductId = (int)reader["ProductId"],
                ProductSymbol = (string)reader["ProductSymbol"],
                Quantity = (int)reader["Quantity"],
                Price = (decimal)reader["Price"],
                Fees = (decimal)reader["Fees"],
                // Populate the extra properties needed by the calculator
                ContractMultiplier = Convert.ToDecimal(reader["ContractMultiplier"]),
                CategoryMultiplier = Convert.ToDecimal(reader["CategoryMultiplier"])
            });
        }
        return transactions;
    }

    private static DataTable ToDataTable<T>(IList<T> data)
    {
        var table = new DataTable();
        var properties = typeof(T).GetProperties();
        foreach (var prop in properties)
        {
            table.Columns.Add(prop.Name, Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType);
        }
        foreach (var item in data)
        {
            table.Rows.Add(properties.Select(p => p.GetValue(item) ?? DBNull.Value).ToArray());
        }
        return table;
    }

    /// <summary>
    /// Converts the stored dollar value of a ZB bond price back to its 'points.ticks' representation.
    /// </summary>
    /// <param name="storedPrice">The dollar value from the database (e.g., 117562.50m).</param>
    /// <returns>The price in 'points.ticks' format (e.g., 117.18m).</returns>
    private decimal FormatZbBondPrice(decimal storedPrice)
    {
        // The stored AveragePrice is a full dollar value (e.g., 117562.50 for 117 and 18/32nds).
        // To reverse: (Price / 1000) gives points with decimals.
        var priceInPoints = storedPrice / 1000m;
        var points = Math.Floor(priceInPoints);
        var ticks = (priceInPoints - points) * 32m;
        return points + (ticks / 100m); // Combine into "points.ticks" format, e.g., 117.18
    }

    /// <summary>
    /// Groups a list of chronologically sorted transactions into "lots".
    /// A new lot begins when a position is opened from zero or flips sign (e.g., long to short).
    /// </summary>
    /// <param name="transactions">A list of transactions, sorted by Date and Id.</param>
    /// <returns>A list of lots, where each lot is a list of transactions.</returns>
    private List<List<SecurityTransactionLog>> GroupTransactionsIntoLots(List<SecurityTransactionLog> transactions)
    {
        if (transactions == null || !transactions.Any())
        {
            return new List<List<SecurityTransactionLog>>();
        }

        var lots = new List<List<SecurityTransactionLog>>();
        var currentLot = new List<SecurityTransactionLog>();
        decimal runningQuantity = 0;

        foreach (var t in transactions)
        {
            decimal signedQuantity = t.OperationId == -1 ? t.Quantity : -t.Quantity; // BUY is positive quantity

            // Check if this transaction starts a new lot.
            if (currentLot.Any() && (runningQuantity == 0 || Math.Sign(runningQuantity) * Math.Sign(runningQuantity + signedQuantity) == -1))
            {
                lots.Add(currentLot);
                currentLot = new List<SecurityTransactionLog>();
            }

            currentLot.Add(t);
            runningQuantity += signedQuantity;
        }

        // Add the final lot to the list.
        if (currentLot.Any())
        {
            lots.Add(currentLot);
        }

        return lots;
    }

    private async Task<string> ReadToJsonAsync(SqlCommand command)
    {
        using var reader = await command.ExecuteReaderAsync();
        if (!reader.HasRows)
        {
            return "[]";
        }

        var jsonBuilder = new StringBuilder("[");

        var columns = new string[reader.FieldCount];
        for (int i = 0; i < reader.FieldCount; i++)
        {
            columns[i] = reader.GetName(i);
        }

        var firstRow = true;
        while (await reader.ReadAsync())
        {
            if (!firstRow)
            {
                jsonBuilder.Append(',');
            }
            firstRow = false;

            jsonBuilder.Append('{');
            for (int i = 0; i < reader.FieldCount; i++)
            {
                // Using JsonConvert to handle proper escaping and type formatting (numbers, strings, dates, etc.)
                var value = JsonConvert.SerializeObject(reader[i]);
                jsonBuilder.AppendFormat("\"{0}\":{1}{2}", columns[i], value, i < reader.FieldCount - 1 ? "," : "");
            }
            jsonBuilder.Append('}');
        }

        return jsonBuilder.Append(']').ToString();
    }

    string ConnectionString { get; }

} /* end class TransactionLoggerService */

} /* end FalxGroup.Finance.Service namespace */