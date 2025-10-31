using System;
using System.Text;
using System.Threading.Tasks;
// --
using Microsoft.Data.SqlClient;
// --
using System.Linq;
using System.Collections.Generic;
using System.Data;
using Newtonsoft.Json;
using FalxGroup.Finance.Model;
using FalxGroup.Finance.BusinessLogic;

namespace FalxGroup.Finance.Service
{

public class TransactionLoggerService
{
    private readonly PositionSnapshotCalculator _snapshotCalculator;
    public TransactionLoggerService(string connectionString)
    {
        this.ConnectionString = connectionString;
        this._snapshotCalculator = new PositionSnapshotCalculator();
    }

    public async Task<Tuple<int, string>> LogTransaction(TransactionLog record, string mode)
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

    public async Task<Tuple<int, string>> UpdateTransactionLog(TransactionLog record)
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
            var getOldSymbolCmd = new SqlCommand("SELECT [ProductSymbol] FROM [Klondike].[TransactionLogs] WHERE [Id] = @Id AND [UserId] = @UserId  AND [ClientId] = @ClientId", connection, (SqlTransaction)transaction);
            getOldSymbolCmd.Parameters.AddWithValue("@Id", record.Id);
            getOldSymbolCmd.Parameters.AddWithValue("@UserId", record.UserId);
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

    public async Task<Tuple<int, string>> DeleteTransactionLog(TransactionLog record)
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
            var getSymbolCmd = new SqlCommand("SELECT [ProductSymbol] FROM [Klondike].[TransactionLogs] WHERE [Id] = @Id AND [UserId] = @UserId AND [ClientId] = @ClientId", connection, (SqlTransaction)transaction);
            getSymbolCmd.Parameters.AddWithValue("@Id", record.Id);
            getSymbolCmd.Parameters.AddWithValue("@UserId", record.UserId);
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

    public async Task<string> GetOpenPositions(TransactionLog record)
    {
        using SqlConnection connection = new SqlConnection(this.ConnectionString);
        await connection.OpenAsync();

        var sqlQuery = "EXEC [Klondike].[getOpenPositions] @UserId, @ClientId";

        using SqlCommand command = new SqlCommand(sqlQuery, connection);
        command.Parameters.AddWithValue("@UserId", record.UserId);
        command.Parameters.AddWithValue("@ClientId", record.ClientId);

        return await ReadToJsonAsync(command);
    }
    
    public async Task<string> GetTransactionLogById(TransactionLog record)
    {
        using SqlConnection connection = new SqlConnection(this.ConnectionString);
        await connection.OpenAsync();

        var sqlQuery = "EXEC [Klondike].[getTransactionLogById] @Id, @UserId, @ClientId";

        using SqlCommand command = new SqlCommand(sqlQuery, connection);
        command.Parameters.AddWithValue("@UId", record.Id);
        command.Parameters.AddWithValue("@UserId", record.UserId);
        command.Parameters.AddWithValue("@ClientId", record.ClientId);

        return await ReadToJsonAsync(command);
    }

    public async Task<string> GetProductTransactionLogs(TransactionLog record)
    {
        using SqlConnection connection = new SqlConnection(this.ConnectionString);
        await connection.OpenAsync();

        var sqlQuery = "EXEC [Klondike].[getProductTransactionLogs] @ProductSymbol, @UserId, @ClientId";

        using SqlCommand command = new SqlCommand(sqlQuery, connection);
        command.Parameters.AddWithValue("@ProductSymbol", record.ProductSymbol);
        command.Parameters.AddWithValue("@UserId", record.UserId);
        command.Parameters.AddWithValue("@ClientId", record.ClientId);

        return await ReadToJsonAsync(command);
    }
    
    public async Task<string> GetOpenPositionTransactionLogs(TransactionLog record)
    {
        using SqlConnection connection = new SqlConnection(this.ConnectionString);
        await connection.OpenAsync();

        var sqlQuery = "EXEC [Klondike].[getOpenPositionTransactionLogs] @ProductSymbol, @UserId, @ClientId";

        using SqlCommand command = new SqlCommand(sqlQuery, connection);
        command.Parameters.AddWithValue("@ProductSymbol", record.ProductSymbol);
        command.Parameters.AddWithValue("@UserId", record.UserId);
        command.Parameters.AddWithValue("@ClientId", record.ClientId);

        return await ReadToJsonAsync(command);
    }

    public async Task<string> GetRealizedProfitAndLoss(TransactionLog record)
    {
        using SqlConnection connection = new SqlConnection(this.ConnectionString);
        await connection.OpenAsync();

        var sqlQuery = "EXEC [Klondike].[getProfitAndLoss] @UserId, @ClientId, @ProductCategoryId, @ProductId, @ProductSymbol, @StartDate, @EndDate, @realized";

        using SqlCommand command = new SqlCommand(sqlQuery, connection);
        command.Parameters.AddWithValue("@UserId", record.UserId);
        command.Parameters.AddWithValue("@ClientId", record.ClientId);
        command.Parameters.AddWithValue("@ProductCategoryId", (0 != record.ProductCategoryId) ? record.ProductCategoryId : DBNull.Value);
        command.Parameters.AddWithValue("@ProductId", (0 != record.ProductId) ? record.ProductId : DBNull.Value);
        command.Parameters.AddWithValue("@ProductSymbol", (0 != record.ProductSymbol.Length) ? record.ProductSymbol : DBNull.Value);
        command.Parameters.AddWithValue("@StartDate", record.StartDate.HasValue ? record.StartDate.Value : DBNull.Value);
        command.Parameters.AddWithValue("@EndDate", record.EndDate.HasValue ? record.EndDate.Value : DBNull.Value);
        command.Parameters.AddWithValue("@realized", 1);

        return await ReadToJsonAsync(command);
    }

    public async Task<string> GetTransactionLogs(TransactionLog record)
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

    private async Task<List<TransactionLog>> GetTransactionsForSnapshotAsync(SqlConnection connection, SqlTransaction transaction, long userId, long clientId, string productSymbol)
    {
        var transactions = new List<TransactionLog>();
        var cmd = new SqlCommand("[Klondike].[getTransactionsForSnapshot]", connection, transaction);
        cmd.CommandType = CommandType.StoredProcedure;
        cmd.Parameters.AddWithValue("@UserId", userId);
        cmd.Parameters.AddWithValue("@ClientId", clientId);
        cmd.Parameters.AddWithValue("@ProductSymbol", productSymbol);

        using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            transactions.Add(new TransactionLog
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