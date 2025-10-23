using System;
using System.Text;
using System.Threading.Tasks;
// --
using Microsoft.Data.SqlClient;
// --
using Newtonsoft.Json;
using FalxGroup.Finance.Model;
using Microsoft.AspNetCore.Mvc.Formatters;

namespace FalxGroup.Finance.Service
{

public class TransactionLoggerService
{
    public TransactionLoggerService(string connectionString)
    {
        this.ConnectionString = connectionString;
    }

    public async Task<Tuple<int, string>> LogTransaction(TransactionLog record, string mode)
    {
        if (record == null || record.IsEmpty)
        {
            return new Tuple<int, string>(-1, null);
        }

        using var connection = new SqlConnection(this.ConnectionString);
        await connection.OpenAsync();
        var sqlQuery = "EXEC [Klondike].[logTransaction] @Date, @OperationId, @ProductCategoryId, @ProductId, @ProductSymbol, @Quantity, @Price, @Fees, @Notes, @CreatedById, @ClientId, @Mode, @Id OUTPUT, @InsertedCount OUTPUT";

        using var command = new SqlCommand(sqlQuery, connection);

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

        command.Parameters.Add("@Id", System.Data.SqlDbType.VarChar, 100);
        command.Parameters["@Id"].Direction = System.Data.ParameterDirection.Output;

        var insertedCountParameter = command.Parameters.Add("@InsertedCount", System.Data.SqlDbType.Int);
        insertedCountParameter.Direction = System.Data.ParameterDirection.Output;

        await command.ExecuteNonQueryAsync();

        var id = Convert.ToString(command.Parameters["@Id"].Value);
        var result = (int)insertedCountParameter.Value;

        // If the result is 1 (success), return the new ID.
        // Otherwise (0 for no insert, -2 for validation failure), return a null ID.
        var returnId = result == 1 ? id : null;

        return new Tuple<int, string>(result, returnId);
    }

    public async Task<Tuple<int, string>> UpdateTransactionLog(TransactionLog record)
    {
        if (record == null || record.IsEmpty || string.IsNullOrWhiteSpace(record.Id))
        {
            return new Tuple<int, string>(-1, null);
        }

        using var connection = new SqlConnection(this.ConnectionString);
        await connection.OpenAsync();
        var sqlQuery = "EXEC [Klondike].[updateTransactionLog] @Id, @Date, @OperationId, @ProductCategoryId, @ProductId, @ProductSymbol, @Quantity, @Price, @Fees, @Notes, @ModifiedById, @ClientId, @UpdatedCount OUTPUT";

        using var command = new SqlCommand(sqlQuery, connection);

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

        // If the result is 1 (success), return the record's ID.
        // Otherwise (0 for no update, -2 for validation failure), return a null ID.
        var returnId = result == 1 ? record.Id : null;
        return new Tuple<int, string>(result, returnId);
    }

    public async Task<Tuple<int, string>> DeleteTransactionLog(TransactionLog record)
    {
        if (string.IsNullOrWhiteSpace(record?.Id))
        {
            return new Tuple<int, string>(-1, null);
        }

        using var connection = new SqlConnection(this.ConnectionString);
        await connection.OpenAsync();

        var sqlQuery = "EXEC [Klondike].[deleteTransactionLog] @Id, @ModifiedById, @ClientId, @DeletedCount OUTPUT";

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

        var sqlQuery = "EXEC [Klondike].[getProfitAndLoss] @UserId, @ClientId, @ProductCategoryId, @ProductId, @ProductSymbol, StartDate, @EndDate, @realized";

        using SqlCommand command = new SqlCommand(sqlQuery, connection);
        command.Parameters.AddWithValue("@UserId", record.UserId);
        command.Parameters.AddWithValue("@ClientId", record.ClientId);
        command.Parameters.AddWithValue("@ProductCategoryId", record.ProductCategoryId);
        command.Parameters.AddWithValue("@ProductId", record.ProductId);
        command.Parameters.AddWithValue("@ProductSymbol", record.ProductSymbol);
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
        command.Parameters.AddWithValue("@ProductCategoryId", record.ProductCategoryId);
        command.Parameters.AddWithValue("@ProductId", record.ProductId);
        command.Parameters.AddWithValue("@ProductSymbol", record.ProductSymbol);
        command.Parameters.AddWithValue("@StartDate", record.StartDate.HasValue ? record.StartDate.Value : DBNull.Value);
        command.Parameters.AddWithValue("@EndDate", record.EndDate.HasValue ? record.EndDate.Value : DBNull.Value);

        return await ReadToJsonAsync(command);
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