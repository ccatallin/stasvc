using System;
using System.Text;
using System.Threading.Tasks;
// --
using Microsoft.Data.SqlClient;
// --
using Newtonsoft.Json;
using FalxGroup.Finance.Model;

namespace FalxGroup.Finance.Service
{

public class TransactionLoggerService
{
    public TransactionLoggerService(string connectionString)
    {
        this.ConnectionString = connectionString;
    }

    public async Task<Tuple<int, string>> LogTransaction(TransactionLog record)
    {
        if (record == null || record.IsEmpty)
        {
            return new Tuple<int, string>(-1, null);
        }

        using var connection = new SqlConnection(this.ConnectionString);
        await connection.OpenAsync();
        var sqlQuery = "EXEC [Klondike].[logTransaction] @Date, @TypeId, @ProductCategoryId, @ProductTypeId, @ProductName, @Quantity, @Price, @Fees, @Notes, @CreatedById, @ClientId, @Id OUTPUT";

        using var command = new SqlCommand(sqlQuery, connection);

        command.Parameters.AddWithValue("@Date", record.Date);
        command.Parameters.AddWithValue("@TypeId", record.TypeId);
        
        command.Parameters.AddWithValue("@ProductCategoryId", record.ProductCategoryId);        
        command.Parameters.AddWithValue("@ProductTypeId", record.ProductTypeId);
        command.Parameters.AddWithValue("@ProductName", record.ProductName.Trim());
        command.Parameters.AddWithValue("@Quantity", record.Quantity);
        command.Parameters.AddWithValue("@Price", record.Price);
        command.Parameters.AddWithValue("@Fees", record.Fees);
        
        command.Parameters.AddWithValue("@CreatedById", record.UserId);
        command.Parameters.AddWithValue("@ClientId", record.ClientId);
        command.Parameters.AddWithValue("@Notes", record.Notes?.Trim());

        command.Parameters.Add("@Id", System.Data.SqlDbType.VarChar, 100);
        command.Parameters["@Id"].Direction = System.Data.ParameterDirection.Output;

        var returnParameter = command.Parameters.Add("@RETURN_VALUE", System.Data.SqlDbType.Int);
        returnParameter.Direction = System.Data.ParameterDirection.ReturnValue;

        await command.ExecuteNonQueryAsync();

        var id = Convert.ToString(command.Parameters["@Id"].Value);
        var result = (int)returnParameter.Value;
        return new Tuple<int, string>(result, id);
    }

    public async Task<Tuple<int, string>> UpdateTransactionLog(TransactionLog record)
    {
        if (record == null || record.IsEmpty || string.IsNullOrWhiteSpace(record.Id))
        {
            return new Tuple<int, string>(-1, null);
        }

        using var connection = new SqlConnection(this.ConnectionString);
        await connection.OpenAsync();
        var sqlQuery = "EXEC [Klondike].[updateTransactionLog] @Id, @Date, @TypeId, @ProductCategoryId, @ProductTypeId, @ProductName, @Quantity, @Price, @Fees, @Notes, @ModifiedById, @ClientId";

        using var command = new SqlCommand(sqlQuery, connection);

        command.Parameters.AddWithValue("@Id", record.Id);
        
        command.Parameters.AddWithValue("@Date", record.Date);
        command.Parameters.AddWithValue("@TypeId", record.TypeId);
        
        command.Parameters.AddWithValue("@ProductCategoryId", record.ProductCategoryId);
        command.Parameters.AddWithValue("@ProductTypeId", record.ProductTypeId);
        command.Parameters.AddWithValue("@ProductName", record.ProductName.Trim());
        command.Parameters.AddWithValue("@Quantity", record.Quantity);
        command.Parameters.AddWithValue("@Price", record.Price);
        command.Parameters.AddWithValue("@Fees", record.Fees);

        command.Parameters.AddWithValue("@Notes", record.Notes?.Trim());
            
        command.Parameters.AddWithValue("@ModifiedById", record.UserId);
        command.Parameters.AddWithValue("@ClientId", record.ClientId);

        var returnParameter = command.Parameters.Add("@RETURN_VALUE", System.Data.SqlDbType.Int);
        returnParameter.Direction = System.Data.ParameterDirection.ReturnValue;

        await command.ExecuteNonQueryAsync();

        var result = (int)returnParameter.Value;
        return new Tuple<int, string>(result, record.Id);
    }

    public async Task<Tuple<int, string>> DeleteTransactionLog(TransactionLog record)
    {
        if (string.IsNullOrWhiteSpace(record?.Id))
        {
            return new Tuple<int, string>(-1, null);
        }

        using var connection = new SqlConnection(this.ConnectionString);
        await connection.OpenAsync();

        var sqlQuery = "EXEC [Klondike].[deleteTransactionLog] @Id, @ModifiedById, @ClientId";

        using var command = new SqlCommand(sqlQuery, connection);
        
        command.Parameters.AddWithValue("@Id", record.Id);
        command.Parameters.AddWithValue("@ModifiedById", record.UserId);
        command.Parameters.AddWithValue("@ClientId", record.ClientId);

        var returnParameter = command.Parameters.Add("@RETURN_VALUE", System.Data.SqlDbType.Int);
        returnParameter.Direction = System.Data.ParameterDirection.ReturnValue;

        await command.ExecuteNonQueryAsync();
        
        var result = (int)returnParameter.Value;
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

        var sqlQuery = "EXEC [Klondike].[getProductTransactionLogs] @sProductName, @UserId, @ClientId";

        using SqlCommand command = new SqlCommand(sqlQuery, connection);
        command.Parameters.AddWithValue("@sProductName", record.ProductName);
        command.Parameters.AddWithValue("@UserId", record.UserId);
        command.Parameters.AddWithValue("@ClientId", record.ClientId);

        return await ReadToJsonAsync(command);
    }

    public async Task<string> GetRealizedProfitAndLoss(TransactionLog record)
    {
        using SqlConnection connection = new SqlConnection(this.ConnectionString);
        await connection.OpenAsync();

        var sqlQuery = "EXEC [Klondike].[getProfitAndLossEx] @UserId, @ClientId, @StartDate, @EndDate, @realized";

        using SqlCommand command = new SqlCommand(sqlQuery, connection);
        command.Parameters.AddWithValue("@UserId", record.UserId);
        command.Parameters.AddWithValue("@ClientId", record.ClientId);
        command.Parameters.AddWithValue("@StartDate", record.StartDate.HasValue ? record.StartDate.Value : DBNull.Value);
        command.Parameters.AddWithValue("@EndDate", record.EndDate.HasValue ? record.EndDate.Value : DBNull.Value);
        command.Parameters.AddWithValue("@realized", 1);

        return await ReadToJsonAsync(command);
    }

    public async Task<string> GetTransactionLogs(TransactionLog record)
    {
        using SqlConnection connection = new SqlConnection(this.ConnectionString);
        await connection.OpenAsync();

        var sqlQuery = "EXEC [Klondike].[getTransactionLogs] @UserId, @ClientId, @StartDate, @EndDate";

        using SqlCommand command = new SqlCommand(sqlQuery, connection);
        command.Parameters.AddWithValue("@UserId", record.UserId);
        command.Parameters.AddWithValue("@ClientId", record.ClientId);
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