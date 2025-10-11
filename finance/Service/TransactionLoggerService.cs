using System;
using System.Text;
using System.Threading.Tasks;
// --
using Microsoft.Data.SqlClient;
// --
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
        var sqlQuery = "EXEC [Klondike].[logTransaction] @TransactionDate, @TransactionType, @ProductName, @ProductTypeId, @NoContracts, @ContractPrice, @TransactionFees, @CreatedById, @ClientId, @Notes, @TransactionId OUTPUT";

        using var command = new SqlCommand(sqlQuery, connection);
        command.Parameters.AddWithValue("@TransactionDate", record.TransactionDate);
        command.Parameters.AddWithValue("@TransactionType", record.TransactionType);
        command.Parameters.AddWithValue("@ProductName", record.ProductName.Trim());
        command.Parameters.AddWithValue("@ProductTypeId", record.ProductType);
        command.Parameters.AddWithValue("@NoContracts", record.NoContracts);
        command.Parameters.AddWithValue("@ContractPrice", record.ContractPrice);
        command.Parameters.AddWithValue("@TransactionFees", record.TransactionFees);
        command.Parameters.AddWithValue("@CreatedById", record.UserId);
        command.Parameters.AddWithValue("@ClientId", record.ClientId);
        command.Parameters.AddWithValue("@Notes", record.Notes?.Trim());

        command.Parameters.Add("@TransactionId", System.Data.SqlDbType.VarChar, 100);
        command.Parameters["@TransactionId"].Direction = System.Data.ParameterDirection.Output;

        var result = await command.ExecuteNonQueryAsync();
        var transactionId = Convert.ToString(command.Parameters["@TransactionId"].Value);

        return new Tuple<int, string>(result, transactionId);
    }

    public async Task<Tuple<int, string>> UpdateTransaction(TransactionLog record)
    {
        if (record == null || record.IsEmpty || string.IsNullOrWhiteSpace(record.TransactionId))
        {
            return new Tuple<int, string>(-1, null);
        }

        using var connection = new SqlConnection(this.ConnectionString);
        await connection.OpenAsync();
        var sqlQuery = "EXEC [Klondike].[updateTransactionEx] @TransactionId, @TransactionDate, @TransactionType, @ProductName, @ProductTypeId, @NoContracts, @ContractPrice, @TransactionFees, @ModifiedById, @ClientId, @Notes";

        using var command = new SqlCommand(sqlQuery, connection);
        command.Parameters.AddWithValue("@TransactionId", record.TransactionId);
        command.Parameters.AddWithValue("@TransactionDate", record.TransactionDate);
        command.Parameters.AddWithValue("@TransactionType", record.TransactionType);
        command.Parameters.AddWithValue("@ProductName", record.ProductName.Trim());
        command.Parameters.AddWithValue("@ProductTypeId", record.ProductType);
        command.Parameters.AddWithValue("@NoContracts", record.NoContracts);
        command.Parameters.AddWithValue("@ContractPrice", record.ContractPrice);
        command.Parameters.AddWithValue("@TransactionFees", record.TransactionFees);
        command.Parameters.AddWithValue("@ModifiedById", record.UserId);
        command.Parameters.AddWithValue("@ClientId", record.ClientId);
        command.Parameters.AddWithValue("@Notes", record.Notes?.Trim());

        var result = await command.ExecuteNonQueryAsync();
        var transactionId = record.TransactionId;

        return new Tuple<int, string>(result, transactionId);
    }

    public async Task<Tuple<int, string>> DeleteTransaction(TransactionLog record)
    {
        if (string.IsNullOrWhiteSpace(record?.TransactionId))
        {
            return new Tuple<int, string>(-1, null);
        }

        using var connection = new SqlConnection(this.ConnectionString);
        await connection.OpenAsync();

        var sqlQuery = "EXEC [Klondike].[deleteTransaction] @TransactionId, @ModifiedById, @ClientId";

        using var command = new SqlCommand(sqlQuery, connection);
        command.Parameters.AddWithValue("@TransactionId", record.TransactionId);
        command.Parameters.AddWithValue("@ModifiedById", record.UserId);
        command.Parameters.AddWithValue("@ClientId", record.ClientId);

        var result = await command.ExecuteNonQueryAsync();
        return new Tuple<int, string>(result, record.TransactionId);
    }

    public async Task<string> GetOpenPositions(TransactionLog record)
    {
        using SqlConnection connection = new SqlConnection(this.ConnectionString);
        await connection.OpenAsync();

        var sqlQuery = "EXEC [Klondike].[getOpenPositionsEx] @UserId, @ClientId";

        using SqlCommand command = new SqlCommand(sqlQuery, connection);
        command.Parameters.AddWithValue("@UserId", record.UserId);
        command.Parameters.AddWithValue("@ClientId", record.ClientId);

        using var reader = await command.ExecuteReaderAsync();
        if (!reader.HasRows)
        {
            return "[]";
        }

        var jsonBuilder = new StringBuilder();
        jsonBuilder.Append("[");

        var columns = new string[reader.FieldCount];
        for (int i = 0; i < reader.FieldCount; i++)
        {
            columns[i] = reader.GetName(i);
        }

        while (await reader.ReadAsync())
        {
            jsonBuilder.Append('{');
            for (int i = 0; i < reader.FieldCount; i++)
            {
                jsonBuilder.AppendFormat("\"{0}\":\"{1}\"{2}", columns[i], reader[i], i < reader.FieldCount - 1 ? "," : "");
            }
            jsonBuilder.Append("},");
        }

        return jsonBuilder.Remove(jsonBuilder.Length - 1, 1).Append("]").ToString();
    }

    public async Task<string> GetProductTransactionLogs(TransactionLog record)
    {
        using SqlConnection connection = new SqlConnection(this.ConnectionString);
        await connection.OpenAsync();

        var sqlQuery = "EXEC [Klondike].[getProductTransactionLogsEx] @sProductName, @UserId, @ClientId";

        using SqlCommand command = new SqlCommand(sqlQuery, connection);
        command.Parameters.AddWithValue("@sProductName", record.ProductName);
        command.Parameters.AddWithValue("@UserId", record.UserId);
        command.Parameters.AddWithValue("@ClientId", record.ClientId);

        using var reader = await command.ExecuteReaderAsync();
        if (!reader.HasRows)
        {
            return "[]";
        }

        var jsonBuilder = new StringBuilder();
        jsonBuilder.Append('[');

        var columns = new string[reader.FieldCount];
        for (int i = 0; i < reader.FieldCount; i++)
        {
            columns[i] = reader.GetName(i);
        }

        while (await reader.ReadAsync())
        {
            jsonBuilder.Append("{");
            for (int i = 0; i < reader.FieldCount; i++)
            {
                jsonBuilder.AppendFormat("\"{0}\":\"{1}\"{2}", columns[i], reader[i], i < reader.FieldCount - 1 ? "," : "");
            }
            jsonBuilder.Append("},");
        }

        return jsonBuilder.Remove(jsonBuilder.Length - 1, 1).Append("]").ToString();
    }

    public async Task<string> GetRealizedProfitAndLoss(TransactionLog record)
    {
        using SqlConnection connection = new SqlConnection(this.ConnectionString);
        await connection.OpenAsync();

        var sqlQuery = "EXEC [Klondike].[getProfitAndLoss] @UserId, @ClientId, @realized";

        using SqlCommand command = new SqlCommand(sqlQuery, connection);
        command.Parameters.AddWithValue("@UserId", record.UserId);
        command.Parameters.AddWithValue("@ClientId", record.ClientId);
        command.Parameters.AddWithValue("@realized", 1);

        using var reader = await command.ExecuteReaderAsync();
        if (!reader.HasRows)
        {
            return "[]";
        }

        var jsonBuilder = new StringBuilder();
        jsonBuilder.Append('[');

        var columns = new string[reader.FieldCount];
        for (int i = 0; i < reader.FieldCount; i++)
        {
            columns[i] = reader.GetName(i);
        }

        while (await reader.ReadAsync())
        {
            jsonBuilder.Append('{');
            for (int i = 0; i < reader.FieldCount; i++)
            {
                jsonBuilder.AppendFormat("\"{0}\":\"{1}\"{2}", columns[i], reader[i], i < reader.FieldCount - 1 ? "," : "");
            }
            jsonBuilder.Append("},");
        }

        return jsonBuilder.Remove(jsonBuilder.Length - 1, 1).Append("]").ToString();
    }

    string ConnectionString { get; }

} /* end class TransactionLoggerService */

} /* end FalxGroup.Finance.Service namespace */