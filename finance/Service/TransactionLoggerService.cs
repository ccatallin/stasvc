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
        if (!record.IsEmpty)
        {
            using (SqlConnection connection = new SqlConnection(this.ConnectionString))
            {
                connection.Open();
                var sqlQuery = "EXEC [Klondike].[logTransaction] @TransactionDate, @TransactionType, @ProductName, @ProductTypeId, @NoContracts, @ContractPrice, @TransactionFees, @CreatedById, @ClientId, @Notes, @TransactionId OUTPUT";

                using (SqlCommand command = new SqlCommand(sqlQuery, connection))
                {
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
                    var transactionId =  Convert.ToString(command.Parameters["@TransactionId"].Value);

                    return new Tuple<int, string>(result, transactionId);
                }
            }
        }
        else
        {
            return new Tuple<int, string>(-1, null);
        }
    }

    public async Task<Tuple<int, string>> UpdateTransaction(TransactionLog record)
    {
        if (!record.IsEmpty)
        {
            using (SqlConnection connection = new SqlConnection(this.ConnectionString))
            {
                connection.Open();
                var sqlQuery = "EXEC [Klondike].[updateTransaction] @TransactionId, @TransactionDate, @TransactionType, @ProductName, @ProductTypeId, @NoContracts, @ContractPrice, @TransactionFees, @ModifiedById, @Notes";

                using (SqlCommand command = new SqlCommand(sqlQuery, connection))
                {
                    command.Parameters.AddWithValue("@TransactionId", record.TransactionId);
                    command.Parameters.AddWithValue("@TransactionDate", record.TransactionDate);
                    command.Parameters.AddWithValue("@TransactionType", record.TransactionType);
                    command.Parameters.AddWithValue("@ProductName", record.ProductName.Trim());
                    command.Parameters.AddWithValue("@ProductTypeId", record.ProductType);
                    command.Parameters.AddWithValue("@NoContracts", record.NoContracts);
                    command.Parameters.AddWithValue("@ContractPrice", record.ContractPrice);
                    command.Parameters.AddWithValue("@TransactionFees", record.TransactionFees);
                    command.Parameters.AddWithValue("@ModifiedById", record.UserId);
                    command.Parameters.AddWithValue("@Notes", record.Notes?.Trim());

                    var result = await command.ExecuteNonQueryAsync();
                    var transactionId = record.TransactionId;

                    return new Tuple<int, string>(result, transactionId);
                }
            }
        }
        else
        {
            return new Tuple<int, string>(-1, null);
        }
    }

    public async Task<Tuple<int, string>> DeleteTransaction(TransactionLog record)
    {
        using (SqlConnection connection = new SqlConnection(this.ConnectionString))
        {
            connection.Open();

            var sqlQuery = "EXEC [Klondike].[deleteTransaction] @TransactionId, @ModifiedById, @ClientId";

            using (SqlCommand command = new SqlCommand(sqlQuery, connection))
            {
                command.Parameters.AddWithValue("@TransactionId", record.TransactionId);
                command.Parameters.AddWithValue("@ModifiedById", record.UserId);
                command.Parameters.AddWithValue("@ClientId", record.ClientId);

                var result = await command.ExecuteNonQueryAsync();

                return new Tuple<int, string>(result, record.TransactionId);
            }
        }
    }

    string ConnectionString { get; }

} /* end class TransactionLoggerService */

} /* end FalxGroup.Finance.Service namespace */