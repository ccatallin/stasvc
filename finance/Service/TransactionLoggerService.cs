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

    public async Task<int> LogTransaction(TransactionLog record)
    {
        if (!record.IsEmpty)
        {
            using (SqlConnection connection = new SqlConnection(this.ConnectionString))
            {
                connection.Open();
                var sqlQuery = "EXEC [Klondike].[logTransaction] @TransactionDate, @TransactionType, @ProductName, @ProductTypeId, @NoContracts, @ContractPrice, @CreatedById, @ClientId";

                using (SqlCommand command = new SqlCommand(sqlQuery, connection))
                {
                    command.Parameters.AddWithValue("@TransactionDate", record.TransactionDate);
                    command.Parameters.AddWithValue("@TransactionType", record.TransactionType);
                    command.Parameters.AddWithValue("@ProductName", record.ProductName.Trim()); // .Replace("'", "\'"));
                    command.Parameters.AddWithValue("@ProductTypeId", record.ProductType);
                    command.Parameters.AddWithValue("@NoContracts", record.NoContracts);
                    command.Parameters.AddWithValue("@ContractPrice", record.ContractPrice);
                    command.Parameters.AddWithValue("@CreatedById", record.CreatedById);
                    command.Parameters.AddWithValue("@ClientId", record.ClientId);

                    return await command.ExecuteNonQueryAsync();
                }
            }
        }
        else
        {
            return -1;
        }
    }

    string ConnectionString { get; }

} /* end class TransactionLoggerService */

} /* end FalxGroup.Finance.Service namespace */