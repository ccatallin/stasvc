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
        public string ProductSymbol { get; set; } = string.Empty;
        public decimal Quantity { get; set; }
        public decimal AveragePrice { get; set; }
        public decimal Cost { get; set; }
        public decimal Commission { get; set; }
        public decimal TotalCost => Cost + Commission;
    }

    public class LotSummary
    {
        public string ProductSymbol { get; set; } = string.Empty;
        public int ProductCategoryId { get; set; }
        public int ProductId { get; set; }
        public DateTime FirstTransactionDate { get; set; }
        public DateTime LastTransactionDate { get; set; }
        public decimal NetQuantity { get; set; }
        public decimal Profit { get; set; }
        public decimal Fees { get; set; }
        public decimal Total { get; set; }
        public bool IsClosed { get; set; }
    }

public class TransactionLoggerService
{
    private readonly PositionSnapshotCalculator _snapshotCalculator;
    public TransactionLoggerService(string connectionString)
    {
        this.ConnectionString = connectionString;
        this._snapshotCalculator = new PositionSnapshotCalculator();
    }

    public async Task<Tuple<int, string?, decimal?>> LogTransaction(SecurityTransactionLog record, string mode)
    {
        if (record == null || record.IsEmpty)
        {
            return new Tuple<int, string?, decimal?>(-1, null, null);
        }

        using var connection = new SqlConnection(this.ConnectionString);
        await connection.OpenAsync();
        await using var transaction = await connection.BeginTransactionAsync();

        try
        {
            const string sql = "[Klondike].[logTransaction]";
            var parameters = new DynamicParameters();
            parameters.Add("@Date", record.Date);
            parameters.Add("@OperationId", record.OperationId);
            parameters.Add("@ProductCategoryId", record.ProductCategoryId);
            parameters.Add("@ProductId", record.ProductId);
            parameters.Add("@ProductSymbol", record.ProductSymbol!.Trim());
            parameters.Add("@Quantity", record.Quantity);
            parameters.Add("@Price", record.Price);
            parameters.Add("@Fees", record.Fees);
            parameters.Add("@Notes", record.Notes?.Trim());
            parameters.Add("@CreatedById", record.UserId);
            parameters.Add("@ClientId", record.ClientId);
            parameters.Add("@Mode", mode.Equals("import") ? 0 : 1);
            parameters.Add("@Id", dbType: DbType.String, direction: ParameterDirection.Output, size: 100);
            parameters.Add("@InsertedCount", dbType: DbType.Int32, direction: ParameterDirection.Output);

            await connection.ExecuteAsync(sql, parameters, transaction: transaction, commandType: CommandType.StoredProcedure);

            string? id = parameters.Get<string>("@Id");
            int result = parameters.Get<int>("@InsertedCount");

            decimal? newCashBalance = null;
            if (result == 1)
            {
                // First, update the cash balance with the immediate impact of the trade (e.g., fees for futures).
                newCashBalance = await UpdateCashBalanceForSecurityTransactionAsync(connection, (SqlTransaction)transaction, record);

                // Then, update the position snapshots.
                await UpdateSnapshotsForProductAsync(connection, (SqlTransaction)transaction, record.UserId, record.ClientId, record.ProductSymbol!);

                // Finally, if this trade closed a futures position, realize the P&L into the cash balance.
                newCashBalance = await HandleFuturesPnLRealizationAsync(connection, (SqlTransaction)transaction, record, newCashBalance);
            }

            await transaction.CommitAsync();
            return new Tuple<int, string?, decimal?>(result, result == 1 ? id : null, newCashBalance);
        }
        catch
        {
            await transaction.RollbackAsync();
            throw;
        }
    }

    public async Task<Tuple<int, string?, decimal?>> UpdateTransactionLog(SecurityTransactionLog record)
    {
        if (record == null || record.IsEmpty || string.IsNullOrWhiteSpace(record.Id))
        {
            return new Tuple<int, string?, decimal?>(-1, null, null);
        }

        using var connection = new SqlConnection(this.ConnectionString);
        await connection.OpenAsync();
        await using var transaction = await connection.BeginTransactionAsync();

        try
        {
            // First, get the original transaction details to revert its cash impact.
            // Alias CreatedById as UserId so Dapper maps it to SecurityTransactionLog.UserId.
            const string getOldTxSql = "SELECT *, [CreatedById] AS [UserId] FROM [Klondike].[TransactionLogs] WHERE [Id] = @Id AND [ClientId] = @ClientId";
            var oldTransaction = await connection.QuerySingleOrDefaultAsync<SecurityTransactionLog>(getOldTxSql, new { record.Id, record.ClientId }, transaction);
            var oldProductSymbol = oldTransaction?.ProductSymbol;

            // For futures: revert any previously realized P&L BEFORE the SP changes the DB state.
            // Without this, HandleFuturesPnLRealizationAsync later adds the new P&L on top of the
            // old one that is already in the cash balance — causing double-counting.
            decimal? newCashBalance = null;
            if (oldTransaction != null)
            {
                newCashBalance = await RevertFuturesPnLIfClosedAsync(connection, (SqlTransaction)transaction, oldTransaction, newCashBalance);
            }

            // Now, execute the update.
            const string updateSql = "[Klondike].[updateTransactionLog]";
            var parameters = new DynamicParameters();
            parameters.Add("@Id", record.Id);
            parameters.Add("@Date", record.Date);
            parameters.Add("@OperationId", record.OperationId);
            parameters.Add("@ProductCategoryId", record.ProductCategoryId);
            parameters.Add("@ProductId", record.ProductId);
            parameters.Add("@ProductSymbol", record.ProductSymbol!.Trim());
            parameters.Add("@Quantity", record.Quantity);
            parameters.Add("@Price", record.Price);
            parameters.Add("@Fees", record.Fees);
            parameters.Add("@Notes", record.Notes?.Trim());
            parameters.Add("@ModifiedById", record.UserId);
            parameters.Add("@ClientId", record.ClientId);
            parameters.Add("@UpdatedCount", dbType: DbType.Int32, direction: ParameterDirection.Output);

            await connection.ExecuteAsync(updateSql, parameters, transaction: transaction, commandType: CommandType.StoredProcedure);

            int result = parameters.Get<int>("@UpdatedCount");

            if (result == 1)
            {
                // Smartly update cash balance only if relevant fields have changed.
                // This prevents incorrect balance changes when updating non-financial fields like notes or date.
                bool cashImpactChanged = oldTransaction is null ||
                                         oldTransaction.OperationId != record.OperationId ||
                                         oldTransaction.Quantity != record.Quantity ||
                                         oldTransaction.Price != record.Price ||
                                         oldTransaction.Fees != record.Fees;

                if (cashImpactChanged)
                {
                    // Revert the old transaction's cash impact and apply the new one.
                    decimal? oldRevertedBalance = await UpdateCashBalanceForSecurityTransactionAsync(connection, (SqlTransaction)transaction, oldTransaction, revert: true);
                    newCashBalance = await UpdateCashBalanceForSecurityTransactionAsync(connection, (SqlTransaction)transaction, record, currency: "USD", startingBalance: oldRevertedBalance);
                }
                else
                {
                    // If cash impact hasn't changed, we still need to return the current balance.
                    const string getBalanceSql = "SELECT Balance FROM [Klondike].[CashBalances] WHERE CreatedById = @UserId AND ClientId = @ClientId AND Currency = 'USD'";
                    newCashBalance = await connection.QuerySingleOrDefaultAsync<decimal?>(
                        getBalanceSql, 
                        new { record.UserId, record.ClientId }, 
                        transaction);
                }

                // Recalculate for the new/current product symbol.
                await UpdateSnapshotsForProductAsync(connection, (SqlTransaction)transaction, record.UserId, record.ClientId, record.ProductSymbol!);
                // Realize P&L for the new symbol if a position was closed.
                newCashBalance = await HandleFuturesPnLRealizationAsync(connection, (SqlTransaction)transaction, record, newCashBalance);

                // If the symbol changed, we must also recalculate the old one.
                if (!string.IsNullOrEmpty(oldProductSymbol) && oldProductSymbol != record.ProductSymbol)
                {
                    await UpdateSnapshotsForProductAsync(connection, (SqlTransaction)transaction, record.UserId, record.ClientId, oldProductSymbol);
                }
            }

            await transaction.CommitAsync();
            return new Tuple<int, string?, decimal?>(result, result == 1 ? record.Id : null, newCashBalance);
        }
        catch
        {
            await transaction.RollbackAsync();
            throw;
        }
    }

    public async Task<Tuple<int, string?, decimal?>> DeleteTransactionLog(SecurityTransactionLog record)
    {
        if (string.IsNullOrWhiteSpace(record?.Id))
        {
            return new Tuple<int, string?, decimal?>(-1, null, null);
        }

        using var connection = new SqlConnection(this.ConnectionString);
        await connection.OpenAsync();
        await using var transaction = await connection.BeginTransactionAsync();

        try
        {
            // Get the full transaction details to revert its cash impact.
            // Alias CreatedById as UserId so Dapper maps it to SecurityTransactionLog.UserId.
            const string getOldTxSql = "SELECT *, [CreatedById] AS [UserId] FROM [Klondike].[TransactionLogs] WHERE [Id] = @Id AND [ClientId] = @ClientId";
            var deletedTransaction = await connection.QuerySingleOrDefaultAsync<SecurityTransactionLog>(getOldTxSql, new { record.Id, record.ClientId }, transaction);
            var productSymbol = deletedTransaction?.ProductSymbol;

            // For futures: revert any realized P&L BEFORE the SP hard-deletes the record.
            // The delete SP removes the row from the DB; after that we can no longer query it
            // to compute which P&L was realized when this trade closed the position.
            decimal? newCashBalance = null;
            if (deletedTransaction != null)
            {
                newCashBalance = await RevertFuturesPnLIfClosedAsync(connection, (SqlTransaction)transaction, deletedTransaction, newCashBalance);
            }

            // Execute the delete.
            const string deleteSql = "[Klondike].[deleteTransactionLog]";
            var parameters = new DynamicParameters();
            parameters.Add("@Id", record.Id);
            parameters.Add("@ModifiedById", record.UserId);
            parameters.Add("@ClientId", record.ClientId);
            parameters.Add("@DeletedCount", dbType: DbType.Int32, direction: ParameterDirection.Output);

            await connection.ExecuteAsync(deleteSql, parameters, transaction: transaction, commandType: CommandType.StoredProcedure);
            int result = parameters.Get<int>("@DeletedCount");

            if (result > 0 && !string.IsNullOrEmpty(productSymbol))
            {
                // Revert the cash impact of the deleted trade.
                newCashBalance = await UpdateCashBalanceForSecurityTransactionAsync(connection, (SqlTransaction)transaction, deletedTransaction, revert: true);

                // Recalculate snapshots for the affected product.
                await UpdateSnapshotsForProductAsync(connection, (SqlTransaction)transaction, record.UserId, record.ClientId, productSymbol!);
            }

            await transaction.CommitAsync();
            return new Tuple<int, string?, decimal?>(result, record.Id, newCashBalance);
        }
        catch
        {
            await transaction.RollbackAsync();
            throw;
        }
    }

    public async Task<Tuple<int, string?, decimal?>> LogCashTransaction(CashTransactionLog record)
    {
        if (record == null)
        {
            return new Tuple<int, string?, decimal?>(-1, null, null);
        }

        using var connection = new SqlConnection(this.ConnectionString);
        await connection.OpenAsync();
        await using var transaction = await connection.BeginTransactionAsync();

        try
        {
            const string sql = "[Klondike].[logCashTransaction]";
            var parameters = new DynamicParameters();
            parameters.Add("@Date", record.Date);
            parameters.Add("@OperationId", record.OperationId);
            parameters.Add("@CashCategoryId", record.CashCategoryId);
            parameters.Add("@Amount", record.Amount);
            parameters.Add("@Notes", record.Notes?.Trim());
            parameters.Add("@CreatedById", record.UserId);
            parameters.Add("@ClientId", record.ClientId);
            parameters.Add("@Id", dbType: DbType.String, direction: ParameterDirection.Output, size: 100);
            parameters.Add("@InsertedCount", dbType: DbType.Int32, direction: ParameterDirection.Output);

            await connection.ExecuteAsync(sql, parameters, transaction: transaction, commandType: CommandType.StoredProcedure);

            string? id = parameters.Get<string>("@Id");
            int result = parameters.Get<int>("@InsertedCount");

            decimal? newCashBalance = null;
            if (result == 1)
            {
                newCashBalance = await UpdateCashBalanceForCashTransactionAsync(connection, (SqlTransaction)transaction, record);
            }

            await transaction.CommitAsync();
            string? returnId = result == 1 ? id : null;
            return new Tuple<int, string?, decimal?>(result, returnId, newCashBalance);
        }
        catch
        {
            await transaction.RollbackAsync();
            throw;
        }
    }

    public async Task<Tuple<int, string?, decimal?>> UpdateCashTransactionLog(CashTransactionLog record)
    {
        if (record == null || string.IsNullOrWhiteSpace(record.Id))
        {
            return new Tuple<int, string?, decimal?>(-1, null, null);
        }

        using var connection = new SqlConnection(this.ConnectionString);
        await connection.OpenAsync();
        await using var transaction = await connection.BeginTransactionAsync();

        try
        {
            const string getOldTxSql = "SELECT * FROM [Klondike].[CashTransactionLogs] WHERE [Id] = @Id AND [ClientId] = @ClientId";
            var oldTransaction = await connection.QuerySingleOrDefaultAsync<CashTransactionLog>(getOldTxSql, new { record.Id, record.ClientId }, transaction);

            const string updateSql = "[Klondike].[updateCashTransactionLog]";
            var parameters = new DynamicParameters();
            parameters.Add("@Id", record.Id);
            parameters.Add("@Date", record.Date);
            parameters.Add("@OperationId", record.OperationId);
            parameters.Add("@CashCategoryId", record.CashCategoryId);
            parameters.Add("@Amount", record.Amount);
            parameters.Add("@Notes", record.Notes?.Trim());
            parameters.Add("@ModifiedById", record.UserId);
            parameters.Add("@ClientId", record.ClientId);
            parameters.Add("@UpdatedCount", dbType: DbType.Int32, direction: ParameterDirection.Output);

            await connection.ExecuteAsync(updateSql, parameters, transaction: transaction, commandType: CommandType.StoredProcedure);
            int result = parameters.Get<int>("@UpdatedCount");

            decimal? newCashBalance = null;
            if (result == 1)
            {
                // The compiler warns that oldTransaction could be null.
                if (oldTransaction is not null)
                {
                    await UpdateCashBalanceForCashTransactionAsync(connection, (SqlTransaction)transaction, oldTransaction, revert: true);
                }
                newCashBalance = await UpdateCashBalanceForCashTransactionAsync(connection, (SqlTransaction)transaction, record);
            }

            await transaction.CommitAsync();
            string? returnId = result == 1 ? record.Id : null;
            return new Tuple<int, string?, decimal?>(result, returnId, newCashBalance);
        }
        catch
        {
            await transaction.RollbackAsync();
            throw;
        }
    }

    public async Task<Tuple<int, string?, decimal?>> DeleteCashTransactionLog(CashTransactionLog record)
    {
        if (string.IsNullOrWhiteSpace(record?.Id))
        {
            return new Tuple<int, string?, decimal?>(-1, null, null);
        }

        using var connection = new SqlConnection(this.ConnectionString);
        await connection.OpenAsync();
        await using var transaction = await connection.BeginTransactionAsync();

        try
        {
            const string getOldTxSql = "SELECT * FROM [Klondike].[CashTransactionLogs] WHERE [Id] = @Id AND [ClientId] = @ClientId";
            var deletedTransaction = await connection.QuerySingleOrDefaultAsync<CashTransactionLog>(getOldTxSql, new { record.Id, record.ClientId }, transaction);

            const string deleteSql = "[Klondike].[deleteCashTransactionLog]";
            var parameters = new DynamicParameters();
            parameters.Add("@Id", record.Id);
            parameters.Add("@ModifiedById", record.UserId);
            parameters.Add("@ClientId", record.ClientId);
            parameters.Add("@DeletedCount", dbType: DbType.Int32, direction: ParameterDirection.Output);

            await connection.ExecuteAsync(deleteSql, parameters, transaction: transaction, commandType: CommandType.StoredProcedure);
            int result = parameters.Get<int>("@DeletedCount");

            decimal? newCashBalance = null;
            if (result > 0)
            {
                // Similarly, check if the transaction existed before trying to revert its cash impact.
                if (deletedTransaction is not null)
                {
                    newCashBalance = await UpdateCashBalanceForCashTransactionAsync(connection, (SqlTransaction)transaction, deletedTransaction, revert: true);
                }
            }

            await transaction.CommitAsync();
            return new Tuple<int, string?, decimal?>(result, record.Id, newCashBalance);
        }
        catch
        {
            await transaction.RollbackAsync();
            throw;
        }
    }

    public async Task<string> GetCashTransactionLogs(CashTransactionLog record)
    {
        const string sql = "EXEC [Klondike].[getCashTransactionLogs] @UserId, @ClientId, @StartDate, @EndDate";
        using var connection = new SqlConnection(this.ConnectionString);
        var logs = await connection.QueryAsync(sql, new
        {
            record.UserId,
            record.ClientId,
            record.StartDate,
            record.EndDate
        });
        return JsonConvert.SerializeObject(logs);
    }

    public async Task<string> GetCashTransactionCategories()
    {
        const string sql = "EXEC [Klondike].[getCashTransactionCategories]";
        using var connection = new SqlConnection(this.ConnectionString);
        var categories = await connection.QueryAsync(sql);
        return JsonConvert.SerializeObject(categories);
    }

    public async Task<string> GetCashBalance(CashTransactionLog record)
    {
        const string sql = "EXEC [Klondike].[getCashBalance] @UserId, @ClientId";
        using var connection = new SqlConnection(this.ConnectionString);
        var balance = await connection.QueryAsync(sql, new
        {
            record.UserId,
            record.ClientId
        });
        // The query might return multiple rows if there are multiple currencies.
        // The result is serialized as a JSON array.
        return JsonConvert.SerializeObject(balance);
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
                    WHERE (ps.[UserId] = @UserId) AND (ps.[ClientId] = @ClientId)
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
        var openPositions = (await connection.QueryAsync<OpenPositionViewModel>(sqlQuery,
        new
        {
            record.UserId,
            record.ClientId
        })).ToList();

        if (!openPositions.Any())
        {
            return "[]";
        }

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
        const string sql = "EXEC [Klondike].[getTransactionLogById] @Id, @UserId, @ClientId";
        using var connection = new SqlConnection(this.ConnectionString);
        var log = await connection.QuerySingleOrDefaultAsync(sql, new { record.Id, record.UserId, record.ClientId });
        return log == null ? "[]" : JsonConvert.SerializeObject(new[] { log }); // Return as array for consistency
    }

    public async Task<string> GetCashTransactionLogById(CashTransactionLog record)
    {
        const string sql = "EXEC [Klondike].[getCashTransactionLogById] @Id, @UserId, @ClientId";
        using var connection = new SqlConnection(this.ConnectionString);
        var log = await connection.QuerySingleOrDefaultAsync(sql, new
        {
            record.Id, record.UserId, record.ClientId
        });
        return log == null ? "[]" : JsonConvert.SerializeObject(new[] { log }); // Return as array for consistency
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
                [Quantity], 
                [Price], 
                [Fees] 
        FROM [Klondike].[TransactionLogs]
        WHERE [ProductSymbol] = @ProductSymbol AND [CreatedById] = @UserId AND [ClientID] = @ClientId ORDER BY [Date] ASC;");

        var parameters = new DynamicParameters();
        parameters.Add("ProductSymbol", record.ProductSymbol);
        parameters.Add("UserId", record.UserId);
        parameters.Add("ClientId", record.ClientId);

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
            SELECT [Id], [Date], [OperationId], [ProductCategoryId], [ProductId], [ProductSymbol], [Quantity], [Price], [Fees], [CreatedById] as UserId
            FROM [Klondike].[TransactionLogs]
            WHERE ([ProductSymbol] = @ProductSymbol) AND ([CreatedById] = @UserId) AND ([ClientId] = @ClientId) ORDER BY [Date] ASC;"); // this one really must be ASC ordered

        var parameters = new DynamicParameters();
        parameters.Add("ProductSymbol", record.ProductSymbol);
        parameters.Add("UserId", record.UserId);
        parameters.Add("ClientId", record.ClientId);

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
            WHERE tl.CreatedById = @UserId AND tl.ClientId = @ClientId AND tl.IsDeleted = 0");

        var parameters = new DynamicParameters();
        parameters.Add("UserId", record.UserId);
        parameters.Add("ClientId", record.ClientId);

        if (record.ProductCategoryId > 0) { sqlBuilder.Append(" AND tl.ProductCategoryId = @ProductCategoryId"); parameters.Add("ProductCategoryId", record.ProductCategoryId); }
        if (record.ProductId > 0) { sqlBuilder.Append(" AND tl.ProductId = @ProductId"); parameters.Add("ProductId", record.ProductId); }
        if (!string.IsNullOrEmpty(record.ProductSymbol)) { sqlBuilder.Append(" AND tl.ProductSymbol = @ProductSymbol"); parameters.Add("ProductSymbol", record.ProductSymbol); }
        
        // FIX: Do NOT filter by date in SQL. We need full history to calculate lots correctly.
        // Date filtering must happen AFTER lots are calculated.

        sqlBuilder.Append(" ORDER BY tl.ProductSymbol, tl.Date ASC, tl.Id ASC;");

        using var connection = new SqlConnection(this.ConnectionString);
        var allTransactions = (await connection.QueryAsync<SecurityTransactionLog>(sqlBuilder.ToString(), parameters)).ToList();

        if (!allTransactions.Any())
        {
            return "[]";
        }

        // 2. Calculate P&L using the extracted logic
        var allLots = CalculatePnL(allTransactions);

        // 3. Filter for realized (isClosed) lots
        var realizedLots = allLots.Where(s => s.IsClosed);

        // 4. Apply Date Filtering on the RESULTING lots (based on closing date)
        if (record.StartDate.HasValue)
        {
            realizedLots = realizedLots.Where(r => r.LastTransactionDate >= record.StartDate.Value);
        }
        if (record.EndDate.HasValue)
        {
            realizedLots = realizedLots.Where(r => r.LastTransactionDate < record.EndDate.Value);
        }

        return JsonConvert.SerializeObject(realizedLots.OrderBy(r => r.LastTransactionDate));
    }

    /// <summary>
    /// Pure logic to calculate P&L using FIFO matching.
    /// Each sell (or buy-to-close for shorts) that matches against open positions generates
    /// a realized LotSummary entry immediately, including partial closes.
    /// Fees are amortized proportionally to the matched quantity.
    /// </summary>
    public static List<LotSummary> CalculatePnL(List<SecurityTransactionLog> transactions)
    {
        var results = new List<LotSummary>();

        foreach (var group in transactions.GroupBy(t => t.ProductSymbol))
        {
            var sorted = group.OrderBy(t => t.Date).ThenBy(t => t.Id).ToList();
            // FIFO queue: each entry is an open trade with its remaining unmatched quantity.
            var openPositions = new List<(SecurityTransactionLog Tx, decimal Open)>();

            foreach (var t in sorted)
            {
                decimal netQty = openPositions.Sum(p => p.Tx.OperationId == -1 ? p.Open : -p.Open);
                bool isClosing = openPositions.Count > 0 &&
                                 ((t.OperationId == -1 && netQty < 0) ||  // BUY closes a short
                                  (t.OperationId == 1  && netQty > 0));   // SELL closes a long

                if (!isClosing)
                {
                    openPositions.Add((t, t.Quantity));
                    continue;
                }

                decimal remaining = t.Quantity;

                while (remaining > 0 && openPositions.Count > 0)
                {
                    var (openTx, openQty) = openPositions[0];
                    decimal matched = Math.Min(remaining, openQty);

                    decimal openGrossPerUnit  = GetLotGrossValue(openTx) / openTx.Quantity;
                    decimal closeGrossPerUnit = GetLotGrossValue(t)      / t.Quantity;

                    bool closingLong = t.OperationId == 1;
                    decimal grossPL = closingLong
                        ? (closeGrossPerUnit - openGrossPerUnit) * matched
                        : (openGrossPerUnit  - closeGrossPerUnit) * matched;

                    decimal totalFees = openTx.Fees * (matched / openTx.Quantity)
                                      + t.Fees      * (matched / t.Quantity);

                    results.Add(new LotSummary
                    {
                        ProductSymbol        = t.ProductSymbol!,
                        ProductCategoryId    = t.ProductCategoryId,
                        ProductId            = t.ProductId,
                        FirstTransactionDate = openTx.Date,
                        LastTransactionDate  = t.Date,
                        NetQuantity          = 0,
                        Profit               = grossPL,
                        Fees                 = totalFees,
                        Total                = grossPL - totalFees,
                        IsClosed             = true
                    });

                    if (matched >= openQty)
                        openPositions.RemoveAt(0);
                    else
                        openPositions[0] = (openTx, openQty - matched);

                    remaining -= matched;
                }

                // Quantity that exceeded (or flipped) the open position becomes a new open entry.
                if (remaining > 0)
                    openPositions.Add((t, remaining));
            }

            // Remaining open (unrealized) positions — emitted as IsClosed = false so callers
            // that want to display open lots can still do so; the P&L report filters them out.
            foreach (var (openTx, openQty) in openPositions)
            {
                results.Add(new LotSummary
                {
                    ProductSymbol        = openTx.ProductSymbol!,
                    ProductCategoryId    = openTx.ProductCategoryId,
                    ProductId            = openTx.ProductId,
                    FirstTransactionDate = openTx.Date,
                    LastTransactionDate  = openTx.Date,
                    NetQuantity          = openTx.OperationId == -1 ? openQty : -openQty,
                    Profit               = 0,
                    Fees                 = openTx.Fees * (openQty / openTx.Quantity),
                    Total                = 0,
                    IsClosed             = false
                });
            }
        }

        return results;
    }

    private static decimal GetLotGrossValue(SecurityTransactionLog t)
    {
        if (t.ProductCategoryId == 3 && t.ProductId == 5)
            return t.Quantity * ((Math.Floor(t.Price) + (t.Price - Math.Floor(t.Price)) * 100m / 32m) * 1000m);
        return t.Quantity * t.Price * t.EffectiveContractMultiplier * t.EffectiveCategoryMultiplier;
    }

    public async Task<string> GetTransactionLogs(SecurityTransactionLog record)
    {
        // Logic moved from [Klondike].[getTransactionLogs] stored procedure.
        var sqlBuilder = new StringBuilder(@"
            SELECT [Id], [Date], [OperationId], [ProductCategoryId], [ProductId], [ProductSymbol], [Quantity], [Price], [Fees], [Notes]
            FROM [Klondike].[TransactionLogs]
            WHERE [CreatedById] = @UserId AND [ClientId] = @ClientId");

        var parameters = new DynamicParameters();
        parameters.Add("UserId", record.UserId);
        parameters.Add("ClientId", record.ClientId);

        if (record.ProductCategoryId > 0) { sqlBuilder.Append(" AND [ProductCategoryId] = @ProductCategoryId"); parameters.Add("ProductCategoryId", record.ProductCategoryId); }
        if (record.ProductId > 0) { sqlBuilder.Append(" AND [ProductId] = @ProductId"); parameters.Add("ProductId", record.ProductId); }
        if (!string.IsNullOrEmpty(record.ProductSymbol)) { sqlBuilder.Append(" AND [ProductSymbol] = @ProductSymbol"); parameters.Add("ProductSymbol", record.ProductSymbol); }
        if (record.StartDate.HasValue) { sqlBuilder.Append(" AND [Date] >= @StartDate"); parameters.Add("StartDate", record.StartDate.Value); }
        if (record.EndDate.HasValue) { sqlBuilder.Append(" AND [Date] < @EndDate"); parameters.Add("EndDate", record.EndDate.Value); }

        // The original SP had a special case for ClientId = -1001 to show all clients.
        // This is now handled by the caller not setting a ClientId filter if needed,
        // but we can keep the ordering logic.
        sqlBuilder.Append(" ORDER BY [ProductSymbol], [Date] ASC;");

        using var connection = new SqlConnection(this.ConnectionString);
        var logs = await connection.QueryAsync(sqlBuilder.ToString(), parameters);
        return JsonConvert.SerializeObject(logs);
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
        const string sql = "SELECT DISTINCT [CreatedById], [ClientId], [ProductSymbol] FROM [Klondike].[TransactionLogs] WHERE [IsDeleted] = 0";
        var result = await connection.QueryAsync<(long, long, string)>(sql);
        combos = result.ToList();
        
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
        const string oldProcSql = "[Klondike].[updatePositionSnapshots]";
        await connection.ExecuteAsync(oldProcSql, new { userId, clientId, productSymbol }, commandType: CommandType.StoredProcedure);

        const string getSqlResultsSql = "SELECT * FROM [Klondike].[PositionSnapshots] WHERE ClientId = @ClientId AND ProductSymbol = @ProductSymbol ORDER BY SnapshotDate";
        var sqlResults = await connection.QueryAsync(getSqlResultsSql, new { clientId, productSymbol });
        string expectedJson = JsonConvert.SerializeObject(sqlResults);

        // 2. Run the NEW C# logic and get the results
        await using var transaction = await connection.BeginTransactionAsync();
        await UpdateSnapshotsForProductAsync(connection, (SqlTransaction)transaction, userId, clientId, productSymbol);
        
        // We can read the results before committing because we're in the same session.
        const string getCSharpResultsSql = "SELECT * FROM [Klondike].[PositionSnapshots] WHERE ClientId = @ClientId AND ProductSymbol = @ProductSymbol ORDER BY SnapshotDate";
        var csharpResults = await connection.QueryAsync(getCSharpResultsSql, new { clientId, productSymbol }, transaction);
        string actualJson = JsonConvert.SerializeObject(csharpResults);
        
        await transaction.RollbackAsync(); // Rollback to leave the database unchanged.

        return new Tuple<string, string>(expectedJson, actualJson);
    }

    private async Task<decimal?> UpdateCashBalanceForSecurityTransactionAsync(SqlConnection connection, SqlTransaction transaction, SecurityTransactionLog? record, bool revert = false, string currency = "USD", decimal? startingBalance = null)
    {
        if (record is null) return startingBalance;

        // Step 1: Get multipliers for the product to calculate the correct gross value.
        const string getMultipliersSql = @"
            SELECT ISNULL(p.ContractMultiplier, 1) AS ContractMultiplier, 
                   ISNULL(pc.Multiplier, 1) AS CategoryMultiplier
            FROM [Klondike].[ProductCategories] AS pc
            LEFT JOIN [Klondike].[Products] AS p ON p.ProductCategoryId = pc.Id AND p.Id = @ProductId
            WHERE pc.Id = @ProductCategoryId";

        var multipliers = await connection.QuerySingleOrDefaultAsync<(decimal, decimal)>(
            getMultipliersSql, 
            new { record.ProductCategoryId, record.ProductId }, 
            transaction);

        var (contractMultiplier, categoryMultiplier) = multipliers;
        // Guard: Dapper returns (0,0) when ProductCategoryId has no row in ProductCategories.
        // A multiplier of 0 means grossValue = 0, making SELL decrease cash instead of increase it.
        if (contractMultiplier <= 0) contractMultiplier = 1m;
        if (categoryMultiplier <= 0) categoryMultiplier = 1m;

        // Step 2 & 3: Determine the cash change amount. This is the most critical part.
        decimal amountChange;

        // FUTURES (ProductCategoryId = 3) have a different cash impact than other securities.
        // The initial trade only impacts cash by the fees. The P/L is settled later.
        if (record.ProductCategoryId == 3) 
        {
            amountChange = -record.Fees;
        }
        else
        {
            // For other securities (Equities, Options), the cash impact is the full value of the trade.
            decimal grossValue = record.Quantity * record.Price * contractMultiplier * categoryMultiplier;

            // BUY (OperationId = -1) decreases cash. SELL (OperationId = 1) increases cash.
            amountChange = (record.OperationId == 1 ? grossValue : -grossValue) - record.Fees;
        }

        // If reverting, flip the sign.
        if (revert)
        {
            amountChange = -amountChange;
        }
        
        // If there's no actual change, no need to call the DB.
        if (amountChange == 0) return startingBalance;

        // Step 4: Call the stored procedure to update the cash balance.
        const string updateBalanceSql = "[Klondike].[updateCashBalance]";
        var parameters = new DynamicParameters();
        parameters.Add("@UserId", record.UserId);
        parameters.Add("@ClientId", record.ClientId);
        parameters.Add("@AmountChange", amountChange);
        parameters.Add("@Currency", currency);
        parameters.Add("@NewBalance", dbType: DbType.Decimal, direction: ParameterDirection.Output, precision: 24, scale: 5);

        await connection.ExecuteAsync(updateBalanceSql, parameters, transaction: transaction, commandType: CommandType.StoredProcedure);

        var newBalanceParam = parameters.Get<decimal?>("@NewBalance");

        return newBalanceParam;
    }

    private async Task<decimal?> HandleFuturesPnLRealizationAsync(SqlConnection connection, SqlTransaction transaction, SecurityTransactionLog record, decimal? startingBalance, string currency = "USD")
    {
        // This logic only applies to futures products.
        if (record.ProductCategoryId != 3 || record.ProductSymbol is null)
        {
            return startingBalance;
        }

        // Step 1: Get all transactions for this product to check the position.
        var allTransactions = await GetTransactionsForSnapshotAsync(connection, transaction, record.UserId, record.ClientId, record.ProductSymbol);

        // Step 2: Group transactions into lots.
        var lots = GroupTransactionsIntoLots(allTransactions);
        var lastLot = lots.LastOrDefault();

        if (lastLot == null) return startingBalance;

        // Step 3: Check if the last lot is now closed (net quantity is zero).
        decimal netQuantity = lastLot.Sum(t => t.OperationId == -1 ? t.Quantity : -t.Quantity);

        if (netQuantity == 0)
        {
            // Step 4: The position is closed. Calculate the realized P&L for this lot.
            decimal realizedPL = 0;
            foreach (var t in lastLot)
            {
                // GrossValue calculation, including ZB bond special logic.
                decimal grossValue;
                if (t.ProductCategoryId == 3 && t.ProductId == 5)
                {
                    grossValue = t.Quantity * ((Math.Floor(t.Price) + (t.Price - Math.Floor(t.Price)) * 100m / 32m) * 1000m);
                }
                else
                {
                    grossValue = t.Quantity * t.Price * t.EffectiveContractMultiplier * t.EffectiveCategoryMultiplier;
                }

                // SELL (OpId=1) is positive P/L, BUY (OpId=-1) is negative.
                realizedPL += (t.OperationId == 1 ? grossValue : -grossValue);
            }

            // The fees have already been accounted for trade-by-trade.
            // The amount to change the cash balance by is the gross realized P&L.
            if (realizedPL != 0)
            {
                const string updateBalanceSql = "[Klondike].[updateCashBalance]";
                var parameters = new DynamicParameters();
                parameters.Add("@UserId", record.UserId);
                parameters.Add("@ClientId", record.ClientId);
                parameters.Add("@AmountChange", realizedPL);
                parameters.Add("@Currency", currency);
                parameters.Add("@NewBalance", dbType: DbType.Decimal, direction: ParameterDirection.Output, precision: 24, scale: 5);

                await connection.ExecuteAsync(updateBalanceSql, parameters, transaction: transaction, commandType: CommandType.StoredProcedure);

                return parameters.Get<decimal?>("@NewBalance");
            }
        }

        return startingBalance;
    }

    // Reverses a previously realized futures P&L if the last lot is currently closed.
    // Must be called BEFORE modifying or deleting the transaction so the DB still reflects the old state.
    private async Task<decimal?> RevertFuturesPnLIfClosedAsync(SqlConnection connection, SqlTransaction transaction, SecurityTransactionLog record, decimal? currentBalance, string currency = "USD")
    {
        if (record.ProductCategoryId != 3 || record.ProductSymbol is null) return currentBalance;

        var allTransactions = await GetTransactionsForSnapshotAsync(connection, transaction, record.UserId, record.ClientId, record.ProductSymbol);
        var lots = GroupTransactionsIntoLots(allTransactions);
        var lastLot = lots.LastOrDefault();
        if (lastLot == null) return currentBalance;

        decimal netQuantity = lastLot.Sum(t => t.OperationId == -1 ? t.Quantity : -t.Quantity);
        if (netQuantity != 0) return currentBalance;

        decimal pnlToReverse = 0;
        foreach (var t in lastLot)
        {
            decimal grossValue;
            if (t.ProductCategoryId == 3 && t.ProductId == 5)
            {
                grossValue = t.Quantity * ((Math.Floor(t.Price) + (t.Price - Math.Floor(t.Price)) * 100m / 32m) * 1000m);
            }
            else
            {
                grossValue = t.Quantity * t.Price * t.EffectiveContractMultiplier * t.EffectiveCategoryMultiplier;
            }
            pnlToReverse += (t.OperationId == 1 ? grossValue : -grossValue);
        }

        if (pnlToReverse == 0) return currentBalance;

        const string updateBalanceSql = "[Klondike].[updateCashBalance]";
        var parameters = new DynamicParameters();
        parameters.Add("@UserId", record.UserId);
        parameters.Add("@ClientId", record.ClientId);
        parameters.Add("@AmountChange", -pnlToReverse);
        parameters.Add("@Currency", currency);
        parameters.Add("@NewBalance", dbType: DbType.Decimal, direction: ParameterDirection.Output, precision: 24, scale: 5);

        await connection.ExecuteAsync(updateBalanceSql, parameters, transaction: transaction, commandType: CommandType.StoredProcedure);
        return parameters.Get<decimal?>("@NewBalance");
    }

    private async Task<decimal?> UpdateCashBalanceForCashTransactionAsync(SqlConnection connection, SqlTransaction transaction, CashTransactionLog? record, bool revert = false, string currency = "USD")
    {
        if (record is null) return null;

        // Deposit (OpId=1) increases balance, Withdrawal (OpId=-1) decreases.
        decimal amountChange = record.Amount * record.OperationId;

        if (revert)
        {
            amountChange = -amountChange;
        }

        const string updateBalanceSql = "[Klondike].[updateCashBalance]";
        var parameters = new DynamicParameters();
        parameters.Add("@UserId", record.UserId);
        parameters.Add("@ClientId", record.ClientId);
        parameters.Add("@AmountChange", amountChange);
        parameters.Add("@Currency", currency);
        parameters.Add("@NewBalance", dbType: DbType.Decimal, direction: ParameterDirection.Output, precision: 24, scale: 5);

        await connection.ExecuteAsync(updateBalanceSql, parameters, transaction: transaction, commandType: CommandType.StoredProcedure);
        
        return parameters.Get<decimal?>("@NewBalance");
    }

    private async Task UpdateSnapshotsForProductAsync(SqlConnection connection, SqlTransaction transaction, long userId, long clientId, string productSymbol)
    {
        // Step 1: Fetch all transactions for the product.
        var transactions = await GetTransactionsForSnapshotAsync(connection, transaction, userId, clientId, productSymbol);

        // Step 2: Calculate the new snapshots in C#.
        var snapshots = _snapshotCalculator.Calculate(userId, clientId, productSymbol, transactions);

        // Step 3: Delete the old snapshots for this product.
        const string deleteSql = "DELETE FROM [Klondike].[PositionSnapshots] WHERE ClientId = @ClientId AND ProductSymbol = @ProductSymbol";
        await connection.ExecuteAsync(deleteSql, new { clientId, productSymbol }, transaction);


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
        const string sql = "[Klondike].[getTransactionsForSnapshot]";
        var transactions = await connection.QueryAsync<SecurityTransactionLog>(
            sql, 
            new { userId, clientId, productSymbol }, 
            transaction, 
            commandType: CommandType.StoredProcedure);
        return transactions.ToList();
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
    public static List<List<SecurityTransactionLog>> GroupTransactionsIntoLots(List<SecurityTransactionLog> transactions)
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

    /// <summary>
    /// Executes the stored procedure to create a daily snapshot of all cash balances.
    /// This is intended to be called by a scheduled job.
    /// </summary>
    public async Task CreateDailyCashBalanceSnapshotsAsync()
    {
        using var connection = new SqlConnection(this.ConnectionString);
        const string sql = "[Klondike].[createDailyCashBalanceSnapshots]";
        
        // We pass null for the @SnapshotDate parameter, so the stored procedure
        // will use its default, which is the current UTC date.
        await connection.ExecuteAsync(sql, commandType: CommandType.StoredProcedure);
    }

    string ConnectionString { get; }

} /* end class TransactionLoggerService */

internal static class SqlDataReaderExtensions
{
    public static SecurityTransactionLog? ToSecurityTransactionLog(this SqlDataReader reader)
    {
        if (!reader.HasRows) return null;

        reader.Read(); // Move to the first row

        // Helper to get value or default
        T? GetValue<T>(string columnName)
        {
            var value = reader[columnName];
            return value == DBNull.Value ? default : (T)value;
        }

        return new SecurityTransactionLog
        {
            Id = GetValue<string>("Id"),
            Date = GetValue<DateTime>("Date"),
            OperationId = GetValue<int>("OperationId"),
            ProductCategoryId = GetValue<int>("ProductCategoryId"),
            ProductId = GetValue<int>("ProductId"),
            ProductSymbol = GetValue<string>("ProductSymbol"),
            Quantity = GetValue<int>("Quantity"),
            Price = GetValue<decimal>("Price"),
            Fees = GetValue<decimal>("Fees"),
            UserId = GetValue<long>("CreatedById") // Assuming the original creator is the user context we need
        };
    }

    public static CashTransactionLog? ToCashTransactionLog(this SqlDataReader reader)
    {
        if (!reader.HasRows) return null;

        reader.Read(); // Move to the first row

        T? GetValue<T>(string columnName)
        {
            var value = reader[columnName];
            return value == DBNull.Value ? default : (T)value;
        }

        return new CashTransactionLog
        {
            Id = GetValue<string>("Id"),
            Date = GetValue<DateTime>("Date"),
            OperationId = GetValue<int>("OperationId"),
            CashCategoryId = GetValue<int>("CashCategoryId"),
            Amount = GetValue<decimal>("Amount"),
            UserId = GetValue<long>("CreatedById")
        };
    }
}

} /* end FalxGroup.Finance.Service namespace */