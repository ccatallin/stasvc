using System;
using System.Threading.Tasks;
using FalxGroup.Finance.Service;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace FalxGroup.Finance.Function
{
    public class DailyCashBalanceSnapshotter
    {
        private readonly ILogger _logger;
        private readonly TransactionLoggerService _processor;

        public DailyCashBalanceSnapshotter(ILoggerFactory loggerFactory, TransactionLoggerService processor)
        {
            _logger = loggerFactory.CreateLogger<DailyCashBalanceSnapshotter>();
            _processor = processor;
        }

        /// <summary>
        /// This function runs automatically on a schedule to create daily cash balance snapshots.
        //  The CRON expression "0 5 0 * * *" means "run at 5 minutes past midnight (00:05) UTC every day".
        //  Format is {second} {minute} {hour} {day} {month} {day-of-week}.
        //  For production, it's best to use a configuration setting like "%DailySnapshotSchedule%".
        /// </summary>
        [Function("DailyCashBalanceSnapshotter")]
        public async Task Run([TimerTrigger("%DailySnapshotSchedule%")] TimerInfo myTimer)
        {
            _logger.LogInformation($"C# Timer trigger function executed at: {DateTime.UtcNow}");

            await _processor.CreateDailyCashBalanceSnapshotsAsync();

            _logger.LogInformation($"Successfully created daily cash balance snapshots. Next schedule: {myTimer.ScheduleStatus?.Next}");
        }
    }
}