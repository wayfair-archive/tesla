using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using TeslaSQL.DataUtils;

namespace TeslaSQL.Agents {
    /// <summary>
    /// Cleans up old data on the relay server
    /// </summary>
    class RelayMaintenance : Agent {
        private IDataUtils relayDataUtils { get { return this.sourceDataUtils; } }
        public RelayMaintenance(IDataUtils dataUtils, Logger logger)
            : base(dataUtils, null, logger) {
        }

        public override void ValidateConfig() {
            Config.ValidateRequiredHost(Config.RelayServer);
            if (Config.RelayType == SqlFlavor.None) {
                throw new Exception("RelayMaintenance agent requires a valid SQL flavor for relay");
            }
            if (string.IsNullOrEmpty(Config.RelayDB)) {
                throw new Exception("RelayMaintenance agent requires a valid relayDB");
            }
            if (Config.batchRecordRetentionDays <= 0) {
                throw new Exception("MasterMaintenance agent requires batchConsolidationThreshold to be set and positive");
            }
            if (Config.ChangeRetentionHours <= 0) {
                throw new Exception("MasterMaintenance agent requires changeRetentionHours to be set and positive");
            }
            if (Config.batchRecordRetentionDays * 24 < Config.ChangeRetentionHours) {
                throw new Exception("Configuration indicates to delete batch records before corresponding tables are deleted, which could lead to data loss in exceptional cases.");
            }
        }

        public override void Run() {
            var chopDate = DateTime.Now - new TimeSpan(Config.ChangeRetentionHours, 0, 0);
            var rowChopDate = DateTime.Now - new TimeSpan(Config.batchRecordRetentionDays, 0, 0, 0);
            IEnumerable<long> CTIDs = relayDataUtils.GetOldCTIDsRelay(Config.RelayDB, chopDate);
            IEnumerable<string> allDbs = new List<string> { Config.RelayDB };
            if (Config.ShardDatabases != null) {
                allDbs = allDbs.Concat(Config.ShardDatabases);
            }
            foreach (string db in allDbs) {
                var tables = relayDataUtils.GetTables(db);
                if (tables.Count() > 0) {
                    logger.Log("Deleting {" + string.Join(",", CTIDs) + "} from { " + string.Join(",", tables.Select(t => t.name)) + "}", LogLevel.Info);
                    MaintenanceHelper.DeleteOldTables(CTIDs, tables, relayDataUtils, db);
                } else {
                    logger.Log("No tables to delete for database " + db, LogLevel.Info);
                }
                relayDataUtils.DeleteOldCTVersions(db, rowChopDate);
            }
            relayDataUtils.DeleteOldCTSlaveVersions(Config.RelayDB, rowChopDate);
        }
    }
}
