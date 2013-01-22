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
            Config.ValidateRequiredHost(Config.relayServer);
            if (Config.relayType == SqlFlavor.None) {
                throw new Exception("RelayMaintenance agent requires a valid SQL flavor for relay");
            }
            if (string.IsNullOrEmpty(Config.relayDB)) {
                throw new Exception("RelayMaintenance agent requires a valid relayDB");
            }
            if (Config.batchRecordRetentionDays <= 0) {
                throw new Exception("MasterMaintenance agent requires batchConsolidationThreshold to be set and positive");
            }
            if (Config.changeRetentionHours <= 0) {
                throw new Exception("MasterMaintenance agent requires changeRetentionHours to be set and positive");
            }
            if (Config.batchRecordRetentionDays * 24 < Config.changeRetentionHours) {
                throw new Exception("Configuration indicates to delete batch records before corresponding tables are deleted, which could lead to data loss in exceptional cases.");
            }
        }

        public override void Run() {
            var chopDate = DateTime.Now - new TimeSpan(Config.changeRetentionHours, 0, 0);
            var rowChopDate = DateTime.Now - new TimeSpan(Config.batchRecordRetentionDays, 0, 0, 0);
            IEnumerable<long> ctids = relayDataUtils.GetOldCTIDsRelay(Config.relayDB, chopDate);
            IEnumerable<string> allDbs = new List<string> { Config.relayDB };
            if (Config.shardDatabases != null) {
                allDbs = allDbs.Concat(Config.shardDatabases);
            }
            foreach (string db in allDbs) {
                var tables = relayDataUtils.GetTables(db);
                logger.Log("Deleting {" + string.Join(",", ctids) + "} from { " + string.Join(",", tables.Select(t => t.name)) + "}", LogLevel.Debug);
                MaintenanceHelper.DeleteOldTables(ctids, tables, relayDataUtils, db);
                relayDataUtils.DeleteOldCTVersions(db, rowChopDate);
            }
            relayDataUtils.DeleteOldCTSlaveVersions(Config.relayDB, rowChopDate);
        }
    }
}
