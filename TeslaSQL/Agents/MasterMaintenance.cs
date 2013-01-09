using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using TeslaSQL.DataUtils;
using System.Data;

namespace TeslaSQL.Agents {
    /// <summary>
    /// Cleans up old data on the master
    /// </summary>
    public class MasterMaintenance : Agent {
        //base keyword invokes the base class's constructor
        public MasterMaintenance(IDataUtils dataUtils, IDataUtils destDataUtils, Logger logger)
            : base(dataUtils, destDataUtils, logger) {
        }
        public MasterMaintenance() { }

        public override void ValidateConfig() {
            Config.ValidateRequiredHost(Config.master);
            if (Config.masterType == null) {
                throw new Exception("MasterMaintenance agent requires a valid SQL flavor for master");
            }
            if (Config.masterCTDB == null) {
                throw new Exception("MasterMaintenance agent requires masterCTDB to be set");
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
           var ctids = destDataUtils.GetOldCTIDsMaster(Config.relayDB, chopDate);

            var tables = sourceDataUtils.GetTables(Config.masterCTDB);
            logger.Log("Deleting {" + string.Join(",", ctids) + "} from { " + string.Join(",", tables.Select(t => t.name)) + "}", LogLevel.Debug);
            foreach (var table in tables) {
                int lastUnderscore = table.name.LastIndexOf('_');
                if (lastUnderscore == -1) {
                    continue;
                }
                string end = table.name.Substring(lastUnderscore + 1);

                int tableCtid;
                if (!int.TryParse(end, out tableCtid)) {
                    continue;
                }
                if (ctids.Contains(tableCtid)) {
                    sourceDataUtils.DropTableIfExists(Config.masterCTDB, table.name, table.schema);
                }
                destDataUtils.DeleteOldCTVersionsMaster(Config.masterCTDB, chopDate);
            }
        }
    }
}
