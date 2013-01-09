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
        public MasterMaintenance(IDataUtils dataUtils, Logger logger)
            : base(dataUtils, null, logger) {
        }
        public MasterMaintenance() { }

        public override void ValidateConfig() {
            Config.ValidateRequiredHost(Config.master);
            if (Config.masterType == null) {
                throw new Exception("MasterMaintenance agent requires a valid SQL flavor for master");
            }
            if (Config.masterCTDB == null) {
                throw new Exception("MasterMaintenace agent requires masterCTDB to be set");
            }
            if (Config.batchRecordRetentionDays <= 0) {
                throw new Exception("MasterMaintenace agent requires batchConsolidationThreshold to be set and positive");
            }
            if ( Config.changeRetentionHours <= 0) {
                throw new Exception("MasterMaintenance agent requires changeRetentionHours to be set and positive");
            }
            if (Config.batchRecordRetentionDays * 24 < Config.changeRetentionHours) {
                throw new Exception("Configuration indicates to delete batch records before corresponding tables are deleted, which could lead to data loss in exceptional cases.");
            }
        }

        public override void Run() {
            var chopDate = DateTime.Now - new TimeSpan(Config.changeRetentionHours, 0, 0);
            IEnumerable<int> ctids = sourceDataUtils.GetOldCTIDs(Config.masterCTDB, chopDate, AgentType.MasterMaintenance);

            var tables = sourceDataUtils.GetTables(Config.masterCTDB);
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
                sourceDataUtils.DeleteOldCTVersions(Config.masterCTDB, chopDate, AgentType.MasterMaintenance);
            }
        }
    }
}
