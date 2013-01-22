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
            Config.ValidateRequiredHost(Config.relayServer);
            if (Config.masterType == SqlFlavor.None) {
                throw new Exception("MasterMaintenance agent requires a valid SQL flavor for master");
            }
            if (Config.masterCTDB == null) {
                throw new Exception("MasterMaintenance agent requires masterCTDB to be set");
            }
            if (Config.relayType == SqlFlavor.None) {
                throw new Exception("MasterMaintenance agent requires a valid SQL flavor for relay");
            }
            if (Config.relayDB == null) {
                throw new Exception("MasterMaintenance agent requires relayDB to be set");
            }
            if (Config.changeRetentionHours <= 0) {
                throw new Exception("MasterMaintenance agent requires changeRetentionHours to be set and positive");
            }
        }

        public override void Run() {
            var chopDate = DateTime.Now - new TimeSpan(Config.changeRetentionHours, 0, 0);
            var ctids = destDataUtils.GetOldCTIDsMaster(Config.relayDB, chopDate);

            var tables = sourceDataUtils.GetTables(Config.masterCTDB);
            logger.Log("Deleting {" + string.Join(",", ctids) + "} from { " + string.Join(",", tables.Select(t => t.name)) + "}", LogLevel.Debug);
            MaintenanceHelper.DeleteOldTables(ctids, tables, sourceDataUtils, Config.masterCTDB);
        }
    }
}
