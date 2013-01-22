using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using TeslaSQL.DataUtils;

namespace TeslaSQL.Agents {
    /// <summary>
    /// This class is used for cleaning up old data on the slave (such as changetables for old batches)
    /// </summary>
    class SlaveMaintenance : Agent {
        private IDataUtils relayDataUtils { get { return sourceDataUtils; } }
        private IDataUtils slaveDataUtils { get { return destDataUtils; } }
        //base keyword invokes the base class's constructor
        public SlaveMaintenance(IDataUtils relayDataUtils, IDataUtils slaveDataUtils, Logger logger)
            : base(relayDataUtils, slaveDataUtils, logger) {
        }

        public override void ValidateConfig() {
            Config.ValidateRequiredHost(Config.Slave);
            if (Config.SlaveType == SqlFlavor.None) {
                throw new Exception("SlaveMaintenance agent requires a valid SQL flavor for slave");
            }
        }
        public override void Run() {
            var chopDate = DateTime.Now - new TimeSpan(Config.ChangeRetentionHours, 0, 0);
            IEnumerable<long> CTIDs = relayDataUtils.GetOldCTIDsSlave(Config.RelayDB, chopDate, Config.Slave);
            var tables = slaveDataUtils.GetTables(Config.SlaveCTDB);
            logger.Log("Deleting {" + string.Join(",", CTIDs) + "} from { " + string.Join(",", tables.Select(t => t.name)) + "}", LogLevel.Debug);
            MaintenanceHelper.DeleteOldTables(CTIDs, tables, slaveDataUtils, Config.SlaveCTDB);
        }
    }
}
