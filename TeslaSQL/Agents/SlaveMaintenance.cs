using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace TeslaSQL.Agents
{
    /// <summary>
    /// This class is used for cleaning up old data on the slave (such as changetables for old batches)
    /// </summary>
    class SlaveMaintenance : Agent
    {
        //base keyword invokes the base class's constructor
        public SlaveMaintenance(Config config, IDataUtils dataUtils) : base(config, dataUtils) {

        }

        public override void ValidateConfig()
        {
            config.ValidateRequiredHost(config.slave);
            if (config.slaveType == null) {
                throw new Exception("SlaveMaintenance agent requires a valid SQL flavor for slave");
            }
        }

        public override void Run()
        {
            throw new NotImplementedException();
        }
    }
}
