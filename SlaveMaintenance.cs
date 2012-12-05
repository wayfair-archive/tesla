using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace TeslaSQL
{
    class SlaveMaintenance : Agent
    {
        public override void ValidateConfig()
        {
            Config.ValidateRequiredHost(Config.slave);
            if (Config.slaveType == null) {
                throw new Exception("SlaveMaintenance agent requires a valid SQL flavor for slave");
            }
        }

        public override int Run()
        {
            throw new NotImplementedException();
        }
    }
}
