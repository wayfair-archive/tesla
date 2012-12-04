using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace TeslaSQL
{
    class MasterMaintenance : Agent
    {
        public override void ValidateConfig()
        {
            Config.ValidateRequiredHost(Config.master);
            if (Config.masterType == null) {
                throw new Exception("MasterMaintenance agent requires a valid SQL flavor for master");
            }
        }

        public override int Run()
        {
            throw new NotImplementedException();
        }
    }
}
