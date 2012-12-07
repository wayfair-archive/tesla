using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace TeslaSQL
{
    class RelayMaintenance : Agent
    {
        public override void ValidateConfig()
        {
            Config.ValidateRequiredHost(Config.relayServer);
            if (Config.relayType == null) {
                throw new Exception("RelayMaintenance agent requires a valid SQL flavor for relay");
            }
        }

        public override void Run()
        {
            throw new NotImplementedException();
        }
    }
}
