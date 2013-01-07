using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using TeslaSQL.DataUtils;

namespace TeslaSQL.Agents
{
    /// <summary>
    /// Cleans up old data on the relay server
    /// </summary>
    class RelayMaintenance : Agent
    {
        //base keyword invokes the base class's constructor
        public RelayMaintenance(IDataUtils dataUtils)
            : base(dataUtils, null, null) {

        }

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
