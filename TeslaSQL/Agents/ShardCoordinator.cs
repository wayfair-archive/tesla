using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace TeslaSQL.Agents
{
    /// <summary>
    /// This agent consolidates data from different shards so that slaves see a unified database
    /// </summary>
    class ShardCoordinator : Agent
    {
        //base keyword invokes the base class's constructor
        public ShardCoordinator(Config config, IDataUtils dataUtils) : base(config, dataUtils) {

        }

        public override void ValidateConfig()
        {
            config.ValidateRequiredHost(config.relayServer);
            if (config.relayType == null) {
                throw new Exception("ShardCoordinator agent requires a valid SQL flavor for relay");
            }
        }

        public override void Run()
        {
            throw new NotImplementedException();
        }
    }
}
