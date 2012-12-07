using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace TeslaSQL.Agents
{
    /// <summary>
    /// Cleans up old data on the master
    /// </summary>
    class MasterMaintenance : Agent
    {

        //base keyword invokes the base class's constructor
        public MasterMaintenance(Config config, IDataUtils dataUtils) : base(config, dataUtils) {

        }
        
        public override void ValidateConfig()
        {
            Config.ValidateRequiredHost(Config.master);
            if (Config.masterType == null) {
                throw new Exception("MasterMaintenance agent requires a valid SQL flavor for master");
            }
        }

        public override void Run()
        {
            throw new NotImplementedException();
        }
    }
}
