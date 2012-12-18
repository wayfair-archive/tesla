using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using TeslaSQL.DataUtils;

namespace TeslaSQL.Agents {
    /// <summary>
    /// This agent consolidates data from different shards so that slaves see a unified database
    /// </summary>
    class ShardCoordinator : Agent {
        IList<string> shardDatabases;
        public ShardCoordinator(Config config, IDataUtils dataUtils)
            : base(config, dataUtils, null) {

        }

        public override void ValidateConfig() {
            Config.ValidateRequiredHost(config.relayServer);
            if (config.relayType == null) {
                throw new Exception("ShardCoordinator agent requires a valid SQL flavor for relay");
            }
        }

        public override void Run() {
            var batch = new ChangeTrackingBatch(sourceDataUtils.GetLastCTBatch(config.relayDB, AgentType.ShardCoordinator));
            if ((batch.syncBitWise & Convert.ToInt32(SyncBitWise.UploadChanges)) > 0) {
                sourceDataUtils.CreateCTVersion(config.relayDB, batch.syncStopVersion, batch.syncStopVersion + 1);
                foreach (var db in shardDatabases) {
                    sourceDataUtils.CreateCTVersion(db, batch.syncStopVersion, batch.syncStopVersion + 1);
                }
            } else {
                foreach (var t in config.tables) {
                    var columnsByTable = shardDatabases.Select(sd => sourceDataUtils.GetFieldList(sd, t.Name, t.schemaName).Keys);
                    if (columnsByTable.Distinct().Count() > 1) {
                        //go tell all the masters that they need to start over
                    }
                }
                if (shardDatabases.All(dbName => (sourceDataUtils.GetCTBatch(dbName, batch.CTID).CTID & Convert.ToInt32(SyncBitWise.UploadChanges)) > 0)) {
                    Consolidate(batch);
                    //sourceDataUtils.MarkBatchComplete(config.relayDB, batch.CTID,
                    //    Convert.ToInt32(SyncBitWise.CaptureChanges) | Convert.ToInt32(SyncBitWise.UploadChanges), DateTime.Now, AgentType.ShardCoordinator);
                }
            }
        }

        private void Consolidate(ChangeTrackingBatch batch) {
            if ((batch.syncBitWise & Convert.ToInt32(SyncBitWise.PublishSchemaChanges)) > 0) {
                ApplySchemaChanges(config.tables, shardDatabases.First(), config.relayDB, batch.CTID);
            }
            ConsolidateTables(batch);
            ConsolidateInfoTables(batch);
        }

        private void ConsolidateInfoTables(ChangeTrackingBatch batch) {
            throw new NotImplementedException();
        }

        private void ConsolidateTables(ChangeTrackingBatch batch) {
            throw new NotImplementedException();
        }
    }
}
