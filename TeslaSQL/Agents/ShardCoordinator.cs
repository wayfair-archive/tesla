using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using TeslaSQL.DataUtils;
using TeslaSQL.DataCopy;
using System.Threading;
using System.Threading.Tasks;

namespace TeslaSQL.Agents {
    /// <summary>
    /// This agent consolidates data from different shards so that slaves see a unified database
    /// </summary>
    public class ShardCoordinator : Agent {
        protected IEnumerable<string> shardDatabases;
        IList<TableConf> tablesWithChanges;
        Dictionary<TableConf, Dictionary<string, List<TColumn>>> tableDBFieldLists;
        public ShardCoordinator(Config config, IDataUtils dataUtils)
            : base(config, dataUtils, dataUtils) {
            shardDatabases = config.shardDatabases;
            tablesWithChanges = new List<TableConf>();
        }

        public ShardCoordinator() {
            //this constructor is only used by for running unit tests
            this.logger = new Logger(LogLevel.Critical, null, null, null);
        }

        public override void ValidateConfig() {
            Config.ValidateRequiredHost(config.relayServer);
            if (config.relayType == null) {
                throw new Exception("ShardCoordinator agent requires a valid SQL flavor for relay");
            }
            if (string.IsNullOrEmpty(config.masterShard)) {
                throw new Exception("ShardCoordinator agent requires a master shard");
            }
            if (!config.shardDatabases.Contains(config.masterShard)) {
                throw new Exception("ShardCoordinator agent requires that the masterShard element be one of the shards listed in shardDatabases");
            }
        }

        public override void Run() {
            var batch = new ChangeTrackingBatch(sourceDataUtils.GetLastCTBatch(config.relayDB, AgentType.ShardCoordinator));
            if ((batch.syncBitWise & Convert.ToInt32(SyncBitWise.UploadChanges)) > 0) {
                logger.Log("Creating new CT versions for slaves", LogLevel.Info);
                batch = CreateNewVersionsForShards(batch);
            } else {
                tableDBFieldLists = GetFieldListsByDB(batch.CTID);
                if (SchemasOutOfSync(batch, tableDBFieldLists.Values)) {
                    foreach (var sd in shardDatabases) {
                        sourceDataUtils.RevertCTBatch(sd, batch.CTID);
                    }
                    logger.Log("Schemas out of sync, quitting", LogLevel.Info);
                    return;
                }
                if (shardDatabases.All(dbName => (sourceDataUtils.GetCTBatch(dbName, batch.CTID).syncBitWise & Convert.ToInt32(SyncBitWise.UploadChanges)) > 0)) {
                    Consolidate(batch);
                    sourceDataUtils.WriteBitWise(config.relayDB, batch.CTID,
                        Convert.ToInt32(SyncBitWise.CaptureChanges) | Convert.ToInt32(SyncBitWise.UploadChanges), AgentType.ShardCoordinator);
                } else {
                    logger.Log("Not all shards are done yet, waiting until they catch up", LogLevel.Info);
                }
            }
        }
        /// <param name="dbFieldLists">a list of maps from dbName to list of TColumns. 
        /// This is a list (not just a dict) because there needs to be one dict per table. </param>
        /// <returns></returns>
        // virtual so i can unit test it.
        virtual internal bool SchemasOutOfSync(ChangeTrackingBatch batch, IEnumerable<Dictionary<string, List<TColumn>>> dbFieldLists) {
            foreach (var dbFieldList in dbFieldLists) {
                var orderedFieldLists = dbFieldList.Values.Select(lc => lc.OrderBy(c => c.name));
                bool schemaOutOfSync = orderedFieldLists.Any(ofc => !ofc.SequenceEqual(orderedFieldLists.First()));
                if (schemaOutOfSync) {
                    return true;
                }
            }
            return false;
        }

        protected Dictionary<TableConf, Dictionary<string, List<TColumn>>> GetFieldListsByDB(Int64 ctid) {
            var fieldListByDB = new Dictionary<TableConf, Dictionary<string, List<TColumn>>>();
            foreach (var table in config.tables) {
                var tDict = new Dictionary<string, List<TColumn>>();
                foreach (var sd in shardDatabases) {
                    tDict[sd] = sourceDataUtils.GetFieldList(sd, table.ToCTName(ctid), table.schemaName).Select(kvp => new TColumn(kvp.Key, kvp.Value)).ToList();
                }
                fieldListByDB[table] = tDict;
            }
            return fieldListByDB;
        }

        private ChangeTrackingBatch CreateNewVersionsForShards(ChangeTrackingBatch batch) {
            Int64 ctid = sourceDataUtils.CreateCTVersion(config.relayDB, 0, 0);
            foreach (var db in shardDatabases) {
                var b = new ChangeTrackingBatch(sourceDataUtils.GetLastCTBatch(db, AgentType.ShardCoordinator));
                sourceDataUtils.CreateShardCTVersion(db, ctid, b.syncStopVersion);
            }
            batch = new ChangeTrackingBatch(ctid, 0, 0, 0);
            return batch;
        }

        private void Consolidate(ChangeTrackingBatch batch) {
            if ((batch.syncBitWise & Convert.ToInt32(SyncBitWise.PublishSchemaChanges)) == 0) {
                PublishSchemaChanges(batch);
                sourceDataUtils.WriteBitWise(config.relayDB, batch.CTID, Convert.ToInt32(SyncBitWise.PublishSchemaChanges), AgentType.ShardCoordinator);
            }
            ConsolidateTables(batch);
            ConsolidateInfoTables(batch);
        }

        private void PublishSchemaChanges(ChangeTrackingBatch batch) {
            var dc = DataCopyFactory.GetInstance(config.relayType.Value, config.relayType.Value, sourceDataUtils, sourceDataUtils);
            dc.CopyTable(config.masterShard, batch.schemaChangeTable, "dbo", config.relayDB, 3600);
        }

        private void ConsolidateTables(ChangeTrackingBatch batch) {
            var actions = new List<Action>();
            foreach (var tableDb in tableDBFieldLists) {
                var table = tableDb.Key;
                var firstDB = tableDb.Value.FirstOrDefault(t => t.Value.Count > 0).Key;
                if (firstDB == null) {
                    logger.Log("No shard has CT changes for table " + table.Name, LogLevel.Debug);
                    continue;
                }
                tablesWithChanges.Add(table);
                SetFieldList(table, firstDB, batch);

                Action act = () => MergeTable(batch, tableDb.Value, table, firstDB);
                actions.Add(act);
            }
            logger.Log("Parallel invocation of " + actions.Count + " table merges", LogLevel.Trace);
            //interestingly, Parallel.Invoke does in fact bubble up exceptions, but not until after all threads have completed.
            //actually it looks like what it does is wrap its exceptions in an AggregateException.
            Parallel.Invoke(actions.ToArray());
        }

        private void MergeTable(ChangeTrackingBatch batch, Dictionary<string, List<TColumn>> dbColumns, TableConf table, string firstDB) {
            var dc = DataCopyFactory.GetInstance(config.relayType.Value, config.relayType.Value, sourceDataUtils, sourceDataUtils);
            dc.CopyTableDefinition(firstDB, table.ToCTName(batch.CTID), table.schemaName, config.relayDB, table.ToCTName(batch.CTID));
            foreach (var dbNameFields in dbColumns) {
                var dbName = dbNameFields.Key;
                var columns = dbNameFields.Value;
                if (columns.Count == 0) {
                    //no changes in this DB for this table
                    continue;
                }
                sourceDataUtils.MergeCTTable(table, config.relayDB, dbName, batch.CTID);
            }
        }

        private void ConsolidateInfoTables(ChangeTrackingBatch batch) {
            var rowCounts = GetRowCounts(config.tables, config.relayDB, batch.CTID);
            PublishTableInfo(tablesWithChanges, config.relayDB, rowCounts, batch.CTID);
        }

        private void SetFieldList(TableConf table, string database, ChangeTrackingBatch batch) {
            var cols = sourceDataUtils.GetFieldList(database, table.ToCTName(batch.CTID), table.schemaName);
            var pks = sourceDataUtils.GetPrimaryKeysFromInfoTable(table, batch, database);
            foreach (var pk in pks) {
                cols[pk] = true;
            }
            SetFieldList(table, cols);
        }
    }
}
