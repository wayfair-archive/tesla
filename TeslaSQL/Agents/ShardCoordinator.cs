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
        public ShardCoordinator(IDataUtils dataUtils, Logger logger)
            : base(dataUtils, dataUtils, logger) {
            shardDatabases = Config.ShardDatabases;
            tablesWithChanges = new List<TableConf>();
        }

        public ShardCoordinator() {
            //this constructor is only used by for running unit tests
            this.logger = new Logger(null, null, null, "");
        }

        public override void ValidateConfig() {
            Config.ValidateRequiredHost(Config.RelayServer);
            if (Config.RelayType == SqlFlavor.None) {
                throw new Exception("ShardCoordinator agent requires a valid SQL flavor for relay");
            }
            if (string.IsNullOrEmpty(Config.MasterShard)) {
                throw new Exception("ShardCoordinator agent requires a master shard");
            }
            if (!Config.ShardDatabases.Contains(Config.MasterShard)) {
                throw new Exception("ShardCoordinator agent requires that the masterShard element be one of the shards listed in shardDatabases");
            }
        }

        public override void Run() {
            var batch = new ChangeTrackingBatch(sourceDataUtils.GetLastCTBatch(Config.RelayDB, AgentType.ShardCoordinator));
            Logger.SetProperty("CTID", batch.CTID);
            if ((batch.SyncBitWise & Convert.ToInt32(SyncBitWise.UploadChanges)) > 0) {
                CreateNewVersionsForShards(batch);
                return;
            }
            logger.Log("Working on CTID " + batch.CTID, LogLevel.Info);

            if (AllShardMastersDone(batch)) {
                logger.Log("All shard masters are done, checking field lists", LogLevel.Info);
                tableDBFieldLists = GetFieldListsByDB(batch.CTID);
                if (SchemasOutOfSync(tableDBFieldLists.Values)) {
                    foreach (var sd in shardDatabases) {
                        sourceDataUtils.RevertCTBatch(sd, batch.CTID);
                    }
                    logger.Log("Schemas out of sync, quitting", LogLevel.Info);
                    return;
                }
                logger.Log("Field lists in sync, consolidating", LogLevel.Info);
                Consolidate(batch);
                sourceDataUtils.WriteBitWise(Config.RelayDB, batch.CTID,
                    Convert.ToInt32(SyncBitWise.CaptureChanges) | Convert.ToInt32(SyncBitWise.UploadChanges), AgentType.ShardCoordinator);
                //now that the current batch is done, create a new one for the masters to work on
                CreateNewVersionsForShards(batch);
            } else {
                logger.Log("Not all shards are done yet, waiting until they catch up", LogLevel.Info);
            }

        }

        private bool AllShardMastersDone(ChangeTrackingBatch batch) {
            return shardDatabases.All(dbName => (sourceDataUtils.GetCTBatch(dbName, batch.CTID).SyncBitWise & Convert.ToInt32(SyncBitWise.UploadChanges)) > 0);
        }
        /// <param name="dbFieldLists">a list of maps from dbName to list of TColumns. 
        /// This is a list (not just a dict) because there needs to be one dict per table. </param>
        /// <returns></returns>
        // virtual so i can unit test it.
        virtual internal bool SchemasOutOfSync(IEnumerable<Dictionary<string, List<TColumn>>> dbFieldLists) {
            foreach (var dbFieldList in dbFieldLists) {
                var orderedFieldLists = dbFieldList.Values.Select(lc => lc.OrderBy(c => c.name));
                bool schemaOutOfSync = orderedFieldLists.Any(ofc => !ofc.SequenceEqual(orderedFieldLists.First()));
                if (schemaOutOfSync) {
                    return true;
                }
            }
            return false;
        }

        protected Dictionary<TableConf, Dictionary<string, List<TColumn>>> GetFieldListsByDB(Int64 CTID) {
            var fieldListByDB = new Dictionary<TableConf, Dictionary<string, List<TColumn>>>();
            foreach (var table in Config.Tables) {
                var tDict = new Dictionary<string, List<TColumn>>();
                foreach (var sd in shardDatabases) {
                    logger.Log("GetFieldList for db " + sd, LogLevel.Debug);
                    //only add the columns if we get results. it's perfectly legitimate for a changetable to not exist for a given shard
                    //if it had no changes, and we don't want that to cause the schemas to be considered out of sync
                    var columns = sourceDataUtils.GetFieldList(sd, table.ToCTName(CTID), table.SchemaName);
                    if (columns.Count > 0) {
                        tDict[sd] = columns;
                    }
                }
                fieldListByDB[table] = tDict;
            }
            return fieldListByDB;
        }

        private ChangeTrackingBatch CreateNewVersionsForShards(ChangeTrackingBatch batch) {
            logger.Log("Creating new CT versions for slaves", LogLevel.Info);
            Int64 CTID = sourceDataUtils.CreateCTVersion(Config.RelayDB, 0, 0).CTID;
            Logger.SetProperty("CTID", CTID);
            foreach (var db in shardDatabases) {
                var b = new ChangeTrackingBatch(sourceDataUtils.GetLastCTBatch(db, AgentType.ShardCoordinator));
                sourceDataUtils.CreateShardCTVersion(db, CTID, b.SyncStopVersion);
            }
            logger.Log("Created new CT Version " + CTID + " on " + string.Join(",", shardDatabases), LogLevel.Info);
            batch = new ChangeTrackingBatch(CTID, 0, 0, 0);
            return batch;
        }

        private void Consolidate(ChangeTrackingBatch batch) {
            if ((batch.SyncBitWise & Convert.ToInt32(SyncBitWise.PublishSchemaChanges)) == 0) {
                logger.Log("Publishing schema changes", LogLevel.Debug);
                PublishSchemaChanges(batch);
                sourceDataUtils.WriteBitWise(Config.RelayDB, batch.CTID, Convert.ToInt32(SyncBitWise.PublishSchemaChanges), AgentType.ShardCoordinator);
            }
            ConsolidateTables(batch);
            ConsolidateInfoTables(batch);
        }

        private void PublishSchemaChanges(ChangeTrackingBatch batch) {
            var dc = DataCopyFactory.GetInstance(Config.RelayType, Config.RelayType, sourceDataUtils, sourceDataUtils, logger);
            dc.CopyTable(Config.MasterShard, batch.schemaChangeTable, "dbo", Config.RelayDB, Config.DataCopyTimeout);
        }

        private void ConsolidateTables(ChangeTrackingBatch batch) {
            logger.Log("Consolidating tables", LogLevel.Info);
            var actions = new List<Action>();
            foreach (var tableDb in tableDBFieldLists) {
                var table = tableDb.Key;
                var dbColumns = tableDb.Value;
                var firstDB = tableDb.Value.FirstOrDefault(t => t.Value.Count > 0).Key;
                if (firstDB == null) {
                    logger.Log("No shard has CT changes for table " + table.Name, LogLevel.Debug);
                    continue;
                }
                tablesWithChanges.Add(table);
                SetFieldList(table, firstDB, batch);

                Action act = () => MergeTable(batch, dbColumns, table, firstDB);
                actions.Add(act);
            }
            logger.Log("Parallel invocation of " + actions.Count + " table merges", LogLevel.Info);
            //interestingly, Parallel.Invoke does in fact bubble up exceptions, but not until after all threads have completed.
            //actually it looks like what it does is wrap its exceptions in an AggregateException. We don't ever catch those
            //though because if any exceptions happen inside of MergeTable it would generally be due to things like the server
            //being down or a query timeout.
            var options = new ParallelOptions();
            options.MaxDegreeOfParallelism = Config.MaxThreads;
            Parallel.Invoke(options, actions.ToArray());
        }

        private void MergeTable(ChangeTrackingBatch batch, Dictionary<string, List<TColumn>> dbColumns, TableConf table, string firstDB) {
            logger.Log(new { message = "Merging table", Table = table.Name }, LogLevel.Debug);
            var dc = DataCopyFactory.GetInstance(Config.RelayType, Config.RelayType, sourceDataUtils, sourceDataUtils, logger);
            dc.CopyTableDefinition(firstDB, table.ToCTName(batch.CTID), table.SchemaName, Config.RelayDB, table.ToCTName(batch.CTID));
            foreach (var dbNameFields in dbColumns) {
                var dbName = dbNameFields.Key;
                var columns = dbNameFields.Value;
                if (columns.Count == 0) {
                    //no changes in this DB for this table
                    continue;
                }
                sourceDataUtils.MergeCTTable(table, Config.RelayDB, dbName, batch.CTID);
            }
        }

        private void ConsolidateInfoTables(ChangeTrackingBatch batch) {
            logger.Log("Consolidating info tables", LogLevel.Debug);
            var rowCounts = GetRowCounts(Config.Tables, Config.RelayDB, batch.CTID);
            //publish table info with actual rowcounts for the tables that had changes
            PublishTableInfo(tablesWithChanges, Config.RelayDB, rowCounts, batch.CTID);
            //pull in the table info for tables that didn't have changes from the shard databases
            //start with the master shard database first because it is assumed to have the 
            //most up to date schema information
            foreach (var sd in shardDatabases.OrderBy(d => d == Config.MasterShard ? 0 : 1)) {
                sourceDataUtils.MergeInfoTable(sd, Config.RelayDB, batch.CTID);
            }
        }

        private void SetFieldList(TableConf table, string database, ChangeTrackingBatch batch) {
            var cols = sourceDataUtils.GetFieldList(database, table.ToCTName(batch.CTID), table.SchemaName);
            var pks = sourceDataUtils.GetPrimaryKeysFromInfoTable(table, batch.CTID, database);
            foreach (var pk in pks) {
                cols.First((c => c.name == pk)).isPk = true;
            }
            SetFieldList(table, cols);
        }
    }
}
