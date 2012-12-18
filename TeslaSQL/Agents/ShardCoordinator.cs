using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using TeslaSQL.DataUtils;
using TeslaSQL.DataCopy;

namespace TeslaSQL.Agents {
    /// <summary>
    /// This agent consolidates data from different shards so that slaves see a unified database
    /// </summary>
    class ShardCoordinator : Agent {
        IList<string> shardDatabases;
        Dictionary<TableConf, Dictionary<string, List<TColumn>>> tableDBFieldLists;
        public ShardCoordinator(Config config, IDataUtils dataUtils)
            : base(config, dataUtils, dataUtils) {

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
                batch = CreateNewVersionsForShards(batch);
            } else {
                tableDBFieldLists = GetFieldListsByDB();
                foreach (var dbFieldLists in tableDBFieldLists.Values) {
                    var orderedFieldLists = dbFieldLists.Values.Select(lc => lc.OrderBy(c => c.name));
                    bool schemaOutOfSync = orderedFieldLists.Any(ofc => !ofc.SequenceEqual(orderedFieldLists.First()));
                    if (schemaOutOfSync) {
                        foreach (var sd in shardDatabases) {
                            sourceDataUtils.RevertCTBatch(sd, batch.CTID);
                        }
                        return;
                    }
                }
                if (shardDatabases.All(dbName => (sourceDataUtils.GetCTBatch(dbName, batch.CTID).CTID & Convert.ToInt32(SyncBitWise.UploadChanges)) > 0)) {
                    Consolidate(batch);
                    sourceDataUtils.WriteBitWise(config.relayDB, batch.CTID,
                        Convert.ToInt32(SyncBitWise.CaptureChanges) | Convert.ToInt32(SyncBitWise.UploadChanges), AgentType.ShardCoordinator);
                }
            }
        }

        private Dictionary<TableConf, Dictionary<string, List<TColumn>>> GetFieldListsByDB() {
            var fieldListByDB = new Dictionary<TableConf, Dictionary<string, List<TColumn>>>();
            foreach (var table in config.tables) {
                var tDict = new Dictionary<string, List<TColumn>>();
                foreach (var sd in shardDatabases) {
                    tDict[sd] = sourceDataUtils.GetFieldList(sd, table.Name, table.schemaName).Select(kvp => new TColumn(kvp.Key, kvp.Value)).ToList();
                }
                fieldListByDB[table] = tDict;
            }
            return fieldListByDB;
        }

        private ChangeTrackingBatch CreateNewVersionsForShards(ChangeTrackingBatch batch) {
            sourceDataUtils.CreateCTVersion(config.relayDB, batch.syncStopVersion, batch.syncStopVersion + 1);
            foreach (var db in shardDatabases) {
                Int64 ctid = sourceDataUtils.CreateCTVersion(db, batch.syncStopVersion, batch.syncStopVersion + 1);
                batch = new ChangeTrackingBatch(ctid, batch.syncStopVersion, batch.syncStopVersion + 1, 0);
            }
            return batch;
        }

        private void Consolidate(ChangeTrackingBatch batch) {
            if ((batch.syncBitWise & Convert.ToInt32(SyncBitWise.PublishSchemaChanges)) > 0) {
                ApplySchemaChanges(config.tables, shardDatabases.First(), config.relayDB, batch.CTID);
            }
            ConsolidateTables(batch);
            ConsolidateInfoTables(batch);
        }

        private void ConsolidateInfoTables(ChangeTrackingBatch batch) {
            var rowCounts = new Dictionary<string, Int64>();
            foreach (var db in shardDatabases) {
                var cnts = GetRowCounts(config.tables, db, batch.CTID);
                foreach (var cnt in cnts) {
                    if (rowCounts.ContainsKey(cnt.Key)) {
                        rowCounts[cnt.Key] += cnt.Value;
                    } else {
                        rowCounts[cnt.Key] = cnt.Value;
                    }
                }
            }
            PublishTableInfo(config.tables, config.relayDB, rowCounts, batch.CTID);
        }

        private void ConsolidateTables(ChangeTrackingBatch batch) {
            var databasesWithTableChanges = new Dictionary<TableConf, List<string>>();
            var dc = DataCopyFactory.GetInstance(config.relayType.Value, config.relayType.Value, sourceDataUtils, sourceDataUtils);
            foreach (var tableDb in tableDBFieldLists) {
                var table = tableDb.Key;
                dc.CopyTableDefinition(tableDb.Value.First().Key, table.ToCTName(batch.CTID), table.schemaName, config.relayDB, table.ToCTName(batch.CTID));
                foreach (var dbNameFields in tableDb.Value) {
                    var dbName = dbNameFields.Key;
                    var columns = dbNameFields.Value;
                    if (columns.Count == 0) {
                        //no changes in this DB for this table
                        continue;
                    }
                    sourceDataUtils.MergeCTTable(table, config.relayDB, dbName, batch.CTID);
                }
            }
        }
    }
}
