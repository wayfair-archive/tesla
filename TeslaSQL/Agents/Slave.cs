#region Using Statements
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Xml;
using Xunit;
using TeslaSQL.DataUtils;
using TeslaSQL.DataCopy;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Concurrent;
#endregion

namespace TeslaSQL.Agents {
    //TODO throughout this class add error handling for tables that shouldn't stop on error
    public class Slave : Agent {
        public Slave(IDataUtils sourceDataUtils, IDataUtils destDataUtils, Logger logger)
            : base(sourceDataUtils, destDataUtils, logger) {

        }

        public Slave() {
            //this constructor is only used by for running unit tests
            this.logger = new Logger(LogLevel.Critical, null, null, null, "");
        }

        public override void ValidateConfig() {
            Config.ValidateRequiredHost(Config.relayServer);
            Config.ValidateRequiredHost(Config.slave);
            if (Config.relayType == null || Config.slaveType == null) {
                throw new Exception("Slave agent requires a valid SQL flavor for relay and slave");
            }
        }
        public string TimingKey {
            get {
                return string.Format("db.mssql_changetracking_counters.TeslaRunDuration{0}.{1}.{2}", Config.slave.Replace('.', '_'), AgentType.Slave, Config.slaveDB);
            }
        }
        public override void Run() {
            DateTime start = DateTime.Now;
            logger.Log("Initializing CT batch", LogLevel.Trace);
            var batches = GetIncompleteBatches();
            if (batches.Count == 0) {
                logger.Log("No incomplete batches - initializing new", LogLevel.Debug);
                batches = InitializeBatch();
            } else if (HasMagicHour() && !FullRunTime(DateTime.Now) && batches.All(ctb => (ctb.syncBitWise & Convert.ToInt32(SyncBitWise.ApplySchemaChanges)) > 0)) {
                logger.Log("Magic hours are defined and we are not in one: applying schema changes only", LogLevel.Info);
                batches = InitializeBatch();
                foreach (var batch in batches) {
                    ApplySchemaChangesAndWrite(batch);
                }
                logger.Timing(TimingKey, (int)(DateTime.Now - start).TotalMinutes);
                return;
            }


            /**
             * If you run a batch as Multi, and that batch fails, and before the next run,
             * you increase the batchConsolidationThreshold, this can lead to unexpected behaviour.
             */
            if (Config.batchConsolidationThreshold == 0 || batches.Count < Config.batchConsolidationThreshold) {
                foreach (var batch in batches) {
                    logger.Log("Running single batch " + batch.CTID, LogLevel.Debug);
                    RunSingleBatch(batch);
                }
                logger.Timing("db.mssql_changetracking_counters.DataAppliedAsOf." + Config.slaveDB, DateTime.Now.Hour + DateTime.Now.Minute / 60);
            } else {
                RunMultiBatch(batches);
            }

            logger.Log("Slave agent work complete", LogLevel.Info);
            logger.Timing(TimingKey, (int)(DateTime.Now - start).TotalMinutes);
            return;
        }


        protected bool FullRunTime(DateTime now) {
            DateTime lastRun = GetLastRunTime();
            if (lastRun > now) { throw new Exception("Time went backwards"); }
            foreach (var magicHour in Config.magicHours) {
                if (now.TimeOfDay > magicHour) {
                    //this time slot has passed for today
                    if (lastRun.TimeOfDay < magicHour || lastRun.Date < now.Date) {
                        return true;
                    }
                }
            }
            return false;
        }

        private bool HasMagicHour() {
            return Config.magicHours != null && Config.magicHours.Length > 0;
        }

        private DateTime GetLastRunTime() {
            return sourceDataUtils.GetLastStartTime(Config.relayDB, long.MaxValue, Convert.ToInt32(SyncBitWise.SyncHistoryTables), AgentType.Slave);
        }

        /// <summary>
        /// Initializes version/batch info for a run
        /// </summary>
        /// <returns>List of change tracking batches to work on</returns>
        private IList<ChangeTrackingBatch> InitializeBatch() {
            ChangeTrackingBatch ctb;
            IList<ChangeTrackingBatch> batches = new List<ChangeTrackingBatch>();

            DataRow lastBatch = sourceDataUtils.GetLastCTBatch(Config.relayDB, AgentType.Slave, Config.slave);
            if (lastBatch == null) {
                ctb = new ChangeTrackingBatch(1, 0, 0, 0);
                batches.Add(ctb);
                return batches;
            }

            if ((lastBatch.Field<Int32>("syncBitWise") & Convert.ToInt32(SyncBitWise.SyncHistoryTables)) > 0) {
                logger.Log("Last batch was successful, checking for new batches.", LogLevel.Debug);

                DataTable pendingVersions = sourceDataUtils.GetPendingCTVersions(Config.relayDB, lastBatch.Field<Int64>("CTID"), Convert.ToInt32(SyncBitWise.UploadChanges));
                logger.Log("Retrieved " + pendingVersions.Rows.Count + " pending CT version(s) to work on.", LogLevel.Debug);

                foreach (DataRow row in pendingVersions.Rows) {
                    ctb = new ChangeTrackingBatch(row);
                    batches.Add(ctb);
                    sourceDataUtils.CreateSlaveCTVersion(Config.relayDB, ctb, Config.slave);
                }
                return batches;
            }
            ctb = new ChangeTrackingBatch(lastBatch);
            logger.Log("Last batch failed, retrying CTID " + ctb.CTID, LogLevel.Warn);
            batches.Add(ctb);
            return batches;
        }

        private IList<ChangeTrackingBatch> GetIncompleteBatches() {
            var batches = new List<ChangeTrackingBatch>();
            logger.Log("Retrieving information on last run for slave " + Config.slave, LogLevel.Debug);
            var incompleteBatches = sourceDataUtils.GetPendingCTSlaveVersions(Config.relayDB);
            if (incompleteBatches.Rows.Count > 0) {
                foreach (DataRow row in incompleteBatches.Rows) {
                    batches.Add(new ChangeTrackingBatch(row));
                }
            }
            return batches;
        }

        /// <summary>
        /// Runs a single change tracking batch
        /// </summary>
        /// <param name="CTID">Change tracking batch object to work on</param>
        private void RunSingleBatch(ChangeTrackingBatch ctb) {
            Stopwatch sw;
            ApplySchemaChangesAndWrite(ctb);
            if ((ctb.syncBitWise & Convert.ToInt32(SyncBitWise.DownloadChanges)) == 0) {
                logger.Log("Downloading changes", LogLevel.Debug);
                sw = Stopwatch.StartNew();
                CopyChangeTables(Config.tables, Config.relayDB, Config.slaveCTDB, ctb.CTID);
                logger.Log("CopyChangeTables: " + sw.Elapsed, LogLevel.Trace);
                sourceDataUtils.WriteBitWise(Config.relayDB, ctb.CTID, Convert.ToInt32(SyncBitWise.DownloadChanges), AgentType.Slave);
                //marking this field so that completed slave batches will have the same values
                sourceDataUtils.WriteBitWise(Config.relayDB, ctb.CTID, Convert.ToInt32(SyncBitWise.ConsolidateBatches), AgentType.Slave);
            }

            logger.Log("Populating table list", LogLevel.Debug);
            List<ChangeTable> existingCTTables = PopulateTableList(Config.tables, Config.slaveCTDB, ctb.CTID);

            if ((ctb.syncBitWise & Convert.ToInt32(SyncBitWise.ApplyChanges)) == 0) {
                logger.Log("Applying changes", LogLevel.Debug);
                SetFieldListsSlave(Config.slaveCTDB, Config.tables, ctb);
                sw = Stopwatch.StartNew();
                RowCounts total = ApplyChanges(Config.tables, Config.slaveDB, existingCTTables, ctb.CTID);
                RecordRowCounts(total, ctb);
                logger.Log("ApplyChanges: " + sw.Elapsed, LogLevel.Trace);
                sourceDataUtils.WriteBitWise(Config.relayDB, ctb.CTID, Convert.ToInt32(SyncBitWise.ApplyChanges), AgentType.Slave);
            }
            logger.Log("Syncing history tables", LogLevel.Debug);
            sw = Stopwatch.StartNew();
            SyncHistoryTables(Config.tables, Config.slaveCTDB, Config.slaveDB, existingCTTables);
            logger.Log("SyncHistoryTables: " + sw.Elapsed, LogLevel.Trace);
            var syncStopTime = DateTime.Now;
            sourceDataUtils.MarkBatchComplete(Config.relayDB, ctb.CTID, syncStopTime, Config.slave);
            string key = String.Format(
                "db.mssql_changetracking_counters.DataDurationToSync{0}.{1}",
                Config.slave.Replace('.', '_'),
                Config.slaveDB);
            logger.Increment(key, (int)(syncStopTime - ctb.syncStartTime.Value).TotalMinutes);
        }

        private void RecordRowCounts(RowCounts actual, ChangeTrackingBatch ctb) {
            var expected = sourceDataUtils.GetExpectedRowCounts(Config.relayDB, ctb.CTID);
            logger.Log("Expected row counts: " + expected + " | actual: " + actual, LogLevel.Info);
            double diff = expected - actual.Inserted;
            double mismatch = diff / expected;
            int percentDiff = (int)(mismatch * 100);
            string key = string.Format("db.mssql_changetracking_counters.RecordCountMismatchProd{0}.{1}", Config.slave.Replace('.', '_'), Config.slaveDB);
            logger.Increment(key, percentDiff);
        }


        /// <summary>
        /// Consolidates multiple batches and runs them as a group
        /// </summary>
        /// <param name="ctidTable">DataTable object listing all the batches</param>
        private void RunMultiBatch(IList<ChangeTrackingBatch> batches) {
            var existingCTTables = new List<ChangeTable>();

            ChangeTrackingBatch endBatch = batches.OrderBy(item => item.CTID).Last();

            //from here forward all operations will use the bitwise value for the last CTID since they are operating on this whole set of batches

            foreach (ChangeTrackingBatch batch in batches) {
                logger.Log("Populating list of changetables for CTID : " + batch.CTID, LogLevel.Debug);
                existingCTTables = existingCTTables.Concat(PopulateTableList(Config.tables, Config.slaveCTDB, batch.CTID)).ToList();
            }
            SetFieldListsSlave(Config.slaveDB, Config.tables, endBatch);

            foreach (ChangeTrackingBatch batch in batches) {
                if ((batch.syncBitWise & Convert.ToInt32(SyncBitWise.ApplySchemaChanges)) == 0) {
                    logger.Log("Applying schema changes", LogLevel.Debug);
                    ApplySchemaChanges(Config.tables, Config.relayDB, Config.slaveDB, batch.CTID);
                    sourceDataUtils.WriteBitWise(Config.relayDB, batch.CTID, Convert.ToInt32(SyncBitWise.ApplySchemaChanges), AgentType.Slave);
                }
            }

            if ((endBatch.syncBitWise & Convert.ToInt32(SyncBitWise.ConsolidateBatches)) == 0) {
                logger.Log("Consolidating batches", LogLevel.Trace);
                ConsolidateBatches(existingCTTables);
                sourceDataUtils.WriteBitWise(Config.relayDB, endBatch.CTID, Convert.ToInt32(SyncBitWise.ConsolidateBatches), AgentType.Slave);
            }

            if ((endBatch.syncBitWise & Convert.ToInt32(SyncBitWise.DownloadChanges)) == 0) {
                logger.Log("Downloading consolidated changetables", LogLevel.Debug);
                CopyChangeTables(Config.tables, Config.relayDB, Config.slaveCTDB, endBatch.CTID, isConsolidated: true);
                logger.Log("Changes downloaded successfully", LogLevel.Debug);
                sourceDataUtils.WriteBitWise(Config.relayDB, endBatch.CTID, Convert.ToInt32(SyncBitWise.DownloadChanges), AgentType.Slave);
            }

            RowCounts total;
            if ((endBatch.syncBitWise & Convert.ToInt32(SyncBitWise.ApplyChanges)) == 0) {
                total = ApplyChanges(Config.tables, Config.slaveCTDB, existingCTTables, endBatch.CTID);
                sourceDataUtils.WriteBitWise(Config.relayDB, endBatch.CTID, Convert.ToInt32(SyncBitWise.ApplyChanges), AgentType.Slave);
                RecordRowCounts(total, endBatch);
            }
            var lastChangedTables = new List<ChangeTable>();
            foreach (var group in existingCTTables.GroupBy(c => c.name)) {
                lastChangedTables.Add(group.OrderByDescending(c => c.ctid).First());
            }
            if ((endBatch.syncBitWise & Convert.ToInt32(SyncBitWise.SyncHistoryTables)) == 0) {
                SyncHistoryTables(Config.tables, Config.slaveCTDB, Config.slaveDB, lastChangedTables);
                sourceDataUtils.WriteBitWise(Config.relayDB, endBatch.CTID, Convert.ToInt32(SyncBitWise.SyncHistoryTables), AgentType.Slave);
            }
            //success! go through and mark all the batches as complete in the db
            foreach (ChangeTrackingBatch batch in batches) {
                sourceDataUtils.MarkBatchComplete(Config.relayDB, batch.CTID, DateTime.Now, Config.slave);
            }
            logger.Timing("db.mssql_changetracking_counters.DataAppliedAsOf." + Config.slaveDB, DateTime.Now.Hour + DateTime.Now.Minute / 60);
        }

        private IEnumerable<ChangeTable> ConsolidateBatches(IList<ChangeTable> tables) {
            var lu = new Dictionary<string, List<ChangeTable>>();
            var actions = new List<Action>();
            foreach (var changeTable in tables) {
                if (!lu.ContainsKey(changeTable.name)) {
                    lu[changeTable.name] = new List<ChangeTable>();
                }
                lu[changeTable.name].Add(changeTable);
            }
            var consolidatedTables = new List<ChangeTable>();
            foreach (var table in Config.tables) {
                if (!lu.ContainsKey(table.Name)) {
                    continue;
                }
                var lastChangeTable = lu[table.Name].OrderByDescending(c => c.ctid).First();
                consolidatedTables.Add(lastChangeTable);
                TableConf tLocal = table;
                IDataCopy dataCopy = DataCopyFactory.GetInstance(Config.relayType.Value, Config.slaveType.Value, sourceDataUtils, sourceDataUtils, logger);
                Action act = () => {
                    try {
                        dataCopy.CopyTable(Config.relayDB, lastChangeTable.ctName, tLocal.schemaName, Config.relayDB, Config.dataCopyTimeout, lastChangeTable.consolidatedName);
                        foreach (var changeTable in lu[lastChangeTable.name].OrderByDescending(c => c.ctid)) {
                            sourceDataUtils.Consolidate(changeTable.ctName, changeTable.consolidatedName, Config.relayDB, tLocal.schemaName);
                        }
                        sourceDataUtils.RemoveDuplicatePrimaryKeyChangeRows(tLocal, lastChangeTable.consolidatedName, Config.relayDB);
                    } catch (Exception e) {
                        HandleException(e, tLocal);
                    }
                };
                actions.Add(act);
            }
            logger.Log("Parallel invocation of " + actions.Count + " changetable consolidations", LogLevel.Trace);
            var options = new ParallelOptions();
            options.MaxDegreeOfParallelism = Config.maxThreads;
            Parallel.Invoke(options, actions.ToArray());
            return consolidatedTables;
        }

        private void SetFieldListsSlave(string dbName, IEnumerable<TableConf> tables, ChangeTrackingBatch batch) {
            foreach (var table in tables) {
                var cols = sourceDataUtils.GetFieldList(dbName, table.ToCTName(batch.CTID), table.schemaName);

                //this is hacky but these aren't columns we actually care about, but we expect them to be there
                cols.Remove("SYS_CHANGE_VERSION");
                cols.Remove("SYS_CHANGE_OPERATION");
                var pks = sourceDataUtils.GetPrimaryKeysFromInfoTable(table, batch, dbName);
                foreach (var pk in pks) {
                    cols[pk] = true;
                }
                SetFieldList(table, cols);
            }
        }

        private void ApplySchemaChangesAndWrite(ChangeTrackingBatch ctb) {
            if ((ctb.syncBitWise & Convert.ToInt32(SyncBitWise.ApplySchemaChanges)) == 0) {
                logger.Log("Applying schema changes", LogLevel.Debug);
                var sw = Stopwatch.StartNew();
                ApplySchemaChanges(Config.tables, Config.relayDB, Config.slaveDB, ctb.CTID);
                logger.Log("ApplySchemaChanges: " + sw.Elapsed, LogLevel.Trace);
                sourceDataUtils.WriteBitWise(Config.relayDB, ctb.CTID, Convert.ToInt32(SyncBitWise.ApplySchemaChanges), AgentType.Slave);
                //marking this field so that all completed slave batches will have the same values
                sourceDataUtils.WriteBitWise(Config.relayDB, ctb.CTID, Convert.ToInt32(SyncBitWise.ConsolidateBatches), AgentType.Slave);
            }
        }

        private void SyncHistoryTables(TableConf[] tableConf, string slaveCTDB, string slaveDB, List<ChangeTable> existingCTTables) {
            var actions = new List<Action>();
            foreach (var t in existingCTTables) {
                var s = tableConf.First(tc => tc.Name.Equals(t.name, StringComparison.InvariantCultureIgnoreCase));
                if (!s.recordHistoryTable) {
                    logger.Log("Skipping writing history table for " + t.name + " because it is not configured", LogLevel.Debug);
                    continue;
                }
                ChangeTable tLocal = t;
                Action act = () => {
                    logger.Log("Writing history table for " + tLocal.name, LogLevel.Debug);
                    try {
                        destDataUtils.CopyIntoHistoryTable(tLocal, slaveCTDB);
                        logger.Log("Successfully wrote history for " + tLocal.name, LogLevel.Debug);
                    } catch (Exception e) {
                        HandleException(e, s);
                    }
                };
                actions.Add(act);
            }

            logger.Log("Parallel invocation of " + actions.Count + " history table syncs", LogLevel.Trace);
            var options = new ParallelOptions();
            options.MaxDegreeOfParallelism = Config.maxThreads;
            Parallel.Invoke(options, actions.ToArray());
        }

        private RowCounts ApplyChanges(TableConf[] tableConf, string slaveDB, List<ChangeTable> tables, Int64 CTID) {
            var hasArchive = new Dictionary<TableConf, TableConf>();
            foreach (var table in tableConf) {
                if (tables.Any(s => s.name == table.Name)) {
                    if (hasArchive.ContainsKey(table)) {
                        //so we don't grab tblOrderArchive, insert tlbOrder: tblOrderArchive, and then go back and insert tblOrder: null.
                        continue;
                    }
                    if (table.Name.EndsWith("Archive")) {
                        string nonArchiveTableName = CTTableName(table.Name.Substring(0, table.Name.Length - table.Name.LastIndexOf("Archive")), CTID);
                        if (tables.Any(s => s.name == nonArchiveTableName)) {
                            var nonArchiveTable = tableConf.First(t => t.Name == nonArchiveTableName);
                            hasArchive[nonArchiveTable] = table;
                        } else {
                            hasArchive[table] = null;
                        }
                    } else {
                        hasArchive[table] = null;
                    }
                }
            }
            var actions = new List<Action>();
            var counts = new ConcurrentDictionary<string, RowCounts>();
            foreach (var tableArchive in hasArchive) {
                KeyValuePair<TableConf, TableConf> tLocal = tableArchive;
                Action act = () => {
                    try {
                        logger.Log("Applying changes for table " + tLocal.Key.Name + (hasArchive == null ? "" : " (and archive)"), LogLevel.Debug);
                        var sw = Stopwatch.StartNew();
                        var rc = destDataUtils.ApplyTableChanges(tLocal.Key, tLocal.Value, Config.slaveDB, CTID, Config.slaveCTDB);
                        counts[tLocal.Key.Name] = rc;
                        logger.Log("ApplyTableChanges " + tLocal.Key.Name + ": " + sw.Elapsed, LogLevel.Trace);
                    } catch (Exception e) {
                        HandleException(e, tLocal.Key);
                    }
                };
                actions.Add(act);
            }
            logger.Log("Parallel invocation of " + actions.Count + " table change applies", LogLevel.Trace);
            var options = new ParallelOptions();
            options.MaxDegreeOfParallelism = Config.maxThreads;
            Parallel.Invoke(options, actions.ToArray());
            RowCounts total = counts.Values.Aggregate(new RowCounts(0, 0), (a, b) => new RowCounts(a.Inserted + b.Inserted, a.Deleted + b.Deleted));
            return total;
        }


        /// <summary>
        /// For the specified list of tables, populate a list of which CT tables exist
        /// </summary>
        /// <param name="tables">Array of table config objects</param>
        /// <param name="dbName">Database name</param>
        /// <param name="tables">List of table names to populate</param>
        private List<ChangeTable> PopulateTableList(TableConf[] tables, string dbName, Int64 CTID) {
            var tableList = new List<ChangeTable>();
            foreach (TableConf t in tables) {
                var ct = new ChangeTable(t.Name, CTID, t.schemaName, Config.slave);
                try {
                    if (sourceDataUtils.CheckTableExists(dbName, ct.ctName, t.schemaName)) {
                        tableList.Add(ct);
                    } else {
                        logger.Log("Did not find table " + ct.ctName, LogLevel.Debug);
                    }
                } catch (Exception e) {
                    HandleException(e, t);
                }
            }
            return tableList;
        }

        /// <summary>
        /// Given a table name and CTID, returns the CT table name
        /// </summary>
        /// <param name="table">Table name</param>
        /// <param name="CTID">Change tracking batch iD</param>
        /// <returns>CT table name</returns>
        public string CTTableName(string table, Int64? CTID = null) {
            if (CTID != null) {
                return "tblCT" + table + "_" + Convert.ToString(CTID);
            } else {
                //consolidated table always has the same name
                return "tblCT" + table + "_" + Config.slave;
            }
        }


        /// <summary>
        /// Copies change tables from the master to the relay server
        /// </summary>
        /// <param name="tables">Array of table config objects</param>
        /// <param name="sourceCTDB">Source CT database</param>
        /// <param name="destCTDB">Dest CT database</param>
        /// <param name="CTID">CT batch ID this is for</param>
        private void CopyChangeTables(TableConf[] tables, string sourceCTDB, string destCTDB, Int64 CTID, bool isConsolidated = false) {
            if (Config.slave != null && Config.slave == Config.relayServer && sourceCTDB == destCTDB) {
                logger.Log("Skipping download because slave is equal to relay.", LogLevel.Debug);
                return;
            }

            var actions = new List<Action>();
            foreach (TableConf t in tables) {
                IDataCopy dataCopy = DataCopyFactory.GetInstance(Config.relayType.Value, Config.slaveType.Value, sourceDataUtils, destDataUtils, logger);
                var ct = new ChangeTable(t.Name, CTID, t.schemaName, Config.slave);
                string sourceCTTable = isConsolidated ? ct.consolidatedName : ct.ctName;
                string destCTTable = ct.ctName;
                TableConf tLocal = t;
                Action act = () => {
                    try {
                        //hard coding timeout at 1 hour for bulk copy
                        logger.Log("Copying table " + tLocal.schemaName + "." + sourceCTTable + " to slave", LogLevel.Trace);
                        var sw = Stopwatch.StartNew();
                        dataCopy.CopyTable(sourceCTDB, sourceCTTable, tLocal.schemaName, destCTDB, 36000, destCTTable);
                        logger.Log("CopyTable: " + sw.Elapsed, LogLevel.Trace);
                    } catch (DoesNotExistException) {
                        //this is a totally normal and expected case since we only publish changetables when data actually changed
                        logger.Log("No changes to pull for table " + tLocal.schemaName + "." + sourceCTTable + " because it does not exist ", LogLevel.Debug);
                    } catch (Exception e) {
                        HandleException(e, tLocal);
                    }
                };
                actions.Add(act);
            }
            logger.Log("Parallel invocation of " + actions.Count + " changetable downloads", LogLevel.Trace);
            var options = new ParallelOptions();
            options.MaxDegreeOfParallelism = Config.maxThreads;
            Parallel.Invoke(options, actions.ToArray());
            return;
        }

        public void ApplySchemaChanges(TableConf[] tables, string sourceDB, string destDB, Int64 CTID) {
            //get list of schema changes from tblCTSChemaChange_ctid on the relay server/db
            DataTable result = sourceDataUtils.GetSchemaChanges(sourceDB, CTID);

            if (result == null) {
                return;
            }

            TableConf table;
            foreach (DataRow row in result.Rows) {
                var schemaChange = new SchemaChange(row);
                //String.Compare method returns 0 if the strings are equal
                table = tables.SingleOrDefault(item => String.Compare(item.Name, schemaChange.tableName, ignoreCase: true) == 0);

                if (table == null) {
                    logger.Log("Ignoring schema change for table " + row.Field<string>("CscTableName") + " because it isn't in config", LogLevel.Debug);
                    continue;
                }
                logger.Log("Processing schema change (CscID: " + row.Field<int>("CscID") +
                    ") of type " + schemaChange.eventType + " for table " + table.Name, LogLevel.Info);

                if (table.columnList == null || table.columnList.Contains(schemaChange.columnName, StringComparer.OrdinalIgnoreCase)) {
                    logger.Log("Schema change applies to a valid column, so we will apply it", LogLevel.Info);
                    try {
                        ApplySchemaChange(destDB, table, schemaChange);
                    } catch (Exception e) {
                        HandleException(e, table);
                    }
                } else {
                    logger.Log("Skipped schema change because the column it impacts is not in our list", LogLevel.Info);
                }

            }
        }

        private void ApplySchemaChange(string destDB, TableConf table, SchemaChange schemaChange) {
            switch (schemaChange.eventType) {
                case SchemaChangeType.Rename:
                    logger.Log("Renaming column " + schemaChange.columnName + " to " + schemaChange.newColumnName, LogLevel.Info);
                    destDataUtils.RenameColumn(table, destDB, schemaChange.schemaName, schemaChange.tableName,
                        schemaChange.columnName, schemaChange.newColumnName);
                    break;
                case SchemaChangeType.Modify:
                    logger.Log("Changing data type on column " + schemaChange.columnName, LogLevel.Info);
                    destDataUtils.ModifyColumn(table, destDB, schemaChange.schemaName, schemaChange.tableName, schemaChange.columnName, schemaChange.dataType.ToString());
                    break;
                case SchemaChangeType.Add:
                    logger.Log("Adding column " + schemaChange.columnName, LogLevel.Info);
                    destDataUtils.AddColumn(table, destDB, schemaChange.schemaName, schemaChange.tableName, schemaChange.columnName, schemaChange.dataType.ToString());
                    break;
                case SchemaChangeType.Drop:
                    logger.Log("Dropping column " + schemaChange.columnName, LogLevel.Info);
                    destDataUtils.DropColumn(table, destDB, schemaChange.schemaName, schemaChange.tableName, schemaChange.columnName);
                    break;
            }
        }

    }
}
