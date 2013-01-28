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
    public class Slave : Agent {
        private static readonly int SCHEMA_CHANGE_COMPLETE = (int)(SyncBitWise.PublishSchemaChanges | SyncBitWise.CaptureChanges | SyncBitWise.UploadChanges | SyncBitWise.ApplySchemaChanges);
        private static readonly int BATCH_COMPLETE = Enum.GetValues(typeof(SyncBitWise)).Cast<int>().Sum();


        public Slave(IDataUtils sourceDataUtils, IDataUtils destDataUtils, Logger logger)
            : base(sourceDataUtils, destDataUtils, logger) {

        }

        public Slave() {
            //this constructor is only used by for running unit tests
            this.logger = new Logger(null, null, null, "");
        }

        public override void ValidateConfig() {
            Config.ValidateRequiredHost(Config.RelayServer);
            Config.ValidateRequiredHost(Config.Slave);
            if (Config.RelayType == SqlFlavor.None || Config.SlaveType == SqlFlavor.None) {
                throw new Exception("Slave agent requires a valid SQL flavor for relay and slave");
            }
        }

        private string TimingKey {
            get {
                return string.Format("db.mssql_changetracking_counters.TeslaRunDuration.{0}.{1}.{2}", Config.Slave.Replace('.', '_'), AgentType.Slave, Config.SlaveDB);
            }
        }

        private string StepTimingKey(string stepName) {
            return string.Format("db.mssql_changetracking_counters.{0}.{1}.{2}", Config.Slave.Replace('.', '_'), Config.SlaveDB, stepName);
        }

        public override void Run() {
            DateTime start = DateTime.Now;
            logger.Log("Initializing CT batch", LogLevel.Info);
            if (HasMagicHour()) {
                var batches = GetIncompleteBatches();
                ApplyBatchedSchemaChanges(batches);
                if (batches.All(b => b.SyncBitWise == SCHEMA_CHANGE_COMPLETE)) {
                    //pull new batches
                    batches = InitializeBatch(SCHEMA_CHANGE_COMPLETE);
                    ApplyBatchedSchemaChanges(batches);
                    batches = GetIncompleteBatches();
                    if (batches.Count > 0 && IsFullRunTime(batches.Last().SyncStartTime.Value)) {
                        logger.Log("Magic hour criteria reached, processing batch(es)", LogLevel.Debug);
                        ProcessBatches(batches);
                    } else {
                        logger.Log("Schema changes for all pending batches complete and magic hour not yet reached", LogLevel.Debug);
                    }
                } else if (batches.Count > 0 && IsFullRunTime(batches.Last().SyncStartTime.Value)) {
                    logger.Log("Magic hour criteria reached, retrying processing batch(es)", LogLevel.Debug);
                    ProcessBatches(batches);
                } else {
                    logger.Log("No new batches published by master", LogLevel.Info);
                }
            } else {
                var batches = GetIncompleteBatches();
                if (batches.Count == 0) {
                    batches = InitializeBatch(BATCH_COMPLETE);
                }
                ProcessBatches(batches);
            }

            logger.Log("Slave agent work complete", LogLevel.Info);
            logger.Timing(TimingKey, (int)(DateTime.Now - start).TotalMinutes);
            return;
        }

        private void ApplyBatchedSchemaChanges(IList<ChangeTrackingBatch> batches) {
            foreach (var batch in batches) {
                ApplySchemaChangesAndWrite(batch);
            }
        }

        private void ProcessBatches(IList<ChangeTrackingBatch> batches) {
            /**
             * If you run a batch as Multi, and that batch fails, and before the next run,
             * you increase the batchConsolidationThreshold, this can lead to unexpected behaviour.
             */
            if (Config.BatchConsolidationThreshold == 0 || batches.Count < Config.BatchConsolidationThreshold) {
                foreach (var batch in batches) {
                    Logger.SetProperty("CTID", batch.CTID);
                    logger.Log("Running single batch " + batch.CTID, LogLevel.Info);
                    RunSingleBatch(batch);
                }
                logger.Timing("db.mssql_changetracking_counters.DataAppliedAsOf." + Config.SlaveDB, DateTime.Now.Hour + DateTime.Now.Minute / 60);
            } else {
                logger.Log("Running multi batch", LogLevel.Info);
                RunMultiBatch(batches);
            }
        }

        protected bool IsFullRunTime(DateTime now) {
            DateTime lastRun = GetLastRunTime();
            if (lastRun > now) { throw new Exception("Time went backwards"); }
            foreach (var magicHour in Config.MagicHours) {
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
            return Config.MagicHours != null && Config.MagicHours.Length > 0;
        }

        private DateTime GetLastRunTime() {
            return sourceDataUtils.GetLastStartTime(Config.RelayDB, long.MaxValue, Convert.ToInt32(SyncBitWise.SyncHistoryTables), AgentType.Slave, Config.Slave);
        }

        /// <summary>
        /// Initializes version/batch info for a run
        /// </summary>
        /// <returns>List of change tracking batches to work on</returns>
        private IList<ChangeTrackingBatch> InitializeBatch(int bitwise) {
            ChangeTrackingBatch ctb;
            IList<ChangeTrackingBatch> batches = new List<ChangeTrackingBatch>();

            DataRow lastBatch = sourceDataUtils.GetLastCTBatch(Config.RelayDB, AgentType.Slave, Config.Slave);
            if (lastBatch == null) {
                ctb = new ChangeTrackingBatch(1, 0, 0, 0);
                batches.Add(ctb);
                return batches;
            }

            if ((lastBatch.Field<Int32>("syncBitWise") & bitwise) == bitwise) {
                logger.Log("Last batch was successful, checking for new batches.", LogLevel.Info);

                DataTable pendingVersions = sourceDataUtils.GetPendingCTVersions(Config.RelayDB, lastBatch.Field<Int64>("CTID"), Convert.ToInt32(SyncBitWise.UploadChanges));
                logger.Log("Retrieved " + pendingVersions.Rows.Count + " pending CT version(s) to work on.", LogLevel.Info);

                foreach (DataRow row in pendingVersions.Rows) {
                    ctb = new ChangeTrackingBatch(row);
                    batches.Add(ctb);
                    sourceDataUtils.CreateSlaveCTVersion(Config.RelayDB, ctb, Config.Slave);
                }
                return batches;
            }
            ctb = new ChangeTrackingBatch(lastBatch);
            logger.Log(new { message = "Last batch failed, retrying", CTID = ctb.CTID }, LogLevel.Warn);
            batches.Add(ctb);
            return batches;
        }

        private IList<ChangeTrackingBatch> GetIncompleteBatches() {
            var batches = new List<ChangeTrackingBatch>();
            logger.Log("Retrieving information on last run", LogLevel.Debug);
            var incompleteBatches = sourceDataUtils.GetPendingCTSlaveVersions(Config.RelayDB, Config.Slave, BATCH_COMPLETE);
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
            logger.Log("Applying schema changes ", LogLevel.Info);
            ApplySchemaChangesAndWrite(ctb);
            //marking this field so that all completed slave batches will have the same values
            sourceDataUtils.WriteBitWise(Config.RelayDB, ctb.CTID, Convert.ToInt32(SyncBitWise.ConsolidateBatches), AgentType.Slave);
            if ((ctb.SyncBitWise & Convert.ToInt32(SyncBitWise.DownloadChanges)) == 0) {
                logger.Log("Downloading changes", LogLevel.Info);
                sw = Stopwatch.StartNew();
                CopyChangeTables(Config.Tables, Config.RelayDB, Config.SlaveCTDB, ctb.CTID);
                logger.Log("CopyChangeTables: " + sw.Elapsed, LogLevel.Trace);
                sourceDataUtils.WriteBitWise(Config.RelayDB, ctb.CTID, Convert.ToInt32(SyncBitWise.DownloadChanges), AgentType.Slave);
                logger.Timing(StepTimingKey("DownloadChanges"), (int)sw.ElapsedMilliseconds);
            }

            logger.Log("Populating table list", LogLevel.Info);
            List<ChangeTable> existingCTTables = PopulateTableList(Config.Tables, Config.RelayDB, new List<ChangeTrackingBatch>() { ctb });

            if ((ctb.SyncBitWise & Convert.ToInt32(SyncBitWise.ApplyChanges)) == 0) {
                logger.Log("Applying changes", LogLevel.Info);
                sw = Stopwatch.StartNew();
                SetFieldListsSlave(Config.RelayDB, Config.Tables, ctb, existingCTTables);
                RowCounts total = ApplyChanges(existingCTTables, ctb.CTID);
                RecordRowCounts(total, ctb);
                logger.Log("ApplyChanges: " + sw.Elapsed, LogLevel.Trace);
                sourceDataUtils.WriteBitWise(Config.RelayDB, ctb.CTID, Convert.ToInt32(SyncBitWise.ApplyChanges), AgentType.Slave);
                logger.Timing(StepTimingKey("ApplyChanges"), (int)sw.ElapsedMilliseconds);
            }
            logger.Log("Syncing history tables", LogLevel.Info);
            sw = Stopwatch.StartNew();
            SyncHistoryTables(Config.SlaveCTDB, existingCTTables);
            logger.Log("SyncHistoryTables: " + sw.Elapsed, LogLevel.Trace);
            var syncStopTime = DateTime.Now;
            sourceDataUtils.MarkBatchComplete(Config.RelayDB, ctb.CTID, syncStopTime, Config.Slave);
            string key = String.Format(
                "db.mssql_changetracking_counters.DataDurationToSync{0}.{1}",
                Config.Slave.Replace('.', '_'),
                Config.SlaveDB);
            logger.Increment(key, (int)(syncStopTime - ctb.SyncStartTime.Value).TotalMinutes);
            logger.Timing(StepTimingKey("SyncHistoryTables"), (int)sw.ElapsedMilliseconds);
        }

        private void RecordRowCounts(RowCounts actual, ChangeTrackingBatch ctb) {
            var expected = sourceDataUtils.GetExpectedRowCounts(Config.RelayDB, ctb.CTID);
            logger.Log("Expected row counts: " + expected + " | actual: " + actual, LogLevel.Info);
            double diff = expected - actual.Inserted;
            double mismatch;
            if (expected == 0) {
                if (actual.Inserted == 0) {
                    mismatch = 0.0;
                } else {
                    logger.Log("Expected 0 rows, got " + actual.Inserted + " rows inserted on slave.", LogLevel.Error);
                    return;
                }
            } else {
                mismatch = diff / expected;
            }
            int percentDiff = (int)(mismatch * 100);
            string key = string.Format("db.mssql_changetracking_counters.RecordCountMismatchProd{0}.{1}", Config.Slave.Replace('.', '_'), Config.SlaveDB);
            logger.Increment(key, percentDiff);
        }


        /// <summary>
        /// Consolidates multiple batches and runs them as a group
        /// </summary>
        /// <param name="ctidTable">DataTable object listing all the batches</param>
        private void RunMultiBatch(IList<ChangeTrackingBatch> batches) {
            ChangeTrackingBatch endBatch = batches.OrderBy(item => item.CTID).Last();
            Logger.SetProperty("CTID", endBatch.CTID);
            logger.Log(string.Format("Running consolidated batches from CTID {0} to {1}",
                batches.OrderBy(item => item.CTID).First().CTID, endBatch.CTID), LogLevel.Info);

            //from here forward all operations will use the bitwise value for the last CTID since they are operating on this whole set of batches
            logger.Log("Populating changetable list for all CTIDs", LogLevel.Info);
            List<ChangeTable> existingCTTables = PopulateTableList(Config.Tables, Config.RelayDB, batches);
            logger.Log("Capturing field lists", LogLevel.Info);
            SetFieldListsSlave(Config.RelayDB, Config.Tables, endBatch, existingCTTables);

            foreach (ChangeTrackingBatch batch in batches) {
                if ((batch.SyncBitWise & Convert.ToInt32(SyncBitWise.ApplySchemaChanges)) == 0) {
                    logger.Log("Applying schema changes for batch " + batch.CTID, LogLevel.Debug);
                    ApplySchemaChanges(Config.RelayDB, Config.SlaveDB, batch.CTID);
                    sourceDataUtils.WriteBitWise(Config.RelayDB, batch.CTID, Convert.ToInt32(SyncBitWise.ApplySchemaChanges), AgentType.Slave);
                }
            }

            if ((endBatch.SyncBitWise & Convert.ToInt32(SyncBitWise.ConsolidateBatches)) == 0) {
                var sw = Stopwatch.StartNew();
                logger.Log("Consolidating batches", LogLevel.Info);
                ConsolidateBatches(existingCTTables);
                sourceDataUtils.WriteBitWise(Config.RelayDB, endBatch.CTID, Convert.ToInt32(SyncBitWise.ConsolidateBatches), AgentType.Slave);
                logger.Timing(StepTimingKey("ConsolidateBatches"), (int)sw.ElapsedMilliseconds);
            }

            if ((endBatch.SyncBitWise & Convert.ToInt32(SyncBitWise.DownloadChanges)) == 0) {
                var sw = Stopwatch.StartNew();
                logger.Log("Downloading consolidated changetables", LogLevel.Info);
                CopyChangeTables(Config.Tables, Config.RelayDB, Config.SlaveCTDB, endBatch.CTID, isConsolidated: true);
                logger.Log("Changes downloaded successfully", LogLevel.Debug);
                sourceDataUtils.WriteBitWise(Config.RelayDB, endBatch.CTID, Convert.ToInt32(SyncBitWise.DownloadChanges), AgentType.Slave);
                logger.Timing(StepTimingKey("DownloadChanges"), (int)sw.ElapsedMilliseconds);
            }

            RowCounts total;
            if ((endBatch.SyncBitWise & Convert.ToInt32(SyncBitWise.ApplyChanges)) == 0) {
                var sw = Stopwatch.StartNew();
                logger.Log("Applying changes", LogLevel.Info);
                total = ApplyChanges(existingCTTables, endBatch.CTID);
                sourceDataUtils.WriteBitWise(Config.RelayDB, endBatch.CTID, Convert.ToInt32(SyncBitWise.ApplyChanges), AgentType.Slave);
                //Expected rowcounts across multiple batches are not currently calculated, it's unclear
                //how we would actually want to calculate them since by the nature of consolidation duplicate inserts/updates are eliminated.
                //Commenting this out for now.
                //RecordRowCounts(total, endBatch);
                logger.Timing(StepTimingKey("ApplyChanges"), (int)sw.ElapsedMilliseconds);
            }

            logger.Log("Figuring out the last batch for each changetable", LogLevel.Debug);
            var lastChangedTables = new List<ChangeTable>();
            foreach (var group in existingCTTables.GroupBy(c => c.name)) {
                var table = group.First();
                lastChangedTables.Add(new ChangeTable(table.name, endBatch.CTID, table.schemaName, table.slaveName));
            }

            if ((endBatch.SyncBitWise & Convert.ToInt32(SyncBitWise.SyncHistoryTables)) == 0) {
                var sw = Stopwatch.StartNew();
                logger.Log("Syncing history tables", LogLevel.Info);
                SyncHistoryTables(Config.SlaveCTDB, lastChangedTables);
                sourceDataUtils.WriteBitWise(Config.RelayDB, endBatch.CTID, Convert.ToInt32(SyncBitWise.SyncHistoryTables), AgentType.Slave);
                logger.Timing(StepTimingKey("SyncHistoryTables"), (int)sw.ElapsedMilliseconds);
            }
            //success! go through and mark all the batches as complete in the db
            sourceDataUtils.MarkBatchesComplete(Config.RelayDB, batches.Select(b => b.CTID), DateTime.Now, Config.Slave);
            logger.Timing("db.mssql_changetracking_counters.DataAppliedAsOf." + Config.SlaveDB, DateTime.Now.Hour + DateTime.Now.Minute / 60);
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
            foreach (var table in Config.Tables) {
                if (!lu.ContainsKey(table.Name)) {
                    logger.Log("No changes captured for " + table.Name, LogLevel.Info);
                    continue;
                }
                var lastChangeTable = lu[table.Name].OrderByDescending(c => c.CTID).First();
                consolidatedTables.Add(lastChangeTable);
                TableConf tLocal = table;
                IDataCopy dataCopy = DataCopyFactory.GetInstance(Config.RelayType, Config.RelayType, sourceDataUtils, sourceDataUtils, logger);
                Action act = () => {
                    try {
                        logger.Log("Copying " + lastChangeTable.ctName, LogLevel.Debug);
                        dataCopy.CopyTable(Config.RelayDB, lastChangeTable.ctName, tLocal.SchemaName, Config.RelayDB, Config.DataCopyTimeout, lastChangeTable.consolidatedName);
                        //skipping the first one because dataCopy.CopyTable already copied it).
                        foreach (var changeTable in lu[lastChangeTable.name].OrderByDescending(c => c.CTID).Skip(1)) {
                            logger.Log("Consolidating " + changeTable.ctName, LogLevel.Debug);
                            sourceDataUtils.Consolidate(changeTable.ctName, changeTable.consolidatedName, Config.RelayDB, tLocal.SchemaName);
                        }
                        sourceDataUtils.RemoveDuplicatePrimaryKeyChangeRows(tLocal, lastChangeTable.consolidatedName, Config.RelayDB);
                    } catch (Exception e) {
                        HandleException(e, tLocal);
                    }
                };
                actions.Add(act);
            }
            logger.Log("Parallel invocation of " + actions.Count + " changetable consolidations", LogLevel.Trace);
            var options = new ParallelOptions();
            options.MaxDegreeOfParallelism = Config.MaxThreads;
            Parallel.Invoke(options, actions.ToArray());
            return consolidatedTables;
        }

        private void SetFieldListsSlave(string dbName, IEnumerable<TableConf> tables, ChangeTrackingBatch batch, List<ChangeTable> existingCTTables) {
            //map each table to the last appropriate CT table, ditching tableconfs with no corresponding CT tables
            var tableCTName = new Dictionary<TableConf, string>();
            foreach (var table in tables) {
                ChangeTable changeTable = existingCTTables.Where(ct => ct.name == table.Name).OrderBy(ct => ct.CTID).LastOrDefault();
                if (changeTable == null) {
                    continue;
                }
                long lastCTIDWithChanges = changeTable.CTID.Value;
                tableCTName[table] = table.ToCTName(lastCTIDWithChanges);
            }
            Dictionary<TableConf, IList<string>> allColumnsByTable = sourceDataUtils.GetAllFields(dbName, tableCTName);
            Dictionary<TableConf, IList<string>> primaryKeysByTable = sourceDataUtils.GetAllPrimaryKeys(dbName, tableCTName.Keys, batch);

            //tableCTName.Keys instead of tables because we've already filtered this for tables that don't have change tables
            //note: allColumnsByTable.Keys or primaryKeysByTable.Keys should work just as well
            foreach (var table in tableCTName.Keys) {
                var columns = allColumnsByTable[table].ToDictionary(c => c, c => false);
                //this is a hacky solution but we will have these columns in CT tables but actually are not interested in them here.
                columns.Remove("SYS_CHANGE_VERSION");
                columns.Remove("SYS_CHANGE_OPERATION");
                var pks = primaryKeysByTable[table];
                foreach (var pk in pks) {
                    columns[pk] = true;
                }
                SetFieldList(table, columns);
            }
        }

        private void ApplySchemaChangesAndWrite(ChangeTrackingBatch ctb) {
            if ((ctb.SyncBitWise & Convert.ToInt32(SyncBitWise.ApplySchemaChanges)) == 0) {
                logger.Log("Applying schema changes", LogLevel.Debug);
                var sw = Stopwatch.StartNew();

                ApplySchemaChanges(Config.RelayDB, Config.SlaveDB, ctb.CTID);

                sourceDataUtils.WriteBitWise(Config.RelayDB, ctb.CTID, Convert.ToInt32(SyncBitWise.ApplySchemaChanges), AgentType.Slave);
                ctb.SyncBitWise += Convert.ToInt32(SyncBitWise.ApplySchemaChanges);

                logger.Timing(StepTimingKey("ApplySchemaChanges"), (int)sw.ElapsedMilliseconds);
            }
        }

        private void SyncHistoryTables(string slaveCTDB, List<ChangeTable> existingCTTables) {
            var actions = new List<Action>();
            foreach (var t in existingCTTables) {
                var s = Config.Tables.First(tc => tc.Name.Equals(t.name, StringComparison.InvariantCultureIgnoreCase));
                if (!s.RecordHistoryTable) {
                    logger.Log(new { message = "Skipping writing history table because it is not configured", Table = t.name }, LogLevel.Debug);
                    continue;
                }
                ChangeTable tLocal = t;
                Action act = () => {
                    logger.Log(new { message = "Writing history table", table = tLocal.name }, LogLevel.Debug);
                    try {
                        destDataUtils.CopyIntoHistoryTable(tLocal, slaveCTDB);
                        logger.Log(new { message = "Successfully wrote history", Table = tLocal.name }, LogLevel.Debug);
                    } catch (Exception e) {
                        HandleException(e, s);
                    }
                };
                actions.Add(act);
            }

            logger.Log("Parallel invocation of " + actions.Count + " history table syncs", LogLevel.Trace);
            var options = new ParallelOptions();
            options.MaxDegreeOfParallelism = Config.MaxThreads;
            Parallel.Invoke(options, actions.ToArray());
        }

        private RowCounts ApplyChanges(List<ChangeTable> tables, Int64 CTID) {
            var hasArchive = ValidTablesAndArchives(tables, CTID);
            var actions = new List<Action>();
            var counts = new ConcurrentDictionary<string, RowCounts>();
            foreach (var tableArchive in hasArchive) {
                KeyValuePair<TableConf, TableConf> tLocal = tableArchive;
                Action act = () => {
                    try {
                        logger.Log(new { message = "Applying changes", Table = tLocal.Key.Name + (tLocal.Value == null ? "" : " (and archive)") }, LogLevel.Debug);
                        var sw = Stopwatch.StartNew();
                        var rc = destDataUtils.ApplyTableChanges(tLocal.Key, tLocal.Value, Config.SlaveDB, CTID, Config.SlaveCTDB);
                        counts[tLocal.Key.Name] = rc;
                        logger.Log(new { message = "ApplyTableChanges : " + sw.Elapsed, Table = tLocal.Key.Name }, LogLevel.Trace);
                    } catch (Exception e) {
                        HandleException(e, tLocal.Key);
                    }
                };
                actions.Add(act);
            }
            logger.Log("Parallel invocation of " + actions.Count + " table change applies", LogLevel.Trace);
            var options = new ParallelOptions();
            options.MaxDegreeOfParallelism = Config.MaxThreads;
            Parallel.Invoke(options, actions.ToArray());
            RowCounts total = counts.Values.Aggregate(new RowCounts(0, 0), (a, b) => new RowCounts(a.Inserted + b.Inserted, a.Deleted + b.Deleted));
            return total;
        }

        protected Dictionary<TableConf, TableConf> ValidTablesAndArchives(IEnumerable<ChangeTable> changeTables, Int64 CTID) {
            var hasArchive = new Dictionary<TableConf, TableConf>();
            foreach (var confTable in Config.Tables) {
                if (!changeTables.Any(s => s.name == confTable.Name)) {
                    continue;
                }
                if (hasArchive.ContainsKey(confTable)) {
                    //so we don't grab tblOrderArchive, insert tlbOrder: tblOrderArchive, and then go back and insert tblOrder: null.
                    continue;
                }
                if (confTable.Name.EndsWith("Archive")) {
                    //if we have an archive table, we want to check if we also have the non-archive version of it configured in CT
                    string nonArchiveTableName = confTable.Name.Substring(0, confTable.Name.Length - confTable.Name.LastIndexOf("Archive") + 1);
                    if (changeTables.Any(s => s.name == nonArchiveTableName)) {
                        //if the non-archive table has any changes, we grab the associated table configuration and pair them
                        var nonArchiveTable = Config.Tables.First(t => t.Name == nonArchiveTableName);
                        hasArchive[nonArchiveTable] = confTable;
                    } else {
                        //otherwise we just go ahead and treat the archive CT table as a normal table
                        hasArchive[confTable] = null;
                    }
                } else {
                    //if the table doesn't end with "Archive," there's no archive table for it to pair up with.
                    hasArchive[confTable] = null;
                }

            }
            return hasArchive;
        }


        /// <summary>
        /// For the specified list of tables, populate a list of which CT tables exist
        /// </summary>
        private List<ChangeTable> PopulateTableList(IEnumerable<TableConf> tables, string dbName, IList<ChangeTrackingBatch> batches) {
            var tableList = new List<ChangeTable>();
            DataTable result = sourceDataUtils.GetTablesWithChanges(dbName, batches);
            foreach (DataRow row in result.Rows) {
                var changeTable = new ChangeTable(row.Field<string>("CtiTableName"), row.Field<long>("CTID"), row.Field<string>("CtiSchemaName"), Config.Slave);
                //only add the table if it's in our config
                if (tables.Where(t => t.Name == changeTable.name).Count() == 1) {
                    tableList.Add(changeTable);
                }
            }
            return tableList;
        }

        /// <summary>
        /// Copies change tables from the master to the relay server
        /// </summary>
        /// <param name="tables">Array of table config objects</param>
        /// <param name="sourceCTDB">Source CT database</param>
        /// <param name="destCTDB">Dest CT database</param>
        /// <param name="CTID">CT batch ID this is for</param>
        private void CopyChangeTables(IEnumerable<TableConf> tables, string sourceCTDB, string destCTDB, Int64 CTID, bool isConsolidated = false) {
            if (Config.Slave == Config.RelayServer && sourceCTDB == destCTDB) {
                logger.Log("Skipping download because slave is equal to relay.", LogLevel.Debug);
                return;
            }

            var actions = new List<Action>();
            foreach (TableConf t in tables) {
                IDataCopy dataCopy = DataCopyFactory.GetInstance(Config.RelayType, Config.SlaveType, sourceDataUtils, destDataUtils, logger);
                var ct = new ChangeTable(t.Name, CTID, t.SchemaName, Config.Slave);
                string sourceCTTable = isConsolidated ? ct.consolidatedName : ct.ctName;
                string destCTTable = ct.ctName;
                TableConf tLocal = t;
                Action act = () => {
                    try {
                        //hard coding timeout at 1 hour for bulk copy
                        logger.Log(new { message = "Copying table to slave", Table = tLocal.SchemaName + "." + sourceCTTable }, LogLevel.Trace);
                        var sw = Stopwatch.StartNew();
                        dataCopy.CopyTable(sourceCTDB, sourceCTTable, tLocal.SchemaName, destCTDB, Config.DataCopyTimeout, destCTTable, tLocal.Name);
                        logger.Log(new { message = "CopyTable: " + sw.Elapsed, Table = tLocal.SchemaName + "." + sourceCTTable }, LogLevel.Trace);
                    } catch (DoesNotExistException) {
                        //this is a totally normal and expected case since we only publish changetables when data actually changed
                        logger.Log("No changes to pull for table " + tLocal.SchemaName + "." + sourceCTTable + " because it does not exist ", LogLevel.Debug);
                    } catch (Exception e) {
                        HandleException(e, tLocal);
                    }
                };
                actions.Add(act);
            }
            logger.Log("Parallel invocation of " + actions.Count + " changetable downloads", LogLevel.Trace);
            var options = new ParallelOptions();
            options.MaxDegreeOfParallelism = Config.MaxThreads;
            Parallel.Invoke(options, actions.ToArray());
            return;
        }

        public void ApplySchemaChanges(string sourceDB, string destDB, Int64 CTID) {
            //get list of schema changes from tblCTSChemaChange_ctid on the relay server/db
            DataTable result = sourceDataUtils.GetSchemaChanges(sourceDB, CTID);

            if (result == null) {
                return;
            }

            foreach (DataRow row in result.Rows) {
                var schemaChange = new SchemaChange(row);
                //String.Compare method returns 0 if the strings are equal
                TableConf table = Config.Tables.SingleOrDefault(item => String.Compare(item.Name, schemaChange.TableName, ignoreCase: true) == 0);

                if (table == null) {
                    logger.Log(new { message = "Ignoring schema change for untracked table", Table = schemaChange.TableName }, LogLevel.Debug);
                    continue;
                }

                logger.Log("Processing schema change (CscID: " + row.Field<int>("CscID") +
                    ") of type " + schemaChange.EventType + " for table " + table.Name, LogLevel.Info);

                if (table.ColumnList == null || table.ColumnList.Contains(schemaChange.ColumnName, StringComparer.OrdinalIgnoreCase)) {
                    logger.Log("Schema change applies to a valid column, so we will apply it", LogLevel.Info);
                    try {
                        ApplySchemaChange(destDB, table, schemaChange);
                    } catch (Exception e) {
                        var wrappedExc = new Exception(schemaChange.ToString(), e);
                        HandleException(wrappedExc, table);
                    }
                } else {
                    logger.Log("Skipped schema change because the column it impacts is not in our list", LogLevel.Info);
                }

            }
        }

        private void ApplySchemaChange(string destDB, TableConf table, SchemaChange schemaChange) {
            switch (schemaChange.EventType) {
                case SchemaChangeType.Rename:
                    logger.Log("Renaming column " + schemaChange.ColumnName + " to " + schemaChange.NewColumnName, LogLevel.Info);
                    destDataUtils.RenameColumn(table, destDB, schemaChange.SchemaName, schemaChange.TableName,
                        schemaChange.ColumnName, schemaChange.NewColumnName);
                    break;
                case SchemaChangeType.Modify:
                    logger.Log("Changing data type on column " + schemaChange.ColumnName, LogLevel.Info);
                    destDataUtils.ModifyColumn(table, destDB, schemaChange.SchemaName, schemaChange.TableName, schemaChange.ColumnName, schemaChange.DataType.ToString());
                    break;
                case SchemaChangeType.Add:
                    logger.Log("Adding column " + schemaChange.ColumnName, LogLevel.Info);
                    destDataUtils.AddColumn(table, destDB, schemaChange.SchemaName, schemaChange.TableName, schemaChange.ColumnName, schemaChange.DataType.ToString());
                    break;
                case SchemaChangeType.Drop:
                    logger.Log("Dropping column " + schemaChange.ColumnName, LogLevel.Info);
                    destDataUtils.DropColumn(table, destDB, schemaChange.SchemaName, schemaChange.TableName, schemaChange.ColumnName);
                    break;
            }
        }

    }
}
