#region Using Statements
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Xml;
using System.Data.SqlTypes;
using Xunit;
using System.Diagnostics;
using TeslaSQL.DataUtils;
using TeslaSQL.DataCopy;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Concurrent;
#endregion

namespace TeslaSQL.Agents {
    public class Master : Agent {
        protected ChangeTrackingBatch ctb;

        public Master(IDataUtils sourceDataUtils, IDataUtils destDataUtils, Logger logger)
            : base(sourceDataUtils, destDataUtils, logger) {
        }

        public Master() {
            //this constructor is only used by for running unit tests
            this.logger = new Logger(null, null, null, "");
        }

        public string TimingKey {
            get {
                return string.Format("db.mssql_changetracking_counters.TeslaRunDuration.{0}.{1}.{2}", Config.Master.Replace('.', '_'), AgentType.Master, Config.MasterDB);
            }
        }

        private string StepTimingKey(string stepName) {
            return string.Format("db.mssql_changetracking_counters.{0}.{1}.{2}", Config.Master.Replace('.', '_'), Config.MasterDB, stepName);
        }

        public override void ValidateConfig() {
            logger.Log("Validating configuration for master", LogLevel.Trace);
            Config.ValidateRequiredHost(Config.RelayServer);
            Config.ValidateRequiredHost(Config.Master);
            if (Config.RelayType == SqlFlavor.None || Config.MasterType == SqlFlavor.None) {
                throw new Exception("Master agent requires a valid SQL flavor for relay and master");
            }
        }

        public override void Run() {
            Stopwatch sw;
            DateTime start = DateTime.Now;
            logger.Log("Getting CHANGE_TRACKING_CURRENT_VERSION from master", LogLevel.Trace);
            Int64 currentVersion = sourceDataUtils.GetCurrentCTVersion(Config.MasterDB);

            logger.Log("Initializing CT batch", LogLevel.Debug);
            //set up the variables and CT version info for this run
            ctb = InitializeBatch(currentVersion);
            if (Config.Sharding && ctb == null) {
                logger.Log("Last batch completed and there is no new batch to work on.", LogLevel.Info);
                return;
            }
            Logger.SetProperty("CTID", ctb.CTID);
            logger.Log(ctb, LogLevel.Debug);

            logger.Log("Working on CTID " + ctb.CTID, LogLevel.Debug);
            DateTime previousSyncStartTime;
            IDictionary<string, Int64> changesCaptured;

            if ((ctb.SyncBitWise & Convert.ToInt32(SyncBitWise.PublishSchemaChanges)) == 0) {
                sw = Stopwatch.StartNew();
                logger.Log("Beginning publish schema changes phase", LogLevel.Info);

                logger.Log("Creating tblCTSchemaChange_" + ctb.CTID + " on relay server", LogLevel.Trace);
                destDataUtils.CreateSchemaChangeTable(Config.RelayDB, ctb.CTID);

                //get the start time of the last batch where we successfully uploaded changes
                logger.Log("Finding start time of the most recent successful batch on relay server", LogLevel.Trace);
                previousSyncStartTime = destDataUtils.GetLastStartTime(Config.RelayDB, ctb.CTID, Convert.ToInt32(SyncBitWise.UploadChanges), AgentType.Master);
                logger.Log("Retrieved previousSyncStartTime of " + previousSyncStartTime + " from relay server", LogLevel.Trace);

                logger.Log("Publishing schema changes from master to relay server", LogLevel.Debug);
                PublishSchemaChanges(Config.Tables, Config.MasterDB, Config.RelayDB, ctb.CTID, previousSyncStartTime);
                logger.Log("Successfully published schema changes, persisting bitwise value now", LogLevel.Debug);

                logger.Log("Writing bitwise value of " + Convert.ToInt32(SyncBitWise.PublishSchemaChanges) + " to tblCTVersion", LogLevel.Trace);
                destDataUtils.WriteBitWise(Config.RelayDB, ctb.CTID, Convert.ToInt32(SyncBitWise.PublishSchemaChanges), AgentType.Master);
                logger.Timing(StepTimingKey("PublishSchemaChanges"), (int)sw.ElapsedMilliseconds);
            }

            logger.Log("Calculating field lists for configured tables", LogLevel.Trace);
            SetFieldLists(Config.MasterDB, Config.Tables, sourceDataUtils);

            if ((ctb.SyncBitWise & Convert.ToInt32(SyncBitWise.CaptureChanges)) == 0) {
                sw = Stopwatch.StartNew();
                logger.Log("Beginning capture changes phase", LogLevel.Info);
                logger.Log("Resizing batch based on batch threshold", LogLevel.Trace);
                Int64 resizedStopVersion = ResizeBatch(ctb.SyncStartVersion, ctb.SyncStopVersion, currentVersion, Config.MaxBatchSize,
                    Config.ThresholdIgnoreStartTime, Config.ThresholdIgnoreEndTime, DateTime.Now);

                if (resizedStopVersion != ctb.SyncStopVersion) {
                    logger.Log("Resized batch due to threshold. Stop version changed from " + ctb.SyncStopVersion +
                        " to " + resizedStopVersion, LogLevel.Debug);
                    ctb.SyncStopVersion = resizedStopVersion;

                    logger.Log("Writing new stopVersion back to tblCTVersion", LogLevel.Trace);
                    destDataUtils.UpdateSyncStopVersion(Config.RelayDB, resizedStopVersion, ctb.CTID);
                }

                logger.Log("Beginning creation of CT tables", LogLevel.Debug);
                changesCaptured = CreateChangeTables(Config.MasterDB, Config.MasterCTDB, ctb);
                logger.Log("Changes captured successfully, persisting bitwise value to tblCTVersion", LogLevel.Debug);

                destDataUtils.WriteBitWise(Config.RelayDB, ctb.CTID, Convert.ToInt32(SyncBitWise.CaptureChanges), AgentType.Master);
                logger.Log("Wrote bitwise value of " + Convert.ToInt32(SyncBitWise.CaptureChanges) + " to tblCTVersion", LogLevel.Trace);
                logger.Timing(StepTimingKey("CaptureChanges"), (int)sw.ElapsedMilliseconds);
            } else {
                logger.Log("CreateChangeTables succeeded on the previous run, running GetRowCounts instead to populate changesCaptured object", LogLevel.Debug);
                changesCaptured = GetRowCounts(Config.Tables, Config.MasterCTDB, ctb.CTID);
                logger.Log("Successfully populated changesCaptured with a list of rowcounts for each changetable", LogLevel.Trace);
            }
            sw = Stopwatch.StartNew();
            //copy change tables from master to relay server
            logger.Log("Beginning publish changetables step, copying CT tables to the relay server", LogLevel.Info);
            PublishChangeTables(Config.MasterCTDB, Config.RelayDB, ctb.CTID, changesCaptured);
            logger.Log("Publishing info table", LogLevel.Info);
            PublishTableInfo(Config.Tables, Config.RelayDB, changesCaptured, ctb.CTID);
            logger.Log("Successfully published changetables, persisting bitwise now", LogLevel.Debug);

            //this signifies the end of the master's responsibility for this batch
            destDataUtils.WriteBitWise(Config.RelayDB, ctb.CTID, Convert.ToInt32(SyncBitWise.UploadChanges), AgentType.Master);
            logger.Log("Wrote bitwise value of " + Convert.ToInt32(SyncBitWise.UploadChanges) + " to tblCTVersion", LogLevel.Trace);
            logger.Timing(StepTimingKey("UploadChanges"), (int)sw.ElapsedMilliseconds);

            logger.Log("Master agent work complete", LogLevel.Info);
            var elapsed = DateTime.Now - start;
            logger.Timing(TimingKey, (int)elapsed.TotalMinutes);

            sourceDataUtils.CleanUpInitializeTable(Config.MasterCTDB, ctb.SyncStartTime.Value);
            return;
        }



        /// <summary>
        /// Initializes version/batch info for a run and creates CTID
        /// </summary>
        /// <param name="currentVersion">current change tracking version on the master</param>
        /// <returns>boolean, which lets the agent know whether or not it should continue creating changetables</returns>
        protected ChangeTrackingBatch InitializeBatch(Int64 currentVersion) {
            logger.Log("Retrieving information about the most recently worked on batch from tblCTVersion", LogLevel.Trace);
            var lastBatch = destDataUtils.GetLastCTBatch(Config.RelayDB, AgentType.Master);

            if (lastBatch == null) {
                logger.Log("No existing batches found, tblCTVersion was empty", LogLevel.Debug);
                //TODO figure out a better way to handle this case, determine an appropriate syncStartVersion. Perhaps use 0 and specially handle that using
                //CHANGE_TRACKING_MIN_VALID_VERSION?
                throw new Exception("Unable to determine appropriate syncStartVersion - version table seems to be empty.");
            }

            if ((lastBatch.Field<Int32>("syncBitWise") & Convert.ToInt32(SyncBitWise.UploadChanges)) > 0) {
                if (Config.Sharding) {
                    return null;
                }
                logger.Log("Last batch succeeded, creating a new one where that left off", LogLevel.Debug);
                Int64 syncStartVersion = lastBatch.Field<Int64>("syncStopVersion");
                var batch = destDataUtils.CreateCTVersion(Config.RelayDB, syncStartVersion, currentVersion);
                logger.Log(new { message = "Created new CT batch", CTID = batch.CTID }, LogLevel.Debug);
                return batch;
            } else if ((lastBatch.Field<Int32>("syncBitWise") & Convert.ToInt32(SyncBitWise.CaptureChanges)) == 0) {
                logger.Log("Last batch failed before creating CT tables. Updating syncStopVersion to avoid falling too far behind", LogLevel.Debug);
                destDataUtils.UpdateSyncStopVersion(Config.RelayDB, currentVersion, lastBatch.Field<Int64>("CTID"));
                logger.Log("New syncStopVersion is the current change tracking version on the master, " + currentVersion, LogLevel.Trace);
                return new ChangeTrackingBatch(lastBatch.Field<Int64>("CTID"),
                    lastBatch.Field<Int64>("syncStartVersion"),
                    currentVersion,
                    lastBatch.Field<Int32>("syncBitWise"),
                    lastBatch.Field<DateTime>("syncStartTime"));
            } else {
                logger.Log("Previous batch failed overall but did create its changetables, so we'll try to publish them once again", LogLevel.Debug);
                return new ChangeTrackingBatch(lastBatch);
            }
        }


        /// <summary>
        /// Publish schema changes that hav eoccurred since the last batch started
        /// </summary>
        /// <param name="tables">Array of table config objects</param>
        /// <param name="sourceDB">Source database name</param>
        /// <param name="destDB">Destination database name</param>
        /// <param name="CTID">Change tracking batch id</param>
        /// <param name="afterDate">Date to pull schema changes after</param>
        public void PublishSchemaChanges(IEnumerable<TableConf> tables, string sourceDB, string destDB, Int64 CTID, DateTime afterDate) {
            logger.Log("Pulling DDL events from master since " + Convert.ToString(afterDate), LogLevel.Debug);
            DataTable ddlEvents = sourceDataUtils.GetDDLEvents(sourceDB, afterDate);
            var schemaChanges = new List<SchemaChange>();
            DDLEvent dde;
            foreach (DataRow row in ddlEvents.Rows) {
                logger.Log("Processing DDLevent...", LogLevel.Trace);
                dde = new DDLEvent(row.Field<int>("DdeID"), row.Field<string>("DdeEventData"));
                logger.Log("Event initialized. DDEID is " + Convert.ToString(dde.DdeID), LogLevel.Trace);

                //a DDL event can yield 0 or more schema change events, hence the List<SchemaChange>
                logger.Log("Parsing DDL event XML", LogLevel.Trace);
                schemaChanges = dde.Parse(tables, sourceDataUtils, sourceDB);

                //iterate through any schema changes for this event and write them to tblCTSchemaChange_CTID
                foreach (SchemaChange schemaChange in schemaChanges) {
                    logger.Log("Publishing schema change for DdeID " + Convert.ToString(schemaChange.DdeID) + " of type " + Convert.ToString(schemaChange.EventType) +
                    " for table " + schemaChange.TableName + ", column " + schemaChange.ColumnName, LogLevel.Trace);

                    destDataUtils.WriteSchemaChange(destDB, CTID, schemaChange);
                }
            }
        }


        /// <summary>
        /// Resize batch based on max batch size configuration variable
        /// </summary>
        /// <param name="startVersion">syncStartVersion for this CT run</param>
        /// <param name="stopVersion">syncStopVersion for this CT run</param>
        /// <param name="curVersion">CHANGE_TRACKING_CURRENT_VERSION() on the master</param>
        /// <param name="maxBatchSize">Configured max batch size (might not be specified, would default to 0)</param>
        /// <param name="thresholdIgnoreStartTime">Beginning of timespan in which to ignore the max batch size (optional)</param>
        /// <param name="thresholdIgnoreEndTime">End of timespan in which to ignore the max batch size (optional)</param>
        /// <param name="CurrentDate">The current date. Mostly a parameter for the purposes of unit testing.</param>
        protected Int64 ResizeBatch(Int64 startVersion, Int64 stopVersion, Int64 curVersion, int maxBatchSize, TimeSpan? thresholdIgnoreStartTime, TimeSpan? thresholdIgnoreEndTime, DateTime CurrentDate) {
            //if max batch is not specified or if this batch is not large, we don't have to do anything
            if (maxBatchSize > 0 && stopVersion - startVersion > maxBatchSize) {
                logger.Log("Batch is susceptible to resizing since the difference between stopVersion and startVersion is larger than configured maxBatchSize", LogLevel.Trace);
                //handle interval wrapping around midnight
                if (thresholdIgnoreStartTime > thresholdIgnoreEndTime) {
                    logger.Log("The threshold for ignoring batch size limitations wraps around midnight", LogLevel.Trace);
                    //if the time span for ignoring batch resizing wraps around midnight, we check whether the current time is
                    //after the start OR before the end (since it can't be both if the interval contains midnight)
                    if (CurrentDate.TimeOfDay > thresholdIgnoreStartTime || CurrentDate.TimeOfDay <= thresholdIgnoreEndTime) {
                        logger.Log("We are currently in the time window for ignoring batch size limitations, not changing the batch size", LogLevel.Trace);
                        stopVersion = curVersion;
                    } else {
                        logger.Log("We are not in the time window for ignoring batch size limitations, resizing the batch", LogLevel.Trace);
                        stopVersion = startVersion + maxBatchSize;
                    }
                } else {
                    logger.Log("Threshold window for batch size limitations isn't configured or doesn't contain midnight", LogLevel.Trace);
                    //simpler case when the time span doesn't contain midnight
                    if (CurrentDate.TimeOfDay > thresholdIgnoreStartTime && CurrentDate.TimeOfDay <= thresholdIgnoreEndTime) {
                        logger.Log("We are currently in the time window for ignoring batch size limitations, not changing the batch size", LogLevel.Trace);
                        stopVersion = curVersion;
                    } else {
                        logger.Log("We are not in the time window for ignoring batch size limitations, resizing the batch", LogLevel.Trace);
                        stopVersion = startVersion + maxBatchSize;
                    }
                }
            }
            return stopVersion;
        }


        /// <summary>
        /// Loops through passed in array of table objects, creates changedata tables for each one on the CT DB
        /// </summary>
        /// <param name="tables">Array of table config objects to create CT tables for</param>
        /// <param name="sourceDB">Database the source data lives in</param>
        /// <param name="sourceCTDB">Database the changetables should go to</param>
        /// <param name="startVersion">Change tracking version to start with</param>
        /// <param name="stopVersion">Change tracking version to stop at</param>
        /// <param name="CTID">CT batch ID this is being run for</param>
        protected IDictionary<string, Int64> CreateChangeTables(string sourceDB, string sourceCTDB, ChangeTrackingBatch batch) {
            var changesCaptured = new ConcurrentDictionary<string, Int64>();
            var actions = new List<Action>();
            foreach (TableConf t in Config.Tables) {
                //local variables inside the loop required for the action to bind properly
                TableConf table = t;
                long rowsAffected;
                Action act = () => {
                    logger.Log("Creating changetable for " + table.SchemaName + "." + table.Name, LogLevel.Debug);
                    rowsAffected = CreateChangeTable(table, sourceDB, sourceCTDB, batch);
                    changesCaptured.TryAdd(table.SchemaName + "." + table.Name, rowsAffected);
                    logger.Log(rowsAffected + " changes captured for table " + table.SchemaName + "." + table.Name, LogLevel.Trace);
                };
                actions.Add(act);
            }
            var options = new ParallelOptions();
            options.MaxDegreeOfParallelism = Config.MaxThreads;
            logger.Log("Parallel invocation of " + actions.Count + " change captures", LogLevel.Trace);
            Parallel.Invoke(options, actions.ToArray());
            return changesCaptured;
        }


        /// <summary>
        /// Creates changetable for an individual table
        /// </summary>
        /// <param name="table">Config table object to create changes for</param>
        /// <param name="sourceDB">Database the source data lives in</param>
        /// <param name="sourceCTDB">Database the changetables should go to</param>
        /// <param name="batch">Batch to work on</param>
        protected long CreateChangeTable(TableConf table, string sourceDB, string sourceCTDB, ChangeTrackingBatch batch) {
            string ctTableName = table.ToCTName(batch.CTID);
            string reason;

            long tableStartVersion = batch.SyncStartVersion;
            long minValidVersion = sourceDataUtils.GetMinValidVersion(sourceDB, table.Name, table.SchemaName);
            if (batch.SyncStartVersion == 0) {
                tableStartVersion = minValidVersion;
            }

            if (sourceDataUtils.IsBeingInitialized(sourceCTDB, table)) {
                return 0;
            }

            long? initializeVersion = sourceDataUtils.GetInitializeStartVersion(sourceCTDB, table);
            if (initializeVersion.HasValue) {
                tableStartVersion = initializeVersion.Value;
            }

            if (!ValidateSourceTable(sourceDB, table.Name, table.SchemaName, tableStartVersion, minValidVersion, out reason)) {
                string message = "Change table creation impossible because : " + reason;
                if (table.StopOnError) {
                    throw new Exception(message);
                } else {
                    logger.Log(message, LogLevel.Error);
                    return 0;
                }
            }

            logger.Log("Dropping table " + ctTableName + " if it exists", LogLevel.Trace);
            sourceDataUtils.DropTableIfExists(sourceCTDB, ctTableName, table.SchemaName);

            logger.Log("Calling SelectIntoCTTable to create CT table", LogLevel.Trace);
            Int64 rowsAffected = sourceDataUtils.SelectIntoCTTable(sourceCTDB, table, sourceDB, batch, Config.QueryTimeout, tableStartVersion);

            logger.Log("Rows affected for table " + table.SchemaName + "." + table.Name + ": " + Convert.ToString(rowsAffected), LogLevel.Debug);
            return rowsAffected;
        }

        /// <summary>
        /// Checks that a table is valid to pull changes from (exists, has a primary key, has change tracking enabled, and has a low enough min_valid_version
        /// </summary>
        /// <param name="dbName">Database name</param>
        /// <param name="table">Table name</param>
        /// <param name="startVersion">Start version to compare to min_valid_version</param>
        /// <param name="reason">Outputs a reason for why the table isn't valid, if it isn't valid.</param>
        /// <returns>Bool indicating whether it's safe to pull changes for this table</returns>
        protected bool ValidateSourceTable(string dbName, string table, string schemaName, Int64 startVersion, Int64 minValidVersion, out string reason) {
            if (!sourceDataUtils.CheckTableExists(dbName, table, schemaName)) {
                reason = "Table " + table + " does not exist in the source database";
                logger.Log(reason, LogLevel.Trace);
                return false;
            } else if (!sourceDataUtils.HasPrimaryKey(dbName, table, schemaName)) {
                reason = "Table " + table + " has no primary key in the source database";
                logger.Log(reason, LogLevel.Trace);
                return false;
            } else if (!sourceDataUtils.IsChangeTrackingEnabled(dbName, table, schemaName)) {
                reason = "Change tracking is not enabled on " + table;
                logger.Log(reason, LogLevel.Trace);
                return false;
            } else if (startVersion < minValidVersion) {
                reason = "Change tracking is far enough out of date that the syncStartVersion is less than the current minimum valid CT version on " + table;
                logger.Log(reason, LogLevel.Trace);
                return false;
            }
            logger.Log("Table " + table + " seems valid for change tracking", LogLevel.Trace);
            reason = "";
            return true;
        }


        /// <summary>
        /// Copies change tables from the master to the relay server
        /// </summary>
        /// <param name="tables">Array of table config objects</param>
        /// <param name="sourceServer">Source server identifer</param>
        /// <param name="sourceCTDB">Source CT database</param>
        /// <param name="destCTDB">Dest CT database</param>
        /// <param name="CTID">CT batch ID this is for</param>
        protected void PublishChangeTables(string sourceCTDB, string destCTDB, Int64 CTID, IDictionary<string, Int64> changesCaptured) {
            if (Config.Master != null && Config.Master == Config.RelayServer && sourceCTDB == destCTDB) {
                logger.Log("Skipping publish because master is equal to relay.", LogLevel.Debug);
                return;
            }

            var actions = new List<Action>();
            foreach (TableConf t in Config.Tables) {
                if (changesCaptured[t.SchemaName + "." + t.Name] > 0) {
                    //we need to define a local variable in this scope for it to be appropriately evaluated in the action
                    TableConf localT = t;
                    Action act = () => PublishChangeTable(localT, sourceCTDB, destCTDB, CTID);
                    actions.Add(act);
                }
            }
            logger.Log("Parallel invocation of " + actions.Count + " changetable publishes", LogLevel.Trace);
            var options = new ParallelOptions();
            options.MaxDegreeOfParallelism = Config.MaxThreads;
            Parallel.Invoke(options, actions.ToArray());
        }

        protected void PublishChangeTable(TableConf table, string sourceCTDB, string destCTDB, Int64 CTID) {
            IDataCopy dataCopy = DataCopyFactory.GetInstance((SqlFlavor)Config.MasterType, (SqlFlavor)Config.RelayType, sourceDataUtils, destDataUtils, logger);
            logger.Log("Publishing changes for table " + table.SchemaName + "." + table.Name, LogLevel.Trace);
            try {
                dataCopy.CopyTable(sourceCTDB, table.ToCTName(CTID), table.SchemaName, destCTDB, Config.DataCopyTimeout);
                logger.Log("Publishing changes succeeded for " + table.SchemaName + "." + table.Name, LogLevel.Trace);
            } catch (Exception e) {
                if (table.StopOnError) {
                    throw;
                } else {
                    logger.Log("Copying change data for table " + table.SchemaName + "." + table.Name + " failed with error: " + e.Message, LogLevel.Error);
                }
            }
        }

        public override void SetFieldLists(string database, IEnumerable<TableConf> tableConfs, IDataUtils dataUtils) {
            var allFieldLists = dataUtils.GetAllFields(database, tableConfs.ToDictionary(t => t, t => t.Name));
            Dictionary<TableConf, IEnumerable<string>> primaryKeys = dataUtils.GetAllPrimaryKeysMaster(database, tableConfs);

            //tableCTName.Keys instead of tables because we've already filtered this for tables that don't have change tables
            //note: allColumnsByTable.Keys or primaryKeysByTable.Keys should work just as well
            foreach (var table in tableConfs) {
                var columns = allFieldLists[table].ToDictionary(c => c, c => false);
                var pks = primaryKeys[table];
                foreach (var pk in pks) {
                    columns[pk] = true;
                }
                SetFieldList(table, columns);
            }
        }

    }
}
