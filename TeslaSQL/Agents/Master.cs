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
#endregion

namespace TeslaSQL.Agents {
    public class Master : Agent {

        //base keyword invokes the base class's constructor
        public Master(Config config, IDataUtils dataUtils) : base(config, dataUtils) {

        }

        public Master() {
            //this constructor is only used by for running unit tests
            this.logger = new Logger(LogLevel.Critical, null, null, null);
        }

        public override void ValidateConfig() {
            logger.Log("Validating configuration for master", LogLevel.Trace);
            config.ValidateRequiredHost(config.relayServer);
            config.ValidateRequiredHost(config.master);
            if (config.relayType == null || config.masterType == null) {
                throw new Exception("Master agent requires a valid SQL flavor for relay and master");
            }
        }

        public override void Run() {
            logger.Log("Getting CHANGE_TRACKING_CURRENT_VERSION from master", LogLevel.Trace);
            Int64 currentVersion = dataUtils.GetCurrentCTVersion(TServer.MASTER, config.masterDB);

            logger.Log("Initializing CT batch", LogLevel.Debug);
            //set up the variables and CT version info for this run
            ChangeTrackingBatch ctb = InitializeBatch(currentVersion);

            DateTime previousSyncStartTime;
            Dictionary<string, Int64> changesCaptured;

            if ((ctb.syncBitWise & Convert.ToInt32(SyncBitWise.PublishSchemaChanges)) == 0) {
                logger.Log("Beginning publish schema changes phase", LogLevel.Info);

                logger.Log("Creating tblCTSchemaChange_<CTID> on relay server", LogLevel.Trace);
                dataUtils.CreateSchemaChangeTable(TServer.RELAY, config.relayDB, ctb.CTID);

                //get the start time of the last batch where we successfully uploaded changes
                logger.Log("Finding start time of the most recent successful batch on relay server", LogLevel.Trace);
                previousSyncStartTime = dataUtils.GetLastStartTime(TServer.RELAY, config.relayDB, ctb.CTID, Convert.ToInt32(SyncBitWise.UploadChanges));
                logger.Log("Retrieved previousSyncStartTime of " + Convert.ToString(previousSyncStartTime) + " from relay server", LogLevel.Trace);

                logger.Log("Publishing schema changes from master to relay server", LogLevel.Debug);
                PublishSchemaChanges(config.tables, TServer.MASTER, config.masterDB, TServer.RELAY, config.relayDB, ctb.CTID, previousSyncStartTime);
                logger.Log("Successfully published schema changes, persisting bitwise value now", LogLevel.Debug);

                //update bitwise on tblCTVersion, indicating that we have completed the schema change publishing step
                logger.Log("Writing bitwise value of " + Convert.ToString(Convert.ToInt32(SyncBitWise.PublishSchemaChanges)) + " to tblCTVersion", LogLevel.Trace);
                dataUtils.WriteBitWise(TServer.RELAY, config.relayDB, ctb.CTID, Convert.ToInt32(SyncBitWise.PublishSchemaChanges), AgentType.Master);
            }

            if ((ctb.syncBitWise & Convert.ToInt32(SyncBitWise.CaptureChanges)) == 0) {
                logger.Log("Beginning capture changes phase", LogLevel.Info);

                //set the field list values on the table config objects
                logger.Log("Calculating field lists for configured tables", LogLevel.Trace);
                SetFieldLists(TServer.MASTER, config.masterDB, config.tables);

                //resize batch based on batch threshold
                logger.Log("Resizing batch based on batch threshold", LogLevel.Trace);
                Int64 resizedStopVersion = ResizeBatch(ctb.syncStartVersion, ctb.syncStopVersion, currentVersion, config.maxBatchSize,
                    config.thresholdIgnoreStartTime, config.thresholdIgnoreEndTime, DateTime.Now);

                if (resizedStopVersion != ctb.syncStopVersion) {
                    logger.Log("Resized batch due to threshold. Stop version changed from " + Convert.ToString(ctb.syncStopVersion) +
                        " to " + Convert.ToString(resizedStopVersion), LogLevel.Debug);
                    ctb.syncStopVersion = resizedStopVersion;

                    logger.Log("Writing new stopVersion back to tblCTVersion", LogLevel.Trace);
                    dataUtils.UpdateSyncStopVersion(TServer.RELAY, config.relayDB, resizedStopVersion, ctb.CTID);
                }

                logger.Log("Beginning creation of CT tables", LogLevel.Debug);
                changesCaptured = CreateChangeTables(config.tables, TServer.MASTER, config.masterDB, config.masterCTDB, ctb.syncStartVersion, ctb.syncStopVersion, ctb.CTID);
                logger.Log("Changes captured successfully, persisting bitwise value to tblCTVersion", LogLevel.Debug);

                //update bitwise on tblCTVersion, indicating that we have completed the change table creation step
                dataUtils.WriteBitWise(TServer.RELAY, config.relayDB, ctb.CTID, Convert.ToInt32(SyncBitWise.CaptureChanges), AgentType.Master);
                logger.Log("Wrote bitwise value of " + Convert.ToString(Convert.ToInt32(SyncBitWise.CaptureChanges)) + " to tblCTVersion", LogLevel.Trace);
            } else {
                logger.Log("CreateChangeTables succeeded on the previous run, running GetRowCounts instead to populate changesCaptured object", LogLevel.Debug);
                changesCaptured = GetRowCounts(config.tables, TServer.MASTER, config.masterCTDB, ctb.CTID);
                logger.Log("Successfully populated changesCaptured with a list of rowcounts for each changetable", LogLevel.Trace);
            }

            //copy change tables from master to relay server
            logger.Log("Beginning publish changetables step, copying CT tables to the relay server", LogLevel.Info);
            PublishChangeTables(config.tables, TServer.MASTER, config.masterCTDB, TServer.RELAY, config.relayDB, ctb.CTID, changesCaptured);
            logger.Log("Successfully published changetables, persisting bitwise now", LogLevel.Debug);

            //this signifies the end of the master's responsibility for this batch
            dataUtils.WriteBitWise(TServer.RELAY, config.relayDB, ctb.CTID, Convert.ToInt32(SyncBitWise.UploadChanges), AgentType.Master);
            logger.Log("Wrote bitwise value of " + Convert.ToString(Convert.ToInt32(SyncBitWise.UploadChanges)) + " to tblCTVersion", LogLevel.Trace);

            logger.Log("Master agent work complete", LogLevel.Info);
            return;
        }


        /// <summary>
        /// Initializes version/batch info for a run and creates CTID
        /// </summary>
        /// <param name="currentVersion">current change tracking version on the master</param>
        /// <returns>boolean, which lets the agent know whether or not it should continue creating changetables</returns>
        private ChangeTrackingBatch InitializeBatch(Int64 currentVersion) {
            logger.Log("Retrieving information about the most recently worked on batch from tblCTVersion", LogLevel.Trace);
            DataRow lastbatch = dataUtils.GetLastCTBatch(TServer.RELAY, config.relayDB, AgentType.Master);

            if (lastbatch == null) {
                logger.Log("No existing batches found, tblCTVersion was empty", LogLevel.Debug);
                //TODO figure out a better way to handle this case, determine an appropriate syncStartVersion. Perhaps use 0 and specially handle that using
                //CHANGE_TRACKING_MIN_VALID_VERSION?
                throw new Exception("Unable to determine appropriate syncStartVersion - version table seems to be empty.");
            }

            if ((lastbatch.Field<Int32>("syncBitWise") & Convert.ToInt32(SyncBitWise.UploadChanges)) > 0) {
                logger.Log("Last batch succeeded, creating a new one where that left off", LogLevel.Debug);
                Int64 syncStartVersion = lastbatch.Field<Int64>("syncStopVersion");

                Int64 CTID = dataUtils.CreateCTVersion(TServer.RELAY, config.relayDB, syncStartVersion, currentVersion);
                logger.Log("Created CTID " + Convert.ToString(CTID), LogLevel.Debug);
                return new ChangeTrackingBatch(CTID, syncStartVersion, currentVersion, 0);
            } else if ((lastbatch.Field<Int32>("syncBitWise") & Convert.ToInt32(SyncBitWise.CaptureChanges)) == 0) {
                logger.Log("Last batch failed before creating CT tables. Updating syncStopVersion to avoid falling too far behind", LogLevel.Debug);
                dataUtils.UpdateSyncStopVersion(TServer.RELAY, config.relayDB, currentVersion, lastbatch.Field<Int64>("CTID"));
                logger.Log("New syncStopVersion is the current change tracking version on the master, " + Convert.ToString(currentVersion), LogLevel.Trace);
                return new ChangeTrackingBatch(lastbatch.Field<Int64>("CTID"),
                    lastbatch.Field<Int64>("syncStartVersion"),
                    currentVersion,
                    lastbatch.Field<Int32>("syncBitWise"));
            } else {
                logger.Log("Previous batch failed overall but did create its changetables, so we'll try to publish them once again", LogLevel.Debug);
                return new ChangeTrackingBatch(lastbatch.Field<Int64>("CTID"),
                    lastbatch.Field<Int64>("syncStartVersion"),
                    lastbatch.Field<Int64>("syncStopVersion"),
                    lastbatch.Field<Int32>("syncBitWise"));
            }
        }


        /// <summary>
        /// Publish schema changes that hav eoccurred since the last batch started
        /// </summary>
        /// <param name="tables">Array of table config objects</param>
        /// <param name="sourceServer">Server identifier to pull from</param>
        /// <param name="sourceDB">Source database name</param>
        /// <param name="destServer">Server identifier to write to</param>
        /// <param name="destDB">Destination database name</param>
        /// <param name="CTID">Change tracking batch id</param>
        /// <param name="afterDate">Date to pull schema changes after</param>
        public void PublishSchemaChanges(TableConf[] tables, TServer sourceServer, string sourceDB, TServer destServer, string destDB, Int64 CTID, DateTime afterDate) {
            logger.Log("Pulling DDL events from master since " + Convert.ToString(afterDate), LogLevel.Debug);
            DataTable ddlEvents = dataUtils.GetDDLEvents(sourceServer, sourceDB, afterDate);
            var schemaChanges = new List<SchemaChange>();
            DDLEvent dde;
            foreach (DataRow row in ddlEvents.Rows) {
                logger.Log("Processing DDLevent...", LogLevel.Trace);
                dde = new DDLEvent(row.Field<int>("DdeID"), row.Field<string>("DdeEventData"));
                logger.Log("Event initialized. DDEID is " + Convert.ToString(dde.ddeID), LogLevel.Trace);

                //a DDL event can yield 0 or more schema change events, hence the List<SchemaChange>
                logger.Log("Parsing DDL event XML", LogLevel.Trace);
                schemaChanges = dde.Parse(tables, dataUtils, sourceServer, sourceDB);

                //iterate through any schema changes for this event and write them to tblCTSchemaChange_CTID
                foreach (SchemaChange schemaChange in schemaChanges) {
                    logger.Log("Publishing schema change for DdeID " + Convert.ToString(schemaChange.ddeID) + " of type " + Convert.ToString(schemaChange.eventType) +
                    " for table " + schemaChange.tableName + ", column " + schemaChange.columnName, LogLevel.Trace);

                    dataUtils.WriteSchemaChange(destServer,
                        destDB,
                        CTID,
                        schemaChange.ddeID,
                        Convert.ToString(schemaChange.eventType),
                        schemaChange.schemaName,
                        schemaChange.tableName,
                        schemaChange.columnName,
                        schemaChange.newColumnName,
                        schemaChange.dataType.baseType,
                        schemaChange.dataType.characterMaximumLength,
                        schemaChange.dataType.numericPrecision,
                        schemaChange.dataType.numericScale
                    );
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
        private Int64 ResizeBatch(Int64 startVersion, Int64 stopVersion, Int64 curVersion, int maxBatchSize, TimeSpan? thresholdIgnoreStartTime, TimeSpan? thresholdIgnoreEndTime, DateTime CurrentDate) {
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
        /// <param name="sourceServer">Server to connect to</param>
        /// <param name="sourceDB">Database the source data lives in</param>
        /// <param name="sourceCTDB">Database the changetables should go to</param>
        /// <param name="startVersion">Change tracking version to start with</param>
        /// <param name="stopVersion">Change tracking version to stop at</param>
        /// <param name="CTID">CT batch ID this is being run for</param>
        private Dictionary<string, Int64> CreateChangeTables(TableConf[] tables, TServer sourceServer, string sourceDB, string sourceCTDB, Int64 startVersion, Int64 stopVersion, Int64 CTID) {
            Dictionary<string, Int64> changesCaptured = new Dictionary<string, Int64>();
            KeyValuePair<string, Int64> result;
            foreach (TableConf t in tables) {
                logger.Log("Creating changetable for " + t.schemaName + "." + t.Name, LogLevel.Debug);
                result = CreateChangeTable(t, sourceServer, sourceDB, sourceCTDB, startVersion, stopVersion, CTID);
                changesCaptured.Add(result.Key, result.Value);
                logger.Log(Convert.ToString(result.Value) + " changes captured for table " + t.schemaName + "." + t.Name, LogLevel.Trace);
            }
            return changesCaptured;
        }

        /// <summary>
        /// Checks that a table is valid to pull changes from (exists, has a primary key, has change tracking enabled, and has a low enough min_valid_version
        /// </summary>
        /// <param name="server">Server identifier</param>
        /// <param name="dbName">Database name</param>
        /// <param name="table">Table name</param>
        /// <param name="startVersion">Start version to compare to min_valid_version</param>
        /// <param name="reason">Outputs a reason for why the table isn't valid, if it isn't valid.</param>
        /// <returns>Bool indicating whether it's safe to pull changes for this table</returns>
        private bool ValidateSourceTable(TServer server, string dbName, string table, string schemaName, Int64 startVersion, out string reason) {
            if (!dataUtils.CheckTableExists(server, dbName, table, schemaName)) {
                reason = "Table " + table + " does not exist in the source database";
                logger.Log(reason, LogLevel.Trace);
                return false;
            } else if (!dataUtils.HasPrimaryKey(server, dbName, table, schemaName)) {
                reason = "Table " + table + " has no primary key in the source database";
                logger.Log(reason, LogLevel.Trace);
                return false;
            } else if (!dataUtils.IsChangeTrackingEnabled(server, dbName, table, schemaName)) {
                reason = "Change tracking is not enabled on " + table;
                logger.Log(reason, LogLevel.Trace);
                return false;
            } else if (startVersion < dataUtils.GetMinValidVersion(server, dbName, table, schemaName)) {
                reason = "Start version of " + Convert.ToString(startVersion) + " is less than CHANGE_TRACKING_MIN_VALID_VERSION for table " + table;
                logger.Log(reason, LogLevel.Trace);
                return false;
            }
            logger.Log("Table " + table + " seems valid for change tracking", LogLevel.Trace);
            reason = "";
            return true;
        }


        /// <summary>
        /// Creates changetable for an individual table
        /// </summary>
        /// <param name="t">Config table object to create changes for</param>
        /// <param name="sourceServer">Server to connect to</param>
        /// <param name="sourceDB">Database the source data lives in</param>
        /// <param name="sourceCTDB">Database the changetables should go to</param>
        /// <param name="startVersion">Change tracking version to start with</param>
        /// <param name="stopVersion">Change tracking version to stop at</param>
        /// <param name="CTID">CT batch ID this is being run for</param>
        private KeyValuePair<string, Int64> CreateChangeTable(TableConf t, TServer sourceServer, string sourceDB, string sourceCTDB, Int64 startVersion, Int64 stopVersion, Int64 CTID) {
            //TODO check tblCTInitialize and change startVersion if necessary? need to decide if we are keeping tblCTInitialize at all
            //alternative to keeping it is to have it live only on a slave which then keeps track of which batches it has applied for that table separately from the CT runs

            //TODO handle case where startVersion is 0, change it to CHANGE_TRACKING_MIN_VALID_VERSION?
            string ctTableName = CTTableName(t.Name, CTID);
            string reason;

            if (!ValidateSourceTable(sourceServer, sourceDB, t.Name, t.schemaName, startVersion, out reason)) {
                string message = "Change table creation impossible because : " + reason;
                if (t.stopOnError) {
                    throw new Exception(message);
                } else {
                    logger.Log(message, LogLevel.Error);
                    return new KeyValuePair<string, Int64>(t.schemaName + "." + t.Name, 0);
                }
            }

            //drop the table if it exists
            logger.Log("Dropping table " + ctTableName + " if it exists", LogLevel.Trace);
            bool tExisted = dataUtils.DropTableIfExists(sourceServer, sourceCTDB, ctTableName, t.schemaName);

            logger.Log("Calling SelectIntoCTTable to create CT table", LogLevel.Trace);
            Int64 rowsAffected = dataUtils.SelectIntoCTTable(sourceServer, sourceCTDB, t.schemaName, t.masterColumnList,
                ctTableName, sourceDB, t.Name, startVersion, t.pkList, stopVersion, t.notNullPKList, 1200);

            logger.Log("Rows affected for table " + t.schemaName + "." + t.Name + ": " + Convert.ToString(rowsAffected), LogLevel.Debug);
            return new KeyValuePair<string, Int64>(t.schemaName + "." + t.Name, rowsAffected);
        }


        /// <summary>
        /// Copies change tables from the master to the relay server
        /// </summary>
        /// <param name="tables">Array of table config objects</param>
        /// <param name="sourceServer">Source server identifer</param>
        /// <param name="sourceCTDB">Source CT database</param>
        /// <param name="destServer">Dest server identifier</param>
        /// <param name="destCTDB">Dest CT database</param>
        /// <param name="CTID">CT batch ID this is for</param>
        private void PublishChangeTables(TableConf[] tables, TServer sourceServer, string sourceCTDB, TServer destServer, string destCTDB, Int64 CTID, Dictionary<string, Int64> changesCaptured) {
            foreach (TableConf t in tables) {
                //don't copy tables that had no changes
                if (changesCaptured[t.schemaName + "." + t.Name] > 0) {
                    logger.Log("Publishing changes for table " + t.schemaName + "." + t.Name, LogLevel.Trace);
                    //hard coding timeout at 1 hour for bulk copy
                    try {
                        dataUtils.CopyTable(sourceServer, sourceCTDB, CTTableName(t.Name, CTID), t.schemaName, destServer, destCTDB, 36000);
                        logger.Log("Publishing changes succeeded for " + t.schemaName + "." + t.Name, LogLevel.Trace);
                    } catch (Exception e) {
                        if (t.stopOnError) {
                            throw e;
                        } else {
                            logger.Log("Copying change data for table " + t.schemaName + "." + t.Name + " failed with error: " + e.Message, LogLevel.Error);
                        }
                    }
                }
            }
        }


        /// <summary>
        /// Gets ChangesCaptured object based on row counts in CT tables
        /// </summary>
        /// <param name="tables">Array of table config objects</param>
        /// <param name="sourceServer">Server identifier for the source</param>
        /// <param name="sourceCTDB">CT database name</param>
        /// <param name="CTID">CT batch id</param>
        private Dictionary<string, Int64> GetRowCounts(TableConf[] tables, TServer sourceServer, string sourceCTDB, Int64 CTID) {
            Dictionary<string, Int64> rowCounts = new Dictionary<string, Int64>();

            foreach (TableConf t in tables) {
                logger.Log("Getting rowcount for table " + t.schemaName + "." + CTTableName(t.Name, CTID), LogLevel.Trace);
                try {
                    rowCounts.Add(t.schemaName + "." + t.Name, dataUtils.GetTableRowCount(sourceServer, sourceCTDB, CTTableName(t.Name, CTID), t.schemaName));
                    logger.Log("Successfully retrieved rowcount of " + Convert.ToString(rowCounts[t.schemaName + "." + t.Name]), LogLevel.Trace);
                } catch (DoesNotExistException) {
                    logger.Log("CT table does not exist, using rowcount of 0", LogLevel.Trace);
                    rowCounts.Add(t.schemaName + "." + t.Name, 0);
                }
            }
            return rowCounts;
        }


        #region Unit Tests
        //unit tests for ResizeBatch method
        [Fact]
        public void TestResizeBatch() {
            //test that it doesn't mess with batch size when maxBatchSize is 0
            Assert.Equal(1000, ResizeBatch(500, 1000, 1000, 0, null, null, new DateTime(2000, 1, 1, 12, 0, 0)));

            //test the basic case with threshold times not set
            Assert.Equal(1000, ResizeBatch(500, 1500, 1500, 500, null, null, new DateTime(2000, 1, 1, 12, 0, 0)));

            //same case with threshold times set (not wrapping around midnight), when we are not in the ignore window
            Assert.Equal(1000, ResizeBatch(500, 1500, 1500, 500, new TimeSpan(1, 0, 0), new TimeSpan(3, 0, 0), new DateTime(2000, 1, 1, 12, 0, 0)));

            //threshold times set (not wrapping around midnight) and we are currently in the ignore window
            Assert.Equal(1500, ResizeBatch(500, 1500, 1500, 500, new TimeSpan(1, 0, 0), new TimeSpan(3, 0, 0), new DateTime(2000, 1, 1, 2, 0, 0)));

            //threshold time wraps around midnight and we are not in the ignore window
            Assert.Equal(1000, ResizeBatch(500, 1500, 1500, 500, new TimeSpan(23, 45, 0), new TimeSpan(1, 30, 0), new DateTime(2000, 1, 1, 12, 0, 0)));

            //threshold time wraps around midnight and we are in the ignore window (before midnight)
            Assert.Equal(1500, ResizeBatch(500, 1500, 1500, 500, new TimeSpan(23, 45, 0), new TimeSpan(1, 30, 0), new DateTime(2000, 1, 1, 23, 55, 0)));

            //threshold time wraps around midnight and we are in the ignore window (after midnight)
            Assert.Equal(1500, ResizeBatch(500, 1500, 1500, 500, new TimeSpan(23, 45, 0), new TimeSpan(1, 30, 0), new DateTime(2000, 1, 1, 0, 30, 0)));
        }
        #endregion
    }
}
