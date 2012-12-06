#region Using Statements
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Xml;
using Xunit;
using System.Diagnostics;
#endregion

namespace TeslaSQL {
    public class Master : Agent {
        public override void ValidateConfig() {
            Config.ValidateRequiredHost(Config.relayServer);
            Config.ValidateRequiredHost(Config.master);
            if (Config.relayType == null || Config.masterType == null) {
                throw new Exception("Master agent requires a valid SQL flavor for relay and master");
            }
        }

        public override int Run() {
            int retval = 0;

            Logger.Log("Getting CHANGE_TRACKING_CURRENT_VERSION from master", LogLevel.Trace);
            Int64 currentVersion = DataUtils.GetCurrentCTVersion(TServer.MASTER, Config.masterDB);

            Logger.Log("Initializing CT batch", LogLevel.Trace);

            //set up the variables and CT version info for this run
            ChangeTrackingBatch ctb = InitializeBatch(currentVersion);

            DateTime previousSyncStartTime;
            Dictionary<string, Int64> changesCaptured;

            if ((ctb.syncBitWise & Convert.ToInt32(SyncBitWise.PublishSchemaChanges)) == 0) {
                //create tblCTSchemaChange_<CTID> on the relay server
                DataUtils.CreateSchemaChangeTable(TServer.RELAY, Config.relayDB, ctb.CTID);

                //get the start time of the last batch where we successfully uploaded changes
                previousSyncStartTime = DataUtils.GetLastStartTime(TServer.RELAY, Config.relayDB, ctb.CTID, Convert.ToInt32(SyncBitWise.UploadChanges));

                //GetDDLEvents
                //TODO call PublishSchemaChanges and other necessary methods
                PublishSchemaChanges(Config.tables, TServer.MASTER, Config.masterDB, TServer.RELAY, Config.relayDB, ctb.CTID, previousSyncStartTime);

                DataUtils.WriteBitWise(TServer.RELAY, Config.relayDB, ctb.CTID, Convert.ToInt32(SyncBitWise.PublishSchemaChanges), AgentType.Master);
            }

            if ((ctb.syncBitWise & Convert.ToInt32(SyncBitWise.CaptureChanges)) == 0) {
                //set the field list values on the table config objects
                SetFieldLists(TServer.MASTER, Config.masterDB, Config.tables);

                //resize batch based on batch threshold
                Int64 resizedStopVersion = ResizeBatch(ctb.syncStartVersion, ctb.syncStopVersion, currentVersion, Config.maxBatchSize,
                    Config.thresholdIgnoreStartTime, Config.thresholdIgnoreEndTime, DateTime.Now);

                //if it changed, persist that change to the database
                if (resizedStopVersion != ctb.syncStopVersion) {
                    ctb.syncStopVersion = resizedStopVersion;
                    DataUtils.UpdateSyncStopVersion(TServer.RELAY, Config.relayDB, resizedStopVersion, ctb.CTID);
                }

                //loop through all tables, create CT table for each one
                changesCaptured = CreateChangeTables(Config.tables, TServer.MASTER, Config.masterDB, Config.masterCTDB, ctb.syncStartVersion, ctb.syncStopVersion, ctb.CTID);

                //update bitwise on tblCTVersion, indicating that we have completed the change table creation step
                DataUtils.WriteBitWise(TServer.RELAY, Config.relayDB, ctb.CTID, Convert.ToInt32(SyncBitWise.CaptureChanges), AgentType.Master);
            } else {
                //since CreateChangeTables ran on the previous run we need to manually populate the ChangesCaptured object
                changesCaptured = GetRowCounts(Config.tables, TServer.MASTER, Config.masterCTDB, ctb.CTID);
            }

            //copy change tables from master to relay server
            PublishChangeTables(Config.tables, TServer.MASTER, Config.masterCTDB, TServer.RELAY, Config.relayDB, ctb.CTID, changesCaptured);

            //this signifies the end of the master's responsibility for this batch
            DataUtils.WriteBitWise(TServer.RELAY, Config.relayDB, ctb.CTID, Convert.ToInt32(SyncBitWise.UploadChanges), AgentType.Master);

            return retval;
        }


        /// <summary>
        /// Initializes version/batch info for a run and creates CTID
        /// </summary>
        /// <param name="currentVersion">current change tracking version on the master</param>
        /// <returns>boolean, which lets the agent know whether or not it should continue creating changetables</returns>
        private ChangeTrackingBatch InitializeBatch(Int64 currentVersion) {

            DataRow lastbatch = DataUtils.GetLastCTBatch(TServer.RELAY, Config.relayDB, AgentType.Master);

            if (lastbatch == null) {
                //TODO figure out a better way to handle this case, determine an appropriate syncStartVersion. Perhaps use 0 and specially handle that using
                //CHANGE_TRACKING_MIN_VALID_VERSION?
                throw new Exception("Unable to determine appropriate syncStartVersion - version table seems to be empty.");
            }

            if ((lastbatch.Field<Int32>("syncBitWise") & Convert.ToInt32(SyncBitWise.UploadChanges)) > 0) {
                //last batch succeeded, so we'll start the new batch where that one left off
                Int64 syncStartVersion = lastbatch.Field<Int64>("syncStopVersion");

                Int64 CTID = DataUtils.CreateCTVersion(TServer.RELAY, Config.relayDB, syncStartVersion, currentVersion);
                return new ChangeTrackingBatch(CTID, syncStartVersion, currentVersion, 0);
            } else if ((lastbatch.Field<Int32>("syncBitWise") & Convert.ToInt32(SyncBitWise.CaptureChanges)) == 0) {
                //last batch failed before creating CT tables. we need to update syncStopVersion to avoid falling behind too far
                DataUtils.UpdateSyncStopVersion(TServer.RELAY, Config.relayDB, currentVersion, lastbatch.Field<Int64>("CTID"));
                return new ChangeTrackingBatch(lastbatch.Field<Int64>("CTID"),
                    lastbatch.Field<Int64>("syncStartVersion"),
                    currentVersion,
                    lastbatch.Field<Int32>("syncBitWise"));
            } else {
                //previous batch failed after creating changetables. just return it so it can be retried.
                return new ChangeTrackingBatch(lastbatch.Field<Int64>("CTID"),
                    lastbatch.Field<Int64>("syncStartVersion"),
                    lastbatch.Field<Int64>("syncStopVersion"),
                    lastbatch.Field<Int32>("syncBitWise"));
            }
        }


        /// <summary>
        /// Publish schema changes that hav eoccurred since the last batch started
        /// </summary>
        /// <param name="t_array">Array of table config objects</param>
        /// <param name="sourceServer">Server identifier to pull from</param>
        /// <param name="sourceDB">Source database name</param>
        /// <param name="destServer">Server identifier to write to</param>
        /// <param name="destDB">Destination database name</param>
        /// <param name="CTID">Change tracking batch id</param>
        /// <param name="afterDate">Date to pull schema changes after</param>
        public void PublishSchemaChanges(TableConf[] t_array, TServer sourceServer, string sourceDB, TServer destServer, string destDB, Int64 CTID, DateTime afterDate) {
            //get all DDL events since afterDate
            DataTable ddlEvents = DataUtils.GetDDLEvents(sourceServer, sourceDB, afterDate);
            var schemaChanges = new List<SchemaChange>();
            DDLEvent dde;
            foreach (DataRow row in ddlEvents.Rows) {
                //create a DDL event object based on the row
                dde = new DDLEvent(row.Field<int>("DdeID"), row.Field<XmlDocument>("DdeEventData"));

                //a DDL event can yield 0 or more schema change events, hence the List<SchemaChange>
                schemaChanges = dde.Parse(t_array, sourceServer, sourceDB);

                //iterate through any schema changes for this event and write them to tblCTSchemaChange_CTID
                foreach (SchemaChange schemaChange in schemaChanges) {
                    DataUtils.WriteSchemaChange(destServer,
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
                //handle interval wrapping around midnight
                if (thresholdIgnoreStartTime > thresholdIgnoreEndTime) {
                    //if the time span for ignoring batch resizing wraps around midnight, we check whether the current time is 
                    //after the start OR before the end (since it can't be both if the interval contains midnight)
                    if (CurrentDate.TimeOfDay > thresholdIgnoreStartTime || CurrentDate.TimeOfDay <= thresholdIgnoreEndTime)
                        stopVersion = curVersion;
                    else
                        stopVersion = startVersion + maxBatchSize;
                } else {
                    //simpler case when the time span doesn't contain midnight
                    if (CurrentDate.TimeOfDay > thresholdIgnoreStartTime && CurrentDate.TimeOfDay <= thresholdIgnoreEndTime)
                        stopVersion = curVersion;
                    else
                        stopVersion = startVersion + maxBatchSize;
                }
            }
            return stopVersion;
        }


        /// <summary>
        /// Loops through passed in array of table objects, creates changedata tables for each one on the CT DB
        /// </summary>
        /// <param name="t_array">Array of table config objects to create CT tables for</param>
        /// <param name="sourceServer">Server to connect to</param>
        /// <param name="sourceDB">Database the source data lives in</param>
        /// <param name="sourceCTDB">Database the changetables should go to</param>
        /// <param name="startVersion">Change tracking version to start with</param>
        /// <param name="stopVersion">Change tracking version to stop at</param>
        /// <param name="ct_id">CT batch ID this is being run for</param>
        private Dictionary<string, Int64> CreateChangeTables(TableConf[] t_array, TServer sourceServer, string sourceDB, string sourceCTDB, Int64 startVersion, Int64 stopVersion, Int64 ct_id) {
            Dictionary<string, Int64> changesCaptured = new Dictionary<string, Int64>();
            KeyValuePair<string, Int64> result;
            foreach (TableConf t in t_array) {
                result = CreateChangeTable(t, sourceServer, sourceDB, sourceCTDB, startVersion, stopVersion, ct_id);
                changesCaptured.Add(result.Key, result.Value);
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
        private bool ValidateSourceTable(TServer server, string dbName, string table, Int64 startVersion, out string reason) {
            if (!DataUtils.CheckTableExists(server, dbName, table)) {
                reason = "Table " + table + " does not exist in the source database";
                return false;
            } else if (!DataUtils.HasPrimaryKey(server, dbName, table)) {
                reason = "Table " + table + " has no primary key in the source database";
                return false;
            } else if (!DataUtils.IsChangeTrackingEnabled(server, dbName, table)) {
                reason = "Change tracking is not enabled on " + table;
                return false;
            } else if (startVersion < DataUtils.GetMinValidVersion(server, dbName, table)) {
                reason = "Start version of " + Convert.ToString(startVersion) + " is less than CHANGE_TRACKING_MIN_VALID_VERSION for table " + table;
                return false;
            }
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
        /// <param name="ct_id">CT batch ID this is being run for</param>
        private KeyValuePair<string, Int64> CreateChangeTable(TableConf t, TServer sourceServer, string sourceDB, string sourceCTDB, Int64 startVersion, Int64 stopVersion, Int64 ct_id) {
            //TODO check tblCTInitialize and change startVersion if necessary? need to decide if we are keeping tblCTInitialize at all
            //alternative to keeping it is to have it live only on a slave which then keeps track of which batches it has applied for that table separately from the CT runs

            //TODO handle case where startVersion is 0, change it to CHANGE_TRACKING_MIN_VALID_VERSION?
            string ctTableName = "tblCT" + t.Name + "_" + Convert.ToString(ct_id);
            string reason;

            if (!ValidateSourceTable(sourceServer, sourceDB, t.Name, startVersion, out reason)) {
                string message = "Change table creation impossible because : " + reason;
                if (t.stopOnError) {
                    throw new Exception(message);
                } else {
                    Logger.Log(message, LogLevel.Error);
                    return new KeyValuePair<string, Int64>(t.Name, 0);
                }
            }

            //drop the table if it exists
            bool tExisted = DataUtils.DropTableIfExists(sourceServer, sourceCTDB, ctTableName);

            //create the changetable
            Int64 rowsAffected = DataUtils.SelectIntoCTTable(sourceServer, sourceCTDB, t.masterColumnList,
                ctTableName, sourceDB, t.Name, startVersion, t.pkList, stopVersion, t.notNullPKList, 1200);

            Logger.Log("Rows affected for table " + t.Name + ": " + Convert.ToString(rowsAffected), LogLevel.Debug);

            return new KeyValuePair<string, Int64>(t.Name, rowsAffected);
        }


        /// <summary>
        /// Copies change tables from the master to the relay server
        /// </summary>
        /// <param name="t_array">Array of table config objects</param>
        /// <param name="sourceServer">Source server identifer</param>
        /// <param name="sourceCTDB">Source CT database</param>
        /// <param name="destServer">Dest server identifier</param>
        /// <param name="destCTDB">Dest CT database</param>
        /// <param name="ct_id">CT batch ID this is for</param>
        private void PublishChangeTables(TableConf[] t_array, TServer sourceServer, string sourceCTDB, TServer destServer, string destCTDB, Int64 ct_id, Dictionary<string, Int64> changesCaptured) {
            foreach (TableConf t in t_array) {
                //don't copy tables that had no changes
                if (changesCaptured[t.Name] > 0) {
                    //hard coding timeout at 1 hour for bulk copy
                    try {
                        DataUtils.CopyTable(sourceServer, sourceCTDB, "tblCT" + t.Name + "_" + Convert.ToString(ct_id), destServer, destCTDB, 36000);
                    } catch (Exception e) {
                        if (t.stopOnError) {
                            throw e;
                        } else {
                            Logger.Log("Copying change data for table " + t.Name + " failed with error: " + e.Message, LogLevel.Error);
                        }
                    }
                }
            }
        }


        /// <summary>
        /// Gets ChangesCaptured object based on row counts in CT tables
        /// </summary>
        /// <param name="t_array">Array of table config objects</param>
        /// <param name="sourceServer">Server identifier for the source</param>
        /// <param name="sourceCTDB">CT database name</param>
        /// <param name="ct_id">CT batch id</param>
        private Dictionary<string, Int64> GetRowCounts(TableConf[] t_array, TServer sourceServer, string sourceCTDB, Int64 ct_id) {
            Dictionary<string, Int64> rowCounts = new Dictionary<string, Int64>();

            foreach (TableConf t in t_array) {
                try {
                    rowCounts.Add(t.Name, DataUtils.GetTableRowCount(sourceServer, sourceCTDB, "tblCT" + t.Name + "_" + Convert.ToString(ct_id)));
                } catch (DoesNotExistException) {
                    rowCounts.Add(t.Name, 0);
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
