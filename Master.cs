#region Using Statements
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using Xunit;
using Microsoft.SqlServer.Management.Smo;
using Microsoft.SqlServer.Management.Common;
using System.Data.Sql;
using System.Data.SqlClient;
using System.Diagnostics;
#endregion

namespace TeslaSQL {
    public class Master : Agent {

        private Int64 syncStartVersion;
        private Int64 syncStopVersion;
        private Int64 CTID;
        private Int32 syncBitWise;
        private Int64 currentVersion;
        private DateTime previousSyncStartTime;
        private Dictionary<string, Int64> ChangesCaptured = new Dictionary<string, Int64>();

        public override void ValidateConfig() {
            Config.ValidateRequiredHost(Config.relayServer);
            Config.ValidateRequiredHost(Config.master);
            if (Config.relayType == null || Config.masterType == null) {
                throw new Exception("Master agent requires a valid SQL flavor for relay and master");
            }
        }

        public override int Run() {
            int retval = 0;
            Logger.Log("Initializing CT batch", LogLevel.Trace);
            //set up the variables and CT version info for this run
            bool doCaptureChanges = InitializeBatch();

            if (doCaptureChanges) {
                //get the start time of the last batch where we successfully uploaded changes
                previousSyncStartTime = DataUtils.GetLastStartTime(TServer.RELAY, Config.relayDB, CTID, Convert.ToInt32(SyncBitWise.UploadChanges));

                //set the field list values on the table config objects
                SetFieldLists(TServer.MASTER, Config.masterDB, Config.tables);

                //resize batch based on batch threshold
                syncStopVersion = ResizeBatch(syncStartVersion, syncStopVersion, currentVersion, Config.maxBatchSize,
                    Config.thresholdIgnoreStartTime, Config.thresholdIgnoreEndTime, DateTime.Now);

                //create tblCTSchemaChange_<CTID> on the relay server
                DataUtils.CreateSchemaChangeTable(TServer.RELAY, Config.relayDB, CTID);

                //populate schema change table with any DDL events that have been captured since the previous successful batch started
                DataUtils.CopyDDLEvents(TServer.MASTER, Config.masterDB, TServer.RELAY, Config.relayDB, previousSyncStartTime, CTID);

                //loop through all tables, create CT table for each one
                CreateChangeTables(Config.tables, TServer.MASTER, Config.masterDB, Config.masterCTDB, syncStartVersion, syncStopVersion, CTID);

                //update bitwise on tblCTVersion, indicating that we have completed the change table creation step
                DataUtils.WriteBitWise(TServer.RELAY, Config.relayDB, CTID, Convert.ToInt32(SyncBitWise.CaptureChanges), AgentType.Master);
            } else {
                //since CreateChangeTables ran on the previous run we need to manually populate the ChangesCaptured object
                SetRowCounts(Config.tables, TServer.MASTER, Config.masterCTDB, CTID);
            }

            //copy change tables from master to relay server
            PublishChangeTables(Config.tables, TServer.MASTER, Config.masterCTDB, TServer.RELAY, Config.relayDB, CTID);

            //this signifies the end of the master's responsibility for this batch
            DataUtils.WriteBitWise(TServer.RELAY, Config.relayDB, CTID, Convert.ToInt32(SyncBitWise.UploadChanges), AgentType.Master);


            return retval;
        }


        /// <summary>
        /// Initializes version/batch info for a run and creates CTID
        /// </summary>
        /// <returns>boolean, which lets the agent know whether or not it should continue creating changetables</returns>
        private bool InitializeBatch() {
            currentVersion = DataUtils.GetCurrentCTVersion(TServer.MASTER, Config.masterDB);

            DataUtils.GetLastCTVersion(TServer.RELAY, Config.relayDB, AgentType.Master, out syncStartVersion, out syncStopVersion, out CTID, out syncBitWise);

            if ((syncBitWise & Convert.ToInt32(SyncBitWise.UploadChanges)) > 0) {
                //last batch succeeded, so we'll start the new batch where that one left off
                syncStartVersion = syncStopVersion;
                syncStopVersion = currentVersion;
                syncBitWise = 0;

                CTID = DataUtils.CreateCTVersion(TServer.RELAY, Config.relayDB, syncStartVersion, syncStopVersion);
                return true;
            } else if ((syncBitWise & Convert.ToInt32(SyncBitWise.CaptureChanges)) > 0) {
                //the first step is already complete for this batch, so skip it
                //log something here?
                Logger.Log("Creating CT tables has already been done for this batch, skipping", LogLevel.Warn);
                return false;
            }
            //TODO decide what to do about this case?
            //if (syncStartVersion == syncStopVersion)

            //to avoid continuously falling further behind when failing we update syncStopVersion on the next run
            syncStopVersion = currentVersion;

            //persist the new syncStopVersion to the database
            DataUtils.UpdateSyncStopVersion(TServer.RELAY, Config.relayDB, syncStopVersion, CTID);

            //if we get here, last batch failed so we are now about to retry
            return true;
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
        private void CreateChangeTables(TableConf[] t_array, TServer sourceServer, string sourceDB, string sourceCTDB, Int64 startVersion, Int64 stopVersion, Int64 ct_id) {
            foreach (TableConf t in t_array) {
                if (!DataUtils.CheckTableExists(sourceServer, sourceDB, t.Name)) {
                    //table doesn't exist, throw error if it's a stopOnError table, log/e-mail if it isn't
                    if (t.stopOnError) {
                        throw new Exception("Generating changetables for " + t.Name + " failed because it does not exist");
                    } else {
                        //TODO log the exception somewhere and/or send an e-mail to DBA@?
                        Logger.Log("Table " + t.Name + " does not exist, skipping", LogLevel.Error);
                        continue;
                    }
                }
                if (!DataUtils.HasPrimaryKey(sourceServer, sourceDB, t.Name)) {
                    throw new Exception("Unable to capture changes for " + t.Name + " because it has no primary key!");
                    //add stoponerror logic
                }
                CreateChangeTable(t, sourceServer, sourceDB, sourceCTDB, startVersion, stopVersion, ct_id);
            }
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
        private void CreateChangeTable(TableConf t, TServer sourceServer, string sourceDB, string sourceCTDB, Int64 startVersion, Int64 stopVersion, Int64 ct_id) {
            //TODO check tblCTInitialize and change startVersion if necessary? need to decide if we are keeping tblCTInitialize at all
            //alternative to keeping it is to have it live only on a slave which then keeps track of which batches it has applied for that table separately from the CT runs
            string ctTableName = "tblCT" + t.Name + "_" + Convert.ToString(ct_id);

            //validate that startVersion >= CHANGE_TRACKING_MIN_VALID_VERSION(OBJECT_ID(@tablename)) for that table
            Int64 minValid = DataUtils.GetMinValidVersion(sourceServer, sourceDB, t.Name);
            if (startVersion < minValid) {
                throw new Exception("Start version is less than CHANGE_TRACKING_MIN_VALID_VERSION() for " + t.Name + ", we aren't able to get accurate changes.");
            }

            //drop the table if it exists
            bool tExisted = DataUtils.DropTableIfExists(sourceServer, sourceCTDB, ctTableName);

            int rowsAffected = DataUtils.SelectIntoCTTable(sourceServer, sourceCTDB, t.masterColumnList,
                ctTableName, sourceDB, t.Name, startVersion, t.pkList, stopVersion, t.notNullPKList, 1200);

            Logger.Log("Rows affected for table " + t.Name + ": " + Convert.ToString(rowsAffected), LogLevel.Debug);

            //TODO handle this case - either drop the CT table and then later copy all CT tables, OR set a config variable
            //if (rowsAffected == 0)
            //current solution:
            ChangesCaptured.Add(t.Name, rowsAffected);
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
        private void PublishChangeTables(TableConf[] t_array, TServer sourceServer, string sourceCTDB, TServer destServer, string destCTDB, Int64 ct_id) {
            foreach (TableConf t in t_array) {
                //don't copy tables that had no changes
                if (ChangesCaptured[t.Name] > 0) {
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
        /// Sets ChangesCaptured object based on row counts in CT tables
        /// </summary>
        /// <param name="t_array">Array of table config objects</param>
        /// <param name="sourceServer">Server identifier for the source</param>
        /// <param name="sourceCTDB">CT database name</param>
        /// <param name="ct_id">CT batch id</param>
        private void SetRowCounts(TableConf[] t_array, TServer sourceServer, string sourceCTDB, Int64 ct_id) {
            Table t_smo;
            foreach (TableConf t in t_array) {
                try {
                    t_smo = DataUtils.GetSmoTable(sourceServer, sourceCTDB, t.Name);
                    ChangesCaptured.Add(t.Name, t_smo.RowCount);
                } catch (DoesNotExistException) {
                    ChangesCaptured.Add(t.Name, 0);
                }
            }
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
