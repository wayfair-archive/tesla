using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Xml;

namespace TeslaSQL.Agents {
    //TODO throughout this class add error handling for tables that shouldn't stop on error
    //TODO we need to set up field lists somewhere in here...
    //TODO figure out where to put check for MSSQL vs. netezza and where to branch the code paths
    class Slave : Agent
    {
        //base keyword invokes the base class's constructor
        public Slave(Config config, IDataUtils dataUtils) : base(config, dataUtils) {

        }

        public override void ValidateConfig()
        {
            Config.ValidateRequiredHost(config.relayServer);
            Config.ValidateRequiredHost(config.slave);
            if (config.relayType == null || config.slaveType == null) {
                throw new Exception("Slave agent requires a valid SQL flavor for relay and slave");
            }
        }

        public override void Run() {
            logger.Log("Initializing CT batch", LogLevel.Trace);
            //set up the variables and CT version info for this run
            List<ChangeTrackingBatch> batches = InitializeBatch();

            if (batches.Count == 0) {
                logger.Log("No pending batches to work on, exiting with success", LogLevel.Info);
                return;
            } else if (batches.Count == 1) {
                logger.Log("Beginning work - single batch", LogLevel.Info);
                RunSingleBatch(batches[0]);
            } else {
                logger.Log("Beginning work - applying multiple batches", LogLevel.Info);
                RunMultiBatch(batches);
            }
            logger.Log("Slave agent work complete", LogLevel.Info);
            return;
        }


        /// <summary>
        /// Initializes version/batch info for a run
        /// </summary>
        /// <returns>List of change tracking batches to work on</returns>
        private List<ChangeTrackingBatch> InitializeBatch() {
            var batches = new List<ChangeTrackingBatch>();
            Int64 CTID;
            Int64 syncStartVersion;
            Int64 syncStopVersion;
            Int32 syncBitWise;
            DateTime syncStartTime;
            ChangeTrackingBatch ctb;

            //get the last CT version this slave worked on in tblCTSlaveVersion
            logger.Log("Retrieving information on last run for slave " + config.slave, LogLevel.Debug);
            DataRow lastBatch = dataUtils.GetLastCTBatch(TServer.RELAY, config.relayDB, AgentType.Slave, config.slave);

            //compare bitwise to the bit for last step of slave agent
            if ((lastBatch.Field<Int32>("syncBitWise") & Convert.ToInt32(SyncBitWise.SyncHistoryTables)) > 0) {

                logger.Log("Last batch was successful, checking for new batches.", LogLevel.Debug);
                //get all pending revisions that this slave hasn't done yet
                DataTable pendingVersions = dataUtils.GetPendingCTVersions(TServer.RELAY, config.relayDB, lastBatch.Field<Int64>("CTID"), Convert.ToInt32(SyncBitWise.UploadChanges));
                logger.Log("Retrieved " + Convert.ToString(pendingVersions.Rows.Count) + " pending CT version(s) to work on.", LogLevel.Debug);

                if (pendingVersions.Rows.Count == 0) {
                    //master hasn't published a new batch so we are done for this run
                    logger.Log("No work to do, exiting with success.", LogLevel.Debug);
                    return batches;
                }

                if (config.batchConsolidationThreshold == 0 || pendingVersions.Rows.Count < config.batchConsolidationThreshold) {
                    logger.Log("Pending versions within threshold of " + Convert.ToString(config.batchConsolidationThreshold) + ", doing next batch.", LogLevel.Debug);

                    //we are an acceptable number of versions behind, so work on the next version
                    CTID = pendingVersions.Rows[0].Field<Int64>("CTID");
                    syncStartVersion = pendingVersions.Rows[0].Field<Int64>("syncStartVersion");
                    syncStopVersion = pendingVersions.Rows[0].Field<Int64>("syncStopVersion");
                    syncBitWise = pendingVersions.Rows[0].Field<Int32>("syncBitWise");
                    syncStartTime = pendingVersions.Rows[0].Field<DateTime>("syncStartTime");

                    logger.Log("Creating entry for CTID " + Convert.ToString(CTID) + " in tblCTSlaveVersion", LogLevel.Debug);
                    dataUtils.CreateSlaveCTVersion(TServer.RELAY, config.relayDB, CTID, config.slave, syncStartVersion, syncStopVersion, syncStartTime, syncBitWise);
                    ctb = new ChangeTrackingBatch(CTID, syncStartVersion, syncStopVersion, syncBitWise);
                    batches.Add(ctb);
                    return batches;
                } else {
                    //we are too far behind, need to consolidate batches to catch up
                    logger.Log("We are more than threshold of " + Convert.ToString(config.batchConsolidationThreshold) + " batches behind, consolidating pending batches.", LogLevel.Debug);
                    foreach (DataRow row in pendingVersions.Rows) {
                        CTID = row.Field<Int64>("CTID");
                        syncStartVersion = row.Field<Int64>("syncStartVersion");
                        syncStopVersion = row.Field<Int64>("syncStopVersion");
                        syncBitWise = row.Field<Int32>("syncBitWise");
                        syncStartTime = row.Field<DateTime>("syncStartTime");
                        ctb = new ChangeTrackingBatch(CTID, syncStartVersion, syncStopVersion, syncBitWise);
                        batches.Add(ctb);
                    }
                    return batches;
                }
            }
            //if we get here, last batch failed so we are now about to retry
            CTID = lastBatch.Field<Int64>("CTID");
            syncStartVersion = lastBatch.Field<Int64>("syncStartVersion");
            syncStopVersion = lastBatch.Field<Int64>("syncStopVersion");
            syncBitWise = lastBatch.Field<Int32>("syncBitWise");
            syncStartTime = lastBatch.Field<DateTime>("syncStartTime");

            logger.Log("Last batch failed, retrying CTID " + Convert.ToString(CTID), LogLevel.Warn);
            ctb = new ChangeTrackingBatch(CTID, syncStartVersion, syncStopVersion, syncBitWise);
            batches.Add(ctb);
            return batches;
        }


        /// <summary>
        /// Consolidates multiple batches and runs them as a group
        /// </summary>
        /// <param name="ctidTable">DataTable object listing all the batches</param>
        private void RunMultiBatch(List<ChangeTrackingBatch> batches) {
            //TODO add logger statements

            //this will hold a list of all the CT tables that exist
            List<string> tables = new List<string>();

            //loop through each batch and copy the ct tables and apply schema changes
            foreach (ChangeTrackingBatch batch in batches) {
                logger.Log("Multi batch run - beginning work on CTID: " + Convert.ToString(batch.CTID), LogLevel.Debug);
                if ((batch.syncBitWise & Convert.ToInt32(SyncBitWise.DownloadChanges)) == 0) {
                    logger.Log("Downloading change tables for CTID: " + Convert.ToString(batch.CTID), LogLevel.Debug);
                    //copy the change tables for each batch if it hasn't been done yet
                    tables.Concat(CopyChangeTables(config.tables, TServer.RELAY, config.relayDB, TServer.SLAVE, config.slaveCTDB, batch.CTID));
                    logger.Log("Changes downloaded successfully for CTID: " + Convert.ToString(batch.CTID), LogLevel.Debug);
                    //persist bitwise progress to database
                    dataUtils.WriteBitWise(TServer.RELAY, config.relayDB, batch.CTID, Convert.ToInt32(SyncBitWise.DownloadChanges), AgentType.Slave);
                } else {
                    logger.Log("Not downloading changes since it was already done, instead populating table list for CTID: " + Convert.ToString(batch.CTID), LogLevel.Debug);
                    //we've already downloaded changes in a previous run so fill in the List of tables using the slave server
                    tables.Concat(PopulateTableList(config.tables, TServer.SLAVE, config.slaveCTDB, batch.CTID));
                }

                if ((batch.syncBitWise & Convert.ToInt32(SyncBitWise.ApplySchemaChanges)) == 0) {
                    //copy the change tables for each batch if it hasn't been done yet
                    //TODO implement
                    //ApplySchemaChanges(config.tables, TServer.RELAY, config.relayDB, TServer.SLAVE, config.slaveCTDB, batch.CTID);

                    //persist bitwise progress to database
                    dataUtils.WriteBitWise(TServer.RELAY, config.relayDB, batch.CTID, Convert.ToInt32(SyncBitWise.ApplySchemaChanges), AgentType.Slave);
                }
            }

            //from here forward all operations will use the bitwise value for the last CTID since they are operating on this whole set of batches
            ChangeTrackingBatch endBatch = batches.OrderBy(item => item.CTID).Last();

            //consolidate the change sets into one changetable per table
            if ((endBatch.syncBitWise & Convert.ToInt32(SyncBitWise.ConsolidateBatches)) == 0) {
                //TODO implement
                //ConsolidateBatches(config.tables, TServer.RELAY, config.relayDB, TServer.SLAVE, config.slaveCTDB, kvp.Key, tables);

                //persist bitwise progress to database
                dataUtils.WriteBitWise(TServer.RELAY, config.relayDB, endBatch.CTID, Convert.ToInt32(SyncBitWise.ConsolidateBatches), AgentType.Slave);
            }

            //apply the changes to the destination tables
            if ((endBatch.syncBitWise & Convert.ToInt32(SyncBitWise.ApplyChanges)) == 0) {
                //TODO implement
                //ApplyBatchedChanges(config.tables, TServer.SLAVE, config.slaveCTDB, config.slaveDB, tables);
                //persist bitwise progress to database
                dataUtils.WriteBitWise(TServer.RELAY, config.relayDB, endBatch.CTID, Convert.ToInt32(SyncBitWise.ApplyChanges), AgentType.Slave);
            }

            //final step, synchronize history tables
            //TODO implement
            //SyncBatchedHistoryTables(config.tables, TServer.SLAVE, config.slaveCTDB, config.slaveDB, tables);
            //success! go through and mark all the batches as complete in the db
            foreach (ChangeTrackingBatch batch in batches) {
                dataUtils.MarkBatchComplete(TServer.RELAY, config.relayDB, batch.CTID, Convert.ToInt32(SyncBitWise.SyncHistoryTables), DateTime.Now, AgentType.Slave, config.slave);
            }
        }


        /// <summary>
        /// Runs a single change tracking batch
        /// </summary>
        /// <param name="CTID">Change tracking batch object to work on</param>
        private void RunSingleBatch(ChangeTrackingBatch ctb) {
            //TODO finish
            //TODO add logger statements
            //this will hold a list of all the CT tables that exist
            List<string> tables = new List<string>();

            //copy change tables to slave if not already done
            if ((ctb.syncBitWise & Convert.ToInt32(SyncBitWise.DownloadChanges)) == 0) {
                tables = CopyChangeTables(config.tables, TServer.RELAY, config.relayDB, TServer.SLAVE, config.slaveCTDB, ctb.CTID);
                dataUtils.WriteBitWise(TServer.RELAY, config.relayDB, ctb.CTID, Convert.ToInt32(SyncBitWise.DownloadChanges), AgentType.Slave);
            } else {
                //since CopyChangeTables doesn't need to be called to fill in CT table list, get it from the slave instead
                tables = PopulateTableList(config.tables, TServer.SLAVE, config.slaveCTDB, ctb.CTID);
            }

            //apply schema changes if not already done
            if ((ctb.syncBitWise & Convert.ToInt32(SyncBitWise.ApplySchemaChanges)) == 0) {
                //TODO implement
                //ApplySchemaChanges(config.tables, TServer.RELAY, config.relayDB, TServer.SLAVE, config.slaveCTDB, kvp.Key);
                dataUtils.WriteBitWise(TServer.RELAY, config.relayDB, ctb.CTID, Convert.ToInt32(SyncBitWise.ApplySchemaChanges), AgentType.Slave);
            }

            //apply changes to destination tables if not already done
            if ((ctb.syncBitWise & Convert.ToInt32(SyncBitWise.ApplyChanges)) == 0) {
                //TODO implement
                //ApplyChanges(config.tables, TServer.SLAVE, config.slaveCTDB, config.slaveDB, tables);
                dataUtils.WriteBitWise(TServer.RELAY, config.relayDB, ctb.CTID, Convert.ToInt32(SyncBitWise.ApplyChanges), AgentType.Slave);
            }

            //update the history tables
            //TODO implement
            //SyncBatchedHistoryTables(config.tables, TServer.SLAVE, config.slaveCTDB, config.slaveDB, tables);


            //success! mark the batch as complete
            dataUtils.MarkBatchComplete(TServer.RELAY, config.relayDB, ctb.CTID, Convert.ToInt32(SyncBitWise.SyncHistoryTables), DateTime.Now, AgentType.Slave, config.slave);
        }


        /// <summary>
        /// For the specified list of batches and tables, populate a list of each CT tables exist
        /// </summary>
        /// <param name="tables">Array of table config objects</param>
        /// <param name="server">Server identifier</param>
        /// <param name="dbName">Database name</param>
        /// <param name="batches">Dictionary of batches, where the key is a CTID</param>
        /// <param name="tables">List of table names to populate</param>
        private List<string> PopulateTableList(TableConf[] tables, TServer server, string dbName, Int64 CTID) {
            //TODO add logger statements
            var tableList = new List<string>();
            string ctTableName;
            foreach (TableConf t in tables) {
                ctTableName = CTTableName(t.Name, CTID);
                if (dataUtils.CheckTableExists(server, dbName, ctTableName, t.schemaName)) {
                    tableList.Add(t.schemaName + "." + ctTableName);
                }
            }
            return tableList;
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
        /// <param name="tables">Reference variable, list of tables that have >0 changes. Passed by ref instead of output
        ///     because in multi batch mode it is built up over several calls to this method.</param>
        private List<string> CopyChangeTables(TableConf[] tables, TServer sourceServer, string sourceCTDB, TServer destServer, string destCTDB, Int64 CTID) {
            //TODO change from ref variable to returning a list
            //TODO add logger statements
            bool found = false;            
            List<string> tableList = new List<string>();
            foreach (TableConf t in tables) {
                found = false;
                string ctTable = CTTableName(t.Name, CTID);
                //attempt to copy the change table locally
                try {
                    //hard coding timeout at 1 hour for bulk copy
                    dataUtils.CopyTable(sourceServer, sourceCTDB, ctTable, t.schemaName, destServer, destCTDB, 36000);
                    found = true;
                } catch (DoesNotExistException) {
                    //this is a totally normal and expected case since we only publish changetables when data actually changed
                    logger.Log("No changes to pull for table " + t.schemaName + "." + ctTable + " because it does not exist ", LogLevel.Debug);
                } catch (Exception e) {
                    if (t.stopOnError) {
                        throw e;
                    } else {
                        logger.Log("Copying change data for table " + t.schemaName + "." + ctTable + " failed with error: " + e.Message, LogLevel.Error);
                    }
                }
                if (found) {
                    tableList.Add(t.schemaName + "." + ctTable);
                }
            }
            return tableList;
        }


        private void ApplySchemaChanges(TableConf[] tables, TServer sourceServer, string sourceDB, TServer destServer, string destDB, Int64 CTID) {
            //get list of schema changes from tblCTSChemaChange_ctid on the relay server/db
            DataTable result = dataUtils.GetSchemaChanges(TServer.RELAY, config.relayDB, CTID);

            if (result == null) {
                return;
            }

            TableConf t;
            foreach (DataRow row in result.Rows) {
                var schemaChange = new SchemaChange(row);
                //String.Compare method returns 0 if the strings are equal, the third "true" flag is for a case insensitive comparison
                t = tables.SingleOrDefault(item => String.Compare(item.Name, schemaChange.tableName, ignoreCase: true) == 0);

                if (t == null) {
                    //table isn't in our config so we don't care about this schema change
                    logger.Log("Ignoring schema change for table " + row.Field<string>("CscTableName") + " because it isn't in config", LogLevel.Debug);
                    continue;
                }
                logger.Log("Processing schema change (CscID: " + Convert.ToString(row.Field<int>("CscID")) + 
                    " of type " + schemaChange.eventType + " for table " + t.Name, LogLevel.Info);

                if (t.columnList == null || t.columnList.Contains(schemaChange.columnName, StringComparer.OrdinalIgnoreCase)) {
                    logger.Log("Schema change applies to a valid column, so we will apply it", LogLevel.Info);
                    switch (schemaChange.eventType) {
                        case SchemaChangeType.Rename:
                            logger.Log("Renaming column " + schemaChange.columnName + " to " + schemaChange.newColumnName, LogLevel.Info);
                            dataUtils.RenameColumn(t, destServer, destDB, schemaChange.schemaName, schemaChange.tableName,
                                schemaChange.columnName, schemaChange.newColumnName);
                            break;
                        case SchemaChangeType.Modify:
                            logger.Log("Changing data type on column " + schemaChange.columnName, LogLevel.Info);
                            dataUtils.ModifyColumn(t, destServer, destDB, schemaChange.schemaName, schemaChange.tableName, schemaChange.columnName,
                                 schemaChange.dataType.baseType,schemaChange.dataType.characterMaximumLength,
                                 schemaChange.dataType.numericPrecision,schemaChange.dataType.numericScale);                                                 
                            break;
                        case SchemaChangeType.Add:
                            logger.Log("Adding column " + schemaChange.columnName, LogLevel.Info);
                            dataUtils.AddColumn(t, destServer, destDB, schemaChange.schemaName, schemaChange.tableName, schemaChange.columnName,
                                 schemaChange.dataType.baseType, schemaChange.dataType.characterMaximumLength,
                                 schemaChange.dataType.numericPrecision, schemaChange.dataType.numericScale);   
                            break;
                        case SchemaChangeType.Drop:
                            logger.Log("Dropping column " + schemaChange.columnName, LogLevel.Info);
                            dataUtils.DropColumn(t, destServer, destDB, schemaChange.schemaName, schemaChange.tableName, schemaChange.columnName);  
                            break;
                    }

                } else {
                    logger.Log("Skipped schema change because the column it impacts is not in our list", LogLevel.Info);
                }

            }
        }
    }
}
