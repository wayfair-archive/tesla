using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Xml;

namespace TeslaSQL {
    //TODO throughout this class add error handling for tables that shouldn't stop on error
    //TODO we need to set up field lists somewhere in here...
    class Slave : Agent
    {
        private Int64 syncStartVersion;
        private Int64 syncStopVersion;
        private Int64 CTID;
        private Int32 syncBitWise;
        private DateTime syncStartTime;
        private DataTable pendingVersions;

        public override void ValidateConfig()
        {
            Config.ValidateRequiredHost(Config.relayServer);
            Config.ValidateRequiredHost(Config.slave);
            if (Config.relayType == null || Config.slaveType == null) {
                throw new Exception("Slave agent requires a valid SQL flavor for relay and slave");
            }
        }

        public override int Run() {
            int retval = 0;
            Logger.Log("Initializing CT batch", LogLevel.Trace);
            //set up the variables and CT version info for this run
            bool doConsolidate;
            bool workToDo = InitializeBatch(out doConsolidate);

            //if this is false there's nothing to do so we'll end up just returning 
            if (workToDo) {
                if (doConsolidate) {
                    RunMultiBatch(pendingVersions);
                } else {
                    RunSingleBatch(CTID);
                }
            }            
            return retval;
        }


        /// <summary>
        /// Initializes version/batch info for a run 
        /// </summary>
        /// <returns>boolean, which lets the agent know whether or not there is work to do</returns>
        private bool InitializeBatch(out bool doConsolidate) {
            doConsolidate = false;
            //get the last CT version this slave worked on in tblCTSlaveVersion
            Logger.Log("Retrieving information on last run for slave " + Config.slave, LogLevel.Debug);
            DataUtils.GetLastCTVersion(TServer.RELAY, Config.relayDB, AgentType.Slave, out syncStartVersion, out syncStopVersion, out CTID, out syncBitWise, Config.slave);

            //compare bitwise to the bit for last step of slave agent
            if ((syncBitWise & Convert.ToInt32(SyncBitWise.SyncHistoryTables)) > 0) {

                Logger.Log("Last batch was successful, checking for new batches.", LogLevel.Debug);
                //get all pending revisions that this slave hasn't done yet
                pendingVersions = DataUtils.GetPendingCTVersions(TServer.RELAY, Config.relayDB, CTID, Convert.ToInt32(SyncBitWise.UploadChanges));
                Logger.Log("Retrieved " + Convert.ToString(pendingVersions.Rows.Count) + " pending CT version(s) to work on.", LogLevel.Debug);

                if (pendingVersions.Rows.Count == 0) {
                    //master hasn't published a new batch so we are done for this run
                    Logger.Log("No work to do, exiting with success.", LogLevel.Debug);
                    return false;
                }

                if (Config.batchConsolidationThreshold == 0 || pendingVersions.Rows.Count < Config.batchConsolidationThreshold) {
                    Logger.Log("Pending versions within threshold of " + Convert.ToString(Config.batchConsolidationThreshold) + ", doing next batch.", LogLevel.Debug);

                    //we are an acceptable number of versions behind, so work on the next version
                    CTID = (Int64)pendingVersions.Rows[0]["CTID"];
                    syncStartVersion = (Int64)pendingVersions.Rows[0]["syncStartVersion"];
                    syncStopVersion = (Int64)pendingVersions.Rows[0]["syncStopVersion"];
                    syncBitWise = (Int32)pendingVersions.Rows[0]["syncBitWise"];
                    syncStartTime = (DateTime)pendingVersions.Rows[0]["syncStartTime"];

                    Logger.Log("Creating entry for CTID " + Convert.ToString(CTID) + " in tblCTSlaveVersion", LogLevel.Debug);
                    DataUtils.CreateSlaveCTVersion(TServer.RELAY, Config.relayDB, CTID, Config.slave, syncStartVersion, syncStopVersion, syncStartTime, syncBitWise);                        
                    return true;
                } else {
                    //we are too far behind, need to consolidate batches to catch up
                    Logger.Log("We are more than threshold of " + Convert.ToString(Config.batchConsolidationThreshold) + " batches behind, consolidating pending batches.", LogLevel.Debug);                              
                    doConsolidate = true;
                    return true;
                }
            } 

            //if we get here, last batch failed so we are now about to retry
            Logger.Log("Last batch failed, retrying CTID " + Convert.ToString(CTID), LogLevel.Warn);
            return true;
        }


        /// <summary>
        /// Consolidates multiple batches and runs them as a group
        /// </summary>
        /// <param name="ctidTable">DataTable object listing all the batches</param>
        private void RunMultiBatch(DataTable ctidTable) {
            //TODO add logger statements
            //dictionary to maintain batch IDs and bitwise for each batch to work on
            Int64 maxCTID = 0;
            Dictionary<Int64, Int32> batches = new Dictionary<Int64, Int32>();
            foreach (DataRow row in ctidTable.Rows) {
                batches.Add((Int64)row["CTID"], (Int32)row["syncBitWise"]);
                if ((Int64)row["CTID"] > maxCTID)
                    maxCTID = (Int64)row["CTID"];
            }
            
            //this will hold a list of all the CT tables that exist
            List<string> tables = new List<string>();
            

            //loop through each batch and copy the ct tables and apply schema changes 
            foreach (KeyValuePair<Int64, Int32> kvp in batches) {
                if ((kvp.Value & Convert.ToInt32(SyncBitWise.DownloadChanges)) == 0) {
                    //copy the change tables for each batch if it hasn't been done yet
                    CopyChangeTables(Config.tables, TServer.RELAY, Config.relayDB, TServer.SLAVE, Config.slaveCTDB, kvp.Key, ref tables);
                    //persist bitwise progress to database and in-memory dictionary
                    DataUtils.WriteBitWise(TServer.RELAY, Config.relayDB, kvp.Key, Convert.ToInt32(SyncBitWise.DownloadChanges), AgentType.Slave);
                    batches[kvp.Key] = kvp.Value + Convert.ToInt32(SyncBitWise.DownloadChanges);
                } else {
                    //we've already downloaded changes in a previous run so fill in the List of tables using the slave server
                    PopulateTableList(Config.tables, TServer.SLAVE, Config.slaveCTDB, ref tables, null, batches);
                }

                if ((kvp.Value & Convert.ToInt32(SyncBitWise.ApplySchemaChanges)) == 0) {
                    //copy the change tables for each batch if it hasn't been done yet
                    //TODO implement
                    //ApplySchemaChanges(Config.tables, TServer.RELAY, Config.relayDB, TServer.SLAVE, Config.slaveCTDB, kvp.Key);

                    //persist bitwise progress to database and in-memory dictionary
                    DataUtils.WriteBitWise(TServer.RELAY, Config.relayDB, kvp.Key, Convert.ToInt32(SyncBitWise.ApplySchemaChanges), AgentType.Slave);
                    batches[kvp.Key] = kvp.Value + Convert.ToInt32(SyncBitWise.ApplySchemaChanges);
                }                
            }

            //from here forward all operations will use the bitwise value for the last CTID since they are operating on this whole set of batches
           
            //consolidate the change sets into one changetable per table
            if ((batches[maxCTID] & Convert.ToInt32(SyncBitWise.ConsolidateBatches)) == 0) {
                //TODO implement
                //ConsolidateBatches(Config.tables, TServer.RELAY, Config.relayDB, TServer.SLAVE, Config.slaveCTDB, kvp.Key, tables);

                //persist bitwise progress to database and in-memory dictionary
                DataUtils.WriteBitWise(TServer.RELAY, Config.relayDB, maxCTID, Convert.ToInt32(SyncBitWise.ConsolidateBatches), AgentType.Slave);
                batches[maxCTID] += Convert.ToInt32(SyncBitWise.ConsolidateBatches);
            }

            //apply the changes to the destination tables
            if ((batches[maxCTID] & Convert.ToInt32(SyncBitWise.ApplyChanges)) == 0) {
                //TODO implement
                //ApplyBatchedChanges(Config.tables, TServer.SLAVE, Config.slaveCTDB, Config.slaveDB, tables);
                //persist bitwise progress to database and in-memory dictionary
                DataUtils.WriteBitWise(TServer.RELAY, Config.relayDB, maxCTID, Convert.ToInt32(SyncBitWise.ApplyChanges), AgentType.Slave);
                batches[maxCTID] += Convert.ToInt32(SyncBitWise.ApplyChanges);
            }
           
            //final step, synchronize history tables  
            //TODO implement
            //SyncBatchedHistoryTables(Config.tables, TServer.SLAVE, Config.slaveCTDB, Config.slaveDB, tables);
            //success! go through and mark all the batches as complete in the db
            foreach (KeyValuePair<Int64, Int32> kvp in batches) {
                DataUtils.MarkBatchComplete(TServer.RELAY, Config.relayDB, kvp.Key, Convert.ToInt32(SyncBitWise.SyncHistoryTables), DateTime.Now, AgentType.Slave, Config.slave);
            }
        }


        /// <summary>
        /// Runs a single change tracking batch
        /// </summary>
        /// <param name="ct_id">The CT batch ID to run</param>
        private void RunSingleBatch(Int64 ct_id) {
            //TODO finish            
            //TODO add logger statements
            //this will hold a list of all the CT tables that exist
            List<string> tables = new List<string>();

            //copy change tables to slave if not already done
            if ((syncBitWise & Convert.ToInt32(SyncBitWise.DownloadChanges)) == 0) {                
                CopyChangeTables(Config.tables, TServer.RELAY, Config.relayDB, TServer.SLAVE, Config.slaveCTDB, ct_id, ref tables);
                DataUtils.WriteBitWise(TServer.RELAY, Config.relayDB, ct_id, Convert.ToInt32(SyncBitWise.DownloadChanges), AgentType.Slave);
            } else {
                //since CopyChangeTables doesn't need to be called to fill in CT table list, get it from the slave instead
                PopulateTableList(Config.tables, TServer.SLAVE, Config.slaveCTDB, ref tables, ct_id, null);
            }

            //apply schema changes if not already done
            if ((syncBitWise & Convert.ToInt32(SyncBitWise.ApplySchemaChanges)) == 0) {
                //TODO implement
                //ApplySchemaChanges(Config.tables, TServer.RELAY, Config.relayDB, TServer.SLAVE, Config.slaveCTDB, kvp.Key);
                DataUtils.WriteBitWise(TServer.RELAY, Config.relayDB, ct_id, Convert.ToInt32(SyncBitWise.ApplySchemaChanges), AgentType.Slave);
            }

            //apply changes to destination tables if not already done
            if ((syncBitWise & Convert.ToInt32(SyncBitWise.ApplyChanges)) == 0) {
                //TODO implement
                //ApplyChanges(Config.tables, TServer.SLAVE, Config.slaveCTDB, Config.slaveDB, tables);
                DataUtils.WriteBitWise(TServer.RELAY, Config.relayDB, ct_id, Convert.ToInt32(SyncBitWise.ApplyChanges), AgentType.Slave);
            }

            //update the history tables
            //TODO implement
            //SyncBatchedHistoryTables(Config.tables, TServer.SLAVE, Config.slaveCTDB, Config.slaveDB, tables);


            //success! mark the batch as complete
            DataUtils.MarkBatchComplete(TServer.RELAY, Config.relayDB, ct_id, Convert.ToInt32(SyncBitWise.SyncHistoryTables), DateTime.Now, AgentType.Slave, Config.slave);
        }


        /// <summary>
        /// For the specified list of batches and tables, populate a list of each CT tables exist
        /// </summary>
        /// <param name="t_array">Array of table config objects</param>
        /// <param name="server">Server identifier</param>
        /// <param name="dbName">Database name</param>
        /// <param name="batches">Dictionary of batches, where the key is a CTID</param>
        /// <param name="tables">List of table names to populate</param>
        private void PopulateTableList(TableConf[] t_array, TServer server, string dbName, ref List<string> tables, Int64? ct_id = null, Dictionary<Int64, Int32> batches = null) {
            //TODO add logger statements
            string ctTable;
            if (batches != null) {
                foreach (KeyValuePair<Int64, Int32> kvp in batches) {
                    foreach (TableConf t in t_array) {
                        ctTable = "tblCT" + t.Name + "_" + kvp.Key;
                        if (DataUtils.CheckTableExists(server, dbName, ctTable))
                            tables.Add(ctTable);
                    }
                }
            } else {
                foreach (TableConf t in t_array) {
                    ctTable = "tblCT" + t.Name + "_" + ct_id;
                    if (DataUtils.CheckTableExists(server, dbName, ctTable))
                        tables.Add(ctTable);
                }
            }
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
        /// <param name="tables">Reference variable, list of tables that have >0 changes. Passed by ref instead of output 
        ///     because in multi batch mode it is built up over several calls to this method.</param>
        private void CopyChangeTables(TableConf[] t_array, TServer sourceServer, string sourceCTDB, 
            TServer destServer, string destCTDB, Int64 ct_id, ref List<string> tables) {
            //TODO change from ref variable to returning a list
            //TODO add logger statements
            bool found = false;
            foreach (TableConf t in t_array) {
                found = false;
                string ctTable = "tblCT" + t.Name + "_" + Convert.ToString(ct_id);
                //attempt to copy the change table locally
                try {
                    //hard coding timeout at 1 hour for bulk copy
                    DataUtils.CopyTable(sourceServer, sourceCTDB, ctTable, destServer, destCTDB, 36000);
                    found = true;
                } catch (DoesNotExistException) {
                    //this is a totally normal and expected case since we only publish changetables when data actually changed
                    Logger.Log("No changes to pull for table ctTable because it does not exist ", LogLevel.Debug);
                } catch (Exception e) {
                    if (t.stopOnError) {
                        throw e;
                    } else {
                        Logger.Log("Copying change data for table " + ctTable + " failed with error: " + e.Message, LogLevel.Error);
                    }
                }
                if (found) {
                    tables.Add(ctTable);
                }
            }
        }


        private void ApplySchemaChanges(TableConf[] t_array, TServer sourceServer, string sourceDB, TServer destServer, string destDB, Int64 ct_id) {
            //get list of schema changes from tblCTSChemaChange_ctid on the relay server/db
            DataTable result = DataUtils.GetSchemaChanges(TServer.RELAY, Config.relayDB, ct_id);

            if (result == null) {
                return;
            }

            TableConf t;
            XmlDocument xml;
            foreach (DataRow row in result.Rows) {
                //String.Compare method returns 0 if the strings are equal, the third "true" flag is for a case insensitive comparison
                t = t_array.SingleOrDefault(item => String.Compare(item.Name, row.Field<string>("DdeTable"), ignoreCase: true) == 0);
                if (t == null) {
                    //table isn't in our config so we don't care about this schema change
                    continue;
                }
                xml = (XmlDocument)row["DdeEventData"];
                string eventType = xml.SelectSingleNode("EVENT_INSTANCE/EventType").InnerText;
                string dbName = xml.SelectSingleNode("/EVENT_INSTANCE/DatabaseName").InnerText;
                string schemaName = xml.SelectSingleNode("/EVENT_INSTANCE/SchemaName").InnerText;
                string objectName = xml.SelectSingleNode("/EVENT_INSTANCE/TargetObjectName").InnerText;
                string columnName = xml.SelectSingleNode("/EVENT_INSTANCE/ObjectName").InnerText;
                string newObjectName = xml.SelectSingleNode("/EVENT_INSTANCE/NewObjectName").InnerText;
                string commandText = xml.SelectSingleNode("/EVENT_INSTANCE/TSQLCommand/CommandText").InnerText;

                //TODO decide how we want to handle rename table?               

                XmlNode node = xml.SelectSingleNode("EVENT_INSTANCE/AlterTableActionList");
                if (node == null) {
                    node = xml.SelectSingleNode("EVENT_INSTANCE/Parameters");
                } 
                if (node == null) {
                    //if neither of these nodes are found it's some type of schema change we don't care about
                    continue;
                }
                switch (node.FirstChild.Name) {
                    case "Param":
                        //if there is a column list in config for this table on this slave
                            //if it does not specify this column, just continue
                            //if it does specify this column, do the rename 
                            /*
                             * SELECT @sql = 'EXEC ' + @DBName+'.' + @SchemaName+ '.'+ 'sp_rename ''''' + @ObjectName+ '.' + @ColumnName
				               + '''''' + ',' + '''''' + @NewObjectName + ''''', ''''COLUMN'''''
   						
			               SELECT @AggSQL = 'IF EXISTS(SELECT 1 FROM ' +  db_name() +'.' + 'information_schema.tables where table_name like ''' +
				              @CTprefix + @ObjectName + @AggSuffix + ''' ) ' + CHAR(10) + CHAR(13)+
				              'EXEC ' + DB_NAME() +'.' + @SchemaName+ '.'+ 'sp_rename ''''' + @CTprefix + @ObjectName + @AggSuffix + '.' + @ColumnName
				            + '''''' + ',' + '''''' + @NewObjectName + ''''', ''''COLUMN''''' + CHAR(10) + CHAR(13)
                             */
                        break;
                    case "Alter":
                        //foreach node in /EVENT_INSTANCE/AlterTableActionList/Alter/Columns/Name
                            //if this column exists on this slave (don't bother checking column lists etc.)
                                //run the alter command on this table and the history table 
                                /*
                                 * SELECT @sql = 'ALTER TABLE ' + @DBName+ '.'+@SchemaName+'.'+ @ObjectName+ ' ALTER COLUMN ' 
						           + @ColumnName + ' ' + @column_type		
                                 */
                                //if history table exists, run it there too
                        break;
                    case "Create":
                        //foreach node in /EVENT_INSTANCE/AlterTableActionList/Create/Columns/Name
                            //if columnlist for this table is specified
                        break;
                    case "Drop":
                        break;
                }
                //we should only support ALTER_TABLE and RENAME (rename can rename a column)
            }
        }
            
    }
}
