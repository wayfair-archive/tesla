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
#endregion

namespace TeslaSQL.Agents {
    //TODO throughout this class add error handling for tables that shouldn't stop on error
    //TODO we need to set up field lists somewhere in here...
    //TODO figure out where to put check for MSSQL vs. netezza and where to branch the code paths
    public class Slave : Agent {
        public Slave(Config config, IDataUtils sourceDataUtils, IDataUtils destDataUtils) {
            this.config = config;
            this.sourceDataUtils = sourceDataUtils;
            this.destDataUtils = destDataUtils;
            //log server is source since source is relay for slave
            this.logger = new Logger(config.logLevel, config.statsdHost, config.statsdPort, config.errorLogDB, sourceDataUtils);
        }

        public Slave() {
            //this constructor is only used by for running unit tests
            this.logger = new Logger(LogLevel.Critical, null, null, null);
        }

        public override void ValidateConfig() {
            Config.ValidateRequiredHost(config.relayServer);
            Config.ValidateRequiredHost(config.slave);
            if (config.relayType == null || config.slaveType == null) {
                throw new Exception("Slave agent requires a valid SQL flavor for relay and slave");
            }
        }

        public override void Run() {
            logger.Log("Initializing CT batch", LogLevel.Trace);
            List<ChangeTrackingBatch> batches = InitializeBatch();

            /**
             * If you run a batch as Multi, and that batch fails, and before the next run,
             * you increase the batchConsolidationThreshold, this can lead to unexpected behaviour.
             */
            if (config.batchConsolidationThreshold == 0 || batches.Count < config.batchConsolidationThreshold) {
                foreach (var batch in batches) {
                    RunSingleBatch(batch);
                }
            } else {
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
            ChangeTrackingBatch ctb;

            var incompleteBatches = sourceDataUtils.GetPendingCTSlaveVersions(config.relayDB);
            if (incompleteBatches.Rows.Count > 0) {
                foreach (DataRow row in incompleteBatches.Rows) {
                    batches.Add(new ChangeTrackingBatch(row));
                }
                return batches;
            }
            //get the last CT version this slave worked on in tblCTSlaveVersion
            logger.Log("Retrieving information on last run for slave " + config.slave, LogLevel.Debug);

            DataRow lastBatch = sourceDataUtils.GetLastCTBatch(config.relayDB, AgentType.Slave, config.slave);
            if (lastBatch == null) {
                ctb = new ChangeTrackingBatch(1, 0, 0, 0);
                batches.Add(ctb);
                return batches;
            }

            //compare bitwise to the bit for last step of slave agent
            if ((lastBatch.Field<Int32>("syncBitWise") & Convert.ToInt32(SyncBitWise.SyncHistoryTables)) > 0) {
                logger.Log("Last batch was successful, checking for new batches.", LogLevel.Debug);

                DataTable pendingVersions = sourceDataUtils.GetPendingCTVersions(config.relayDB, lastBatch.Field<Int64>("CTID"), Convert.ToInt32(SyncBitWise.UploadChanges));
                logger.Log("Retrieved " + pendingVersions.Rows.Count + " pending CT version(s) to work on.", LogLevel.Debug);

                foreach (DataRow row in pendingVersions.Rows) {
                    ctb = new ChangeTrackingBatch(row);
                    batches.Add(ctb);
                    sourceDataUtils.CreateSlaveCTVersion(config.relayDB, ctb, config.slave);
                }
                return batches;
            }
            ctb = new ChangeTrackingBatch(lastBatch);
            logger.Log("Last batch failed, retrying CTID " + ctb.CTID, LogLevel.Warn);
            batches.Add(ctb);
            return batches;
        }


        /// <summary>
        /// Consolidates multiple batches and runs them as a group
        /// </summary>
        /// <param name="ctidTable">DataTable object listing all the batches</param>
        private void RunMultiBatch(List<ChangeTrackingBatch> batches) {
            //TODO add logger statements
            List<string> existingCTTables = new List<string>();

            //get last batch in hte list
            ChangeTrackingBatch endBatch = batches.OrderBy(item => item.CTID).Last();

            foreach (ChangeTrackingBatch batch in batches) {
                logger.Log("Populating list of changetables for CTID : " + batch.CTID, LogLevel.Debug);            
                existingCTTables = existingCTTables.Concat(PopulateTableList(config.tables, config.slaveCTDB, batch.CTID)).ToList();
            }

            //consolidate the change sets into one changetable per table
            if ((endBatch.syncBitWise & Convert.ToInt32(SyncBitWise.ConsolidateBatches)) == 0) {
                //persist bitwise progress to database
                sourceDataUtils.WriteBitWise(config.relayDB, endBatch.CTID, Convert.ToInt32(SyncBitWise.ConsolidateBatches), AgentType.Slave);
                ConsolidateBatches(existingCTTables, batches);
                sourceDataUtils.WriteBitWise(config.relayDB, endBatch.CTID, Convert.ToInt32(SyncBitWise.ConsolidateBatches), AgentType.Slave);
            }

            if ((endBatch.syncBitWise & Convert.ToInt32(SyncBitWise.DownloadChanges)) == 0) {
                logger.Log("Downloading consolidated changetables", LogLevel.Debug);
                CopyChangeTables(config.tables, config.relayDB, config.slaveCTDB, endBatch.CTID, isConsolidated:true);
                logger.Log("Changes downloaded successfully", LogLevel.Debug);
                sourceDataUtils.WriteBitWise(config.relayDB, endBatch.CTID, Convert.ToInt32(SyncBitWise.DownloadChanges), AgentType.Slave);
            } 

            //loop through each batch and apply schema changes
            foreach (ChangeTrackingBatch batch in batches) {
                if ((batch.syncBitWise & Convert.ToInt32(SyncBitWise.ApplySchemaChanges)) == 0) {
                    //copy the change tables for each batch if it hasn't been done yet
                    logger.Log("Applying schema changes", LogLevel.Debug);
                    ApplySchemaChanges(config.tables, config.slaveDB, batch.CTID);
                    sourceDataUtils.WriteBitWise(config.relayDB, batch.CTID, Convert.ToInt32(SyncBitWise.ApplySchemaChanges), AgentType.Slave);
                }
            }

            //from here forward all operations will use the bitwise value for the last CTID since they are operating on this whole set of batches
            
            if ((endBatch.syncBitWise & Convert.ToInt32(SyncBitWise.ApplyChanges)) == 0) {
                //TODO implement
                //ApplyBatchedChanges(config.tables, config.slaveCTDB, config.slaveDB, tables);
                //persist bitwise progress to database
                sourceDataUtils.WriteBitWise(config.relayDB, endBatch.CTID, Convert.ToInt32(SyncBitWise.ApplyChanges), AgentType.Slave);
            }

            //final step, synchronize history tables
            //TODO implement
            //SyncBatchedHistoryTables(config.tables, config.slaveCTDB, config.slaveDB, tables);
            //success! go through and mark all the batches as complete in the db
            foreach (ChangeTrackingBatch batch in batches) {
                sourceDataUtils.MarkBatchComplete(config.relayDB, batch.CTID, Convert.ToInt32(SyncBitWise.SyncHistoryTables), DateTime.Now, AgentType.Slave, config.slave);
            }
        }

        private void ConsolidateBatches(List<string> tables, List<ChangeTrackingBatch> batches) {
            var lu = new Dictionary<string, List<Int64>>();
            foreach (var tableName in tables) {
                var lastUnderscore = tableName.LastIndexOf('_');
                var name = tableName.Substring(0, lastUnderscore);
                var ctid = int.Parse(tableName.Substring(lastUnderscore + 1));
                if (!lu.ContainsKey(name)) {
                    lu[name] = new List<Int64>();
                }
                lu[name].Add(ctid);
            }
            foreach (var table in config.tables) {
                var ctName = CTTableName(table.Name);
                if (!lu.ContainsKey(ctName)) {
                    continue;
                }
                var ctid = lu[ctName].OrderByDescending(c => c).First();

                destDataUtils.CreateConsolidatedTable(ctName, ctid, table.schemaName, config.slaveCTDB);
                foreach (var c in lu[ctName].OrderByDescending(c => c)) {
                    destDataUtils.Consolidate(ctName, c, config.slaveCTDB, table.schemaName);
                }
                destDataUtils.RemoveDuplicatePrimaryKeyChangeRows(table, ctName, config.slaveCTDB);
            }
        }

        /// <summary>
        /// Runs a single change tracking batch
        /// </summary>
        /// <param name="CTID">Change tracking batch object to work on</param>
        private void RunSingleBatch(ChangeTrackingBatch ctb) {
            //TODO add logger statements
            List<string> existingCTTables = new List<string>();

            if ((ctb.syncBitWise & Convert.ToInt32(SyncBitWise.DownloadChanges)) == 0) {
                existingCTTables = CopyChangeTables(config.tables, config.relayDB, config.slaveCTDB, ctb.CTID);
                sourceDataUtils.WriteBitWise(config.relayDB, ctb.CTID, Convert.ToInt32(SyncBitWise.DownloadChanges), AgentType.Slave);
                //marking this field so that completed slave batches will have the same values
                sourceDataUtils.WriteBitWise(config.relayDB, ctb.CTID, Convert.ToInt32(SyncBitWise.ConsolidateBatches), AgentType.Slave);
            } else {
                //since CopyChangeTables doesn't need to be called to fill in CT table list, get it from the slave instead
                existingCTTables = PopulateTableList(config.tables, config.slaveCTDB, ctb.CTID);
            }

            //apply schema changes if not already done
            if ((ctb.syncBitWise & Convert.ToInt32(SyncBitWise.ApplySchemaChanges)) == 0) {
                ApplySchemaChanges(config.tables, config.slaveDB, ctb.CTID);
                sourceDataUtils.WriteBitWise(config.relayDB, ctb.CTID, Convert.ToInt32(SyncBitWise.ApplySchemaChanges), AgentType.Slave);
                //marking this field so that all completed slave batches will have the same values
                sourceDataUtils.WriteBitWise(config.relayDB, ctb.CTID, Convert.ToInt32(SyncBitWise.ConsolidateBatches), AgentType.Slave);
            }
            //apply changes to destination tables if not already done
            if ((ctb.syncBitWise & Convert.ToInt32(SyncBitWise.ApplyChanges)) == 0) {
                ApplyChanges(config.tables, config.slaveDB, existingCTTables, ctb.CTID);
                sourceDataUtils.WriteBitWise(config.relayDB, ctb.CTID, Convert.ToInt32(SyncBitWise.ApplyChanges), AgentType.Slave);
            }

            //update the history tables
            //TODO implement
            //SyncBatchedHistoryTables(config.tables, config.slaveCTDB, config.slaveDB, tables);

            //success! mark the batch as complete
            sourceDataUtils.MarkBatchComplete(config.relayDB, ctb.CTID, Convert.ToInt32(SyncBitWise.SyncHistoryTables), DateTime.Now, AgentType.Slave, config.slave);
        }

        private void ApplyChanges(TableConf[] tableConf, string slaveDB, List<string> tables, Int64 CTID) {            
            SetFieldLists(slaveDB, tableConf, destDataUtils);
            var hasArchive = new Dictionary<TableConf, TableConf>();
            foreach (var table in tableConf) {
                if (tables.Contains(CTTableName(table.Name, CTID))) {
                    if (hasArchive.ContainsKey(table)) {
                        //so we don't grab tblOrderArchive, insert tlbOrder: tblOrderArchive, and then go back and insert tblOrder: null.
                        continue;
                    }
                    if (table.Name.EndsWith("Archive")) {
                        string nonArchiveTableName = CTTableName(table.Name.Substring(0, table.Name.Length - table.Name.LastIndexOf("Archive")), CTID);
                        if (tables.Contains(nonArchiveTableName)) {
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
            foreach (var tableArchive in hasArchive) {
                destDataUtils.ApplyTableChanges(tableArchive.Key, tableArchive.Value, config.slaveDB, CTID);
            }
        }


        /// <summary>
        /// For the specified list of tables, populate a list of which CT tables exist
        /// </summary>
        /// <param name="tables">Array of table config objects</param>
        /// <param name="dbName">Database name</param>
        /// <param name="tables">List of table names to populate</param>
        private List<string> PopulateTableList(TableConf[] tables, string dbName, Int64 CTID) {
            //TODO add logger statements
            var tableList = new List<string>();
            string ctTableName;
            foreach (TableConf t in tables) {
                ctTableName = CTTableName(t.Name, CTID);
                if (sourceDataUtils.CheckTableExists(dbName, ctTableName, t.schemaName)) {
                    tableList.Add(t.schemaName + "." + ctTableName);
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
                return "tblCT" + table + "_" + config.slave;
            }
        }


        /// <summary>
        /// Copies change tables from the master to the relay server
        /// </summary>
        /// <param name="tables">Array of table config objects</param>
        /// <param name="sourceCTDB">Source CT database</param>
        /// <param name="destCTDB">Dest CT database</param>
        /// <param name="CTID">CT batch ID this is for</param>
        /// <param name="tables">Reference variable, list of tables that have >0 changes. Passed by ref instead of output
        ///     because in multi batch mode it is built up over several calls to this method.</param>
        private List<string> CopyChangeTables(TableConf[] tables, string sourceCTDB, string destCTDB, Int64 CTID, bool isConsolidated = false) {
            bool found = false;
            List<string> tableList = new List<string>();
            IDataCopy dataCopy = DataCopyFactory.GetInstance((SqlFlavor)config.relayType, (SqlFlavor)config.slaveType, sourceDataUtils, destDataUtils);
            foreach (TableConf t in tables) {
                found = false;
                string sourceCTTable = isConsolidated ? CTTableName(t.Name, null) : CTTableName(t.Name, CTID);
                string destCTTable = CTTableName(t.Name, CTID);
                //attempt to copy the change table locally
                try {
                    //hard coding timeout at 1 hour for bulk copy
                    dataCopy.CopyTable(sourceCTDB, sourceCTTable, t.schemaName, destCTDB, 36000, destCTTable);
                    logger.Log("Copied table " + t.schemaName + "." + sourceCTTable + " to slave", LogLevel.Trace);
                    found = true;
                } catch (DoesNotExistException) {
                    //this is a totally normal and expected case since we only publish changetables when data actually changed
                    logger.Log("No changes to pull for table " + t.schemaName + "." + sourceCTTable + " because it does not exist ", LogLevel.Debug);
                } catch (Exception e) {
                    if (t.stopOnError) {
                        throw e;
                    } else {
                        logger.Log("Copying change data for table " + t.schemaName + "." + sourceCTTable + " failed with error: " + e.Message, LogLevel.Error);
                    }
                }
                if (found) {
                    tableList.Add(destCTTable);
                }
            }
            return tableList;
        }

        private void ApplySchemaChanges(TableConf[] tables, string destDB, Int64 CTID) {
            //get list of schema changes from tblCTSChemaChange_ctid on the relay server/db
            DataTable result = sourceDataUtils.GetSchemaChanges(config.relayDB, CTID);

            if (result == null) {
                return;
            }

            TableConf t;
            foreach (DataRow row in result.Rows) {
                var schemaChange = new SchemaChange(row);
                //String.Compare method returns 0 if the strings are equal, the third "true" flag is for a case insensitive comparison
                t = tables.SingleOrDefault(item => String.Compare(item.Name, schemaChange.tableName, ignoreCase: true) == 0);

                if (t == null) {
                    logger.Log("Ignoring schema change for table " + row.Field<string>("CscTableName") + " because it isn't in config", LogLevel.Debug);
                    continue;
                }
                logger.Log("Processing schema change (CscID: " + row.Field<int>("CscID") +
                    ") of type " + schemaChange.eventType + " for table " + t.Name, LogLevel.Info);

                if (t.columnList == null || t.columnList.Contains(schemaChange.columnName, StringComparer.OrdinalIgnoreCase)) {
                    logger.Log("Schema change applies to a valid column, so we will apply it", LogLevel.Info);
                    switch (schemaChange.eventType) {
                        case SchemaChangeType.Rename:
                            logger.Log("Renaming column " + schemaChange.columnName + " to " + schemaChange.newColumnName, LogLevel.Info);
                            destDataUtils.RenameColumn(t, destDB, schemaChange.schemaName, schemaChange.tableName,
                                schemaChange.columnName, schemaChange.newColumnName);
                            break;
                        case SchemaChangeType.Modify:
                            logger.Log("Changing data type on column " + schemaChange.columnName, LogLevel.Info);
                            destDataUtils.ModifyColumn(t, destDB, schemaChange.schemaName, schemaChange.tableName, schemaChange.columnName,
                                 schemaChange.dataType.baseType, schemaChange.dataType.characterMaximumLength,
                                 schemaChange.dataType.numericPrecision, schemaChange.dataType.numericScale);
                            break;
                        case SchemaChangeType.Add:
                            logger.Log("Adding column " + schemaChange.columnName, LogLevel.Info);
                            destDataUtils.AddColumn(t, destDB, schemaChange.schemaName, schemaChange.tableName, schemaChange.columnName,
                                 schemaChange.dataType.baseType, schemaChange.dataType.characterMaximumLength,
                                 schemaChange.dataType.numericPrecision, schemaChange.dataType.numericScale);
                            break;
                        case SchemaChangeType.Drop:
                            logger.Log("Dropping column " + schemaChange.columnName, LogLevel.Info);
                            destDataUtils.DropColumn(t, destDB, schemaChange.schemaName, schemaChange.tableName, schemaChange.columnName);
                            break;
                    }

                } else {
                    logger.Log("Skipped schema change because the column it impacts is not in our list", LogLevel.Info);
                }

            }
        }

        #region Unit Tests
        /// <summary>
        /// Subclass so that we can implement the IUseFixture feature
        /// </summary>
        public class ApplySchemaChangeTest : IUseFixture<ApplySchemaChangeTestData> {
            public TableConf[] tables;
            public TestDataUtils sourceDataUtils;
            public TestDataUtils destDataUtils;
            public Slave slave;

            /// <summary>
            /// xunit will automatically create an instance of DDLEventTestData and pass it to this method before running tests
            /// </summary>
            public void SetFixture(ApplySchemaChangeTestData data) {
                this.tables = data.tables;
                this.sourceDataUtils = data.sourceDataUtils;
                this.destDataUtils = data.destDataUtils;
                this.slave = data.slave;
            }

            [Fact]
            public void TestApply_AddSingleColumn_WithoutColumnList() {
                //add a column and make sure it is published
                DataTable dt = sourceDataUtils.testData.Tables["dbo.tblCTSchemaChange_1", "RELAY.CT_testdb"];
                //gets rid of all the rows
                dt.Clear();
                //create a row                
                DataRow row = dt.NewRow();
                row["CscDdeID"] = 1;
                row["CscTableName"] = "test1";
                row["CscEventType"] = "Add";
                row["CscSchema"] = "dbo";
                row["CscColumnName"] = "testadd";
                row["CscNewColumnName"] = DBNull.Value;
                row["CscBaseDataType"] = "int";
                row["CscCharacterMaximumLength"] = DBNull.Value;
                row["CscNumericPrecision"] = DBNull.Value;
                row["CscNumericScale"] = DBNull.Value;
                dt.Rows.Add(row);

                destDataUtils.DropTableIfExists("testdb", "test1", "dbo");
                DataTable test1 = new DataTable("dbo.test1", "SLAVE.testdb");
                test1.Columns.Add("column1", typeof(Int32));
                test1.Columns.Add("column2", typeof(Int32));
                destDataUtils.testData.Tables.Add(test1);

                slave.ApplySchemaChanges(tables, "testdb", 1);

                var expected = new DataColumn("testadd", typeof(Int32));
                var actual = destDataUtils.testData.Tables["dbo.test1", "SLAVE.testdb"].Columns["testadd"];
                //assert.equal doesn't work for objects but this does
                Assert.True(expected.ColumnName == actual.ColumnName && expected.DataType == actual.DataType);
            }

            [Fact]
            public void TestApply_AddSingleColumn_WithColumnInList() {
                //add a column and make sure it is published
                DataTable dt = sourceDataUtils.testData.Tables["dbo.tblCTSchemaChange_1", "RELAY.CT_testdb"];
                //gets rid of all the rows
                dt.Clear();
                //create a row                
                DataRow row = dt.NewRow();
                row["CscDdeID"] = 1;
                row["CscTableName"] = "test2";
                row["CscEventType"] = "Add";
                row["CscSchema"] = "dbo";
                row["CscColumnName"] = "column2";
                row["CscNewColumnName"] = DBNull.Value;
                row["CscBaseDataType"] = "int";
                row["CscCharacterMaximumLength"] = DBNull.Value;
                row["CscNumericPrecision"] = DBNull.Value;
                row["CscNumericScale"] = DBNull.Value;
                dt.Rows.Add(row);

                destDataUtils.DropTableIfExists("testdb", "test2", "dbo");
                DataTable test2 = new DataTable("dbo.test2", "SLAVE.testdb");
                test2.Columns.Add("column1", typeof(Int32));
                test2.Columns.Add("column3", typeof(string));
                destDataUtils.testData.Tables.Add(test2);

                slave.ApplySchemaChanges(tables, "testdb", 1);

                var expected = new DataColumn("column2", typeof(Int32));
                var actual = destDataUtils.testData.Tables["dbo.test2", "SLAVE.testdb"].Columns["column2"];
                Assert.True(expected.ColumnName == actual.ColumnName && expected.DataType == actual.DataType);
            }

            [Fact]
            public void TestApply_AddSingleColumn_WithColumnNotInList() {
                //add a column and make sure it is published
                DataTable dt = sourceDataUtils.testData.Tables["dbo.tblCTSchemaChange_1", "RELAY.CT_testdb"];
                //gets rid of all the rows
                dt.Clear();
                //create a row                
                DataRow row = dt.NewRow();
                row["CscDdeID"] = 1;
                row["CscTableName"] = "test2";
                row["CscEventType"] = "Add";
                row["CscSchema"] = "dbo";
                row["CscColumnName"] = "testadd";
                row["CscNewColumnName"] = DBNull.Value;
                row["CscBaseDataType"] = "int";
                row["CscCharacterMaximumLength"] = DBNull.Value;
                row["CscNumericPrecision"] = DBNull.Value;
                row["CscNumericScale"] = DBNull.Value;
                dt.Rows.Add(row);

                destDataUtils.DropTableIfExists("testdb", "test2", "dbo");
                DataTable test2 = new DataTable("dbo.test2", "SLAVE.testdb");
                test2.Columns.Add("column1", typeof(Int32));
                test2.Columns.Add("column3", typeof(string));
                destDataUtils.testData.Tables.Add(test2);

                slave.ApplySchemaChanges(tables, "testdb", 1);

                Assert.False(destDataUtils.testData.Tables["dbo.test2", "SLAVE.testdb"].Columns.Contains("testadd"));
            }

            [Fact]
            public void TestApply_RenameColumn() {
                //add a column and make sure it is published
                DataTable dt = sourceDataUtils.testData.Tables["dbo.tblCTSchemaChange_1", "RELAY.CT_testdb"];
                //gets rid of all the rows
                dt.Clear();
                //create a row                
                DataRow row = dt.NewRow();
                row["CscDdeID"] = 1;
                row["CscTableName"] = "test1";
                row["CscEventType"] = "Rename";
                row["CscSchema"] = "dbo";
                row["CscColumnName"] = "column2";
                row["CscNewColumnName"] = "column2_new";
                row["CscBaseDataType"] = DBNull.Value;
                row["CscCharacterMaximumLength"] = DBNull.Value;
                row["CscNumericPrecision"] = DBNull.Value;
                row["CscNumericScale"] = DBNull.Value;
                dt.Rows.Add(row);

                destDataUtils.DropTableIfExists("testdb", "test1", "dbo");
                DataTable test1 = new DataTable("dbo.test1", "SLAVE.testdb");
                test1.Columns.Add("column1", typeof(Int32));
                test1.Columns.Add("column2", typeof(Int32));
                destDataUtils.testData.Tables.Add(test1);

                slave.ApplySchemaChanges(tables, "testdb", 1);

                Assert.True(destDataUtils.testData.Tables["dbo.test1", "SLAVE.testdb"].Columns.Contains("column2_new"));
                destDataUtils.testData.Tables["dbo.test1", "SLAVE.testdb"].RejectChanges();
            }

            [Fact]
            public void TestApply_DropColumn() {
                //add a column and make sure it is published
                DataTable dt = sourceDataUtils.testData.Tables["dbo.tblCTSchemaChange_1", "RELAY.CT_testdb"];
                //gets rid of all the rows
                dt.Clear();
                //create a row                
                DataRow row = dt.NewRow();
                row["CscDdeID"] = 1;
                row["CscTableName"] = "test1";
                row["CscEventType"] = "Drop";
                row["CscSchema"] = "dbo";
                row["CscColumnName"] = "column2";
                row["CscNewColumnName"] = DBNull.Value;
                row["CscBaseDataType"] = DBNull.Value;
                row["CscCharacterMaximumLength"] = DBNull.Value;
                row["CscNumericPrecision"] = DBNull.Value;
                row["CscNumericScale"] = DBNull.Value;
                dt.Rows.Add(row);

                destDataUtils.DropTableIfExists("testdb", "test1", "dbo");
                DataTable test1 = new DataTable("dbo.test1", "SLAVE.testdb");
                test1.Columns.Add("column1", typeof(Int32));
                test1.Columns.Add("column2", typeof(Int32));
                destDataUtils.testData.Tables.Add(test1);

                //if this assert fails it means the test setup got borked
                Assert.True(destDataUtils.testData.Tables["dbo.test1", "SLAVE.testdb"].Columns.Contains("column2"));
                slave.ApplySchemaChanges(tables, "testdb", 1);
                Assert.False(destDataUtils.testData.Tables["dbo.test1", "SLAVE.testdb"].Columns.Contains("column2"));
            }

            [Fact]
            public void TestApply_ModifyColumn() {
                //add a column and make sure it is published
                DataTable dt = sourceDataUtils.testData.Tables["dbo.tblCTSchemaChange_1", "RELAY.CT_testdb"];
                //gets rid of all the rows
                dt.Clear();
                //create a row                
                DataRow row = dt.NewRow();
                row["CscDdeID"] = 1;
                row["CscTableName"] = "test1";
                row["CscEventType"] = "Modify";
                row["CscSchema"] = "dbo";
                row["CscColumnName"] = "column2";
                row["CscNewColumnName"] = DBNull.Value;
                row["CscBaseDataType"] = "datetime";
                row["CscCharacterMaximumLength"] = DBNull.Value;
                row["CscNumericPrecision"] = DBNull.Value;
                row["CscNumericScale"] = DBNull.Value;
                dt.Rows.Add(row);

                destDataUtils.DropTableIfExists("testdb", "test1", "dbo");
                DataTable test1 = new DataTable("dbo.test1", "SLAVE.testdb");
                test1.Columns.Add("column1", typeof(Int32));
                test1.Columns.Add("column2", typeof(Int32));
                destDataUtils.testData.Tables.Add(test1);

                //if this assert fails it means the test setup got borked
                var expected = new DataColumn("column2", typeof(DateTime));

                slave.ApplySchemaChanges(tables, "testdb", 1);
                var actual = destDataUtils.testData.Tables["dbo.test1", "SLAVE.testdb"].Columns["column2"];

                Assert.True(expected.ColumnName == actual.ColumnName && expected.DataType == actual.DataType);
            }

        }

        /// <summary>
        /// This class is invoked by xunit.net and passed to SetFixture in the ApplySchemaChangeTest class
        /// </summary>
        public class ApplySchemaChangeTestData {
            public DataSet testData;
            public TableConf[] tables;
            public TestDataUtils sourceDataUtils;
            public TestDataUtils destDataUtils;
            public Slave slave;

            public ApplySchemaChangeTestData() {
                tables = new TableConf[2];
                //first table has no column list
                tables[0] = new TableConf();
                tables[0].Name = "test1";

                //second one has column list
                tables[1] = new TableConf();
                tables[1].Name = "test2";
                tables[1].columnList = new string[2] { "column1", "column2" };

                sourceDataUtils = new TestDataUtils(TServer.RELAY);
                destDataUtils = new TestDataUtils(TServer.SLAVE);

                testData = new DataSet();
                sourceDataUtils.testData = new DataSet();
                destDataUtils.testData = new DataSet();
                //this method, conveniently, sets up the datatable schema we need
                sourceDataUtils.CreateSchemaChangeTable("CT_testdb", 1);

                var config = new Config();
                config.relayDB = "CT_testdb";
                config.logLevel = LogLevel.Critical;
                slave = new Slave(config, (IDataUtils)sourceDataUtils, (IDataUtils)destDataUtils);
            }
        }

        #endregion
    }
}
