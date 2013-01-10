using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using System.Text;
using System.Data.SqlClient;
using System.Data;
using Microsoft.SqlServer.Management.Smo;
using Microsoft.SqlServer.Management.Common;

namespace TeslaSQL.DataUtils {
    /// <summary>
    /// This class is used for unit tests only as a replacement for DataUtils, so that unit tests don't require an actual database
    /// </summary>
    public class TestDataUtils : IDataUtils {
        //dataset to be filled in by the methods running tests
        public DataSet testData { get; set; }

        public Int64 currentVersion { get; set; }

        public string server { get; set; }

        public Logger logger;

        /// <summary>
        /// Constructor for unit tests that use config and logger elements
        /// </summary>
        public TestDataUtils(Logger logger, TServer server) {
            this.logger = logger;
            this.server = Convert.ToString(server);
            testData = new DataSet();
        }

        /// <summary>
        /// Constructor for unit tests that don't need the config or logger elements
        /// </summary>
        public TestDataUtils(TServer server) {
            this.server = Convert.ToString(server);
            testData = new DataSet();
        }

        /// <summary>
        /// Reloads test data from files
        /// </summary>
        /// <param name="testName">Name of the test set to load</param>
        public void ReloadData(string testName) {
            //baseDir resolves to something like "C:\tesla\TeslaSQL\bin\Release"
            string baseDir = AppDomain.CurrentDomain.BaseDirectory;
            DataSet ds = new DataSet();

            //test xml/xsd files are copied during build to bin\Release\Tests
            string filePath = baseDir + @"\Tests\" + testName + @"\input_data_" + server + ".xml";
            ds.ReadXml(filePath, XmlReadMode.ReadSchema);
            testData = ds;
        }

        /// <summary>
        /// Helper method for TestDataUtils only for constructing the tablespace from a server identifer and dbName
        /// </summary>
        /// <param name="server">Server identifer</param>
        /// <param name="dbName">Database name</param>
        /// <returns>String representing the tablespace</returns>
        public string GetTableSpace(string dbName) {
            return Convert.ToString(server) + "." + dbName;
        }

        public DataRow GetLastCTBatch(string dbName, AgentType agentType, string slaveIdentifier = "") {
            //for slave we have to pass the slave identifier in and use tblCTSlaveVersion
            if (agentType.Equals(AgentType.Slave)) {
                DataTable tblCTSlaveVersion = testData.Tables["dbo.tblCTSlaveVersion", GetTableSpace(dbName)];
                return tblCTSlaveVersion.Select("slaveIdentifier = '" + slaveIdentifier + "'", "CTID DESC")[0];
            } else {
                DataTable tblCTVersion = testData.Tables["dbo.tblCTVersion", GetTableSpace(dbName)];
                return tblCTVersion.Select(null, "CTID DESC")[0];
            }
        }

        public DataTable GetPendingCTVersions(string dbName, Int64 CTID, int syncBitWise) {
            /*
             * The below replaces a query like this:
             *
             * SELECT CTID, syncStartVersion, syncStopVersion, syncStartTime, syncBitWise
             * FROM dbo.tblCTVersion WITH(NOLOCK) WHERE CTID > @ctid AND syncBitWise & @syncbitwise > 0
             * ORDER BY CTID ASC
             *
             * But since bitwise operations aren't supported in DataTable expressions, we have to loop through to apply that filter.
             */
            DataTable tblCTVersion = testData.Tables["dbo.tblCTVersion", GetTableSpace(dbName)];
            DataRow[] unfilteredResult = tblCTVersion.Select("CTID > " + Convert.ToString(CTID));
            IEnumerable<DataRow> filteredResult = unfilteredResult.Where(item => (item.Field<Int32>("syncBitWise") & syncBitWise) > 0).OrderBy(item => item.Field<Int64>("CTID"));
            var toReturn = new DataTable();

            foreach (DataRow row in filteredResult) {
                toReturn.Rows.Add(row);
            }
            return toReturn;
        }

        public DataTable GetPendingCTSlaveVersions(string dbName, string slaveIdentifier) {
            throw new NotImplementedException("still have to implement");
        }


        public DateTime GetLastStartTime(string dbName, Int64 CTID, int syncBitWise, AgentType type) {
            DateTime maxDate = DateTime.Now.AddDays(-1);
            DataTable tblCTVersion = testData.Tables["dbo.tblCTVersion", GetTableSpace(dbName)];
            DataRow[] result = tblCTVersion.Select("CTID < " + Convert.ToString(CTID));

            foreach (DataRow row in result) {
                if ((row.Field<Int32>("syncBitWise") & syncBitWise) > 0 && row.Field<DateTime>("syncStartTime") > maxDate) {
                    maxDate = row.Field<DateTime>("syncStartTime");
                }
            }
            return maxDate;
        }

        public Int64 GetCurrentCTVersion(string dbName) {
            return currentVersion;
        }

        public Int64 GetMinValidVersion(string dbName, string table, string schema) {
            DataTable minValidVersions = testData.Tables["minValidVersions", GetTableSpace(dbName)];
            return minValidVersions.Select("table = '" + schema + "." + table + "'")[0].Field<Int64>("version");
        }

        public int SelectIntoCTTable(string sourceCTDB, string masterColumnList, string ctTableName,
            string sourceDB, string schema, string tableName, Int64 startVersion, string pkList, Int64 stopVersion, string notNullPkList, int timeout) {
            //no good way to fake this with DataTables so just return and make sure we are also unit testing the
            //methods that generate these sfield lists
            return testData.Tables[schema + "." + ctTableName, GetTableSpace(sourceCTDB)].Rows.Count;
        }

        public Int64 CreateCTVersion(string dbName, Int64 syncStartVersion, Int64 syncStopVersion) {
            DataTable tblCTVersion = testData.Tables["dbo.tblCTVersion", GetTableSpace(dbName)];
            DataRow row = tblCTVersion.NewRow();
            //this will be generated automatically since it's an auto increment column
            Int64 CTID = row.Field<Int64>("CTID");
            //set the values
            row["syncStartVersion"] = syncStartVersion;
            row["syncStopVersion"] = syncStopVersion;
            row["syncStartTime"] = DateTime.Now;
            row["syncBitWise"] = 0;
            //add it to the datatable
            tblCTVersion.Rows.Add(row);
            //commit the change
            return CTID;
        }


        public void CreateSlaveCTVersion(string dbName, Int64 CTID, string slaveIdentifier,
            Int64 syncStartVersion, Int64 syncStopVersion, DateTime syncStartTime, Int32 syncBitWise) {

            DataTable tblCTSlaveVersion = testData.Tables["dbo.tblCTSlaveVersion", GetTableSpace(dbName)];
            //create the row
            DataRow row = tblCTSlaveVersion.NewRow();
            //set its values
            row["CTID"] = CTID;
            row["slaveIdentifier"] = slaveIdentifier;
            row["syncStartVersion"] = syncStartVersion;
            row["syncStopVersion"] = syncStopVersion;
            row["syncStartTime"] = syncStartTime;
            row["syncBitWise"] = syncBitWise;
            //add it to the datatable
            tblCTSlaveVersion.Rows.Add(row);
            //commit the change
            //tblCTSlaveVersion.AcceptChanges();
        }

        public void CreateSchemaChangeTable(string dbName, Int64 CTID) {
            DataTable tblCTSchemaChange = new DataTable("dbo.tblCTSchemaChange_" + Convert.ToString(CTID), GetTableSpace(dbName));

            DataColumn CscID = tblCTSchemaChange.Columns.Add("CscID", typeof(Int32));
            CscID.AutoIncrement = true;
            CscID.AutoIncrementSeed = 100;
            CscID.AutoIncrementStep = 1;
            tblCTSchemaChange.PrimaryKey = new DataColumn[] { CscID };

            tblCTSchemaChange.Columns.Add("CscDdeID", typeof(Int32));
            tblCTSchemaChange.Columns.Add("CscTableName", typeof(string));
            tblCTSchemaChange.Columns.Add("CscEventType", typeof(string));
            tblCTSchemaChange.Columns.Add("CscSchema", typeof(string));
            tblCTSchemaChange.Columns.Add("CscColumnName", typeof(string));
            tblCTSchemaChange.Columns.Add("CscNewColumnName", typeof(string));
            tblCTSchemaChange.Columns.Add("CscBaseDataType", typeof(string));
            tblCTSchemaChange.Columns.Add("CscCharacterMaximumLength", typeof(Int32));
            tblCTSchemaChange.Columns.Add("CscNumericPrecision", typeof(Int32));
            tblCTSchemaChange.Columns.Add("CscNumericScale", typeof(Int32));

            testData.Tables.Add(tblCTSchemaChange);
        }

        public DataTable GetDDLEvents(string dbName, DateTime afterDate) {
            return testData.Tables["dbo.tblDDLEvent", GetTableSpace(dbName)].Select("DdeTime > '" + Convert.ToString(afterDate) + "'").CopyToDataTable();
        }

        public void WriteSchemaChange(string dbName, Int64 CTID, SchemaChange schemaChange) {
            string schemaChangeTableName = "dbo.tblCTSchemaChange_" + Convert.ToString(CTID);
            DataRow row = testData.Tables[schemaChangeTableName, GetTableSpace(dbName)].NewRow();
            //set its values
            row["CscDdeID"] = schemaChange.ddeID;
            row["CscTableName"] = schemaChange.tableName;
            row["CscEventType"] = schemaChange.eventType;
            row["CscSchema"] = schemaChange.schemaName;
            row["CscColumnName"] = schemaChange.columnName;
            row["CscNewColumnName"] = schemaChange.newColumnName;
            row["CscBaseDataType"] = schemaChange.dataType.baseType;
            row["CscCharacterMaximumLength"] = (object)schemaChange.dataType.characterMaximumLength ?? DBNull.Value;
            row["CscNumericPrecision"] = (object)schemaChange.dataType.numericPrecision ?? DBNull.Value;
            row["CscNumericScale"] = (object)schemaChange.dataType.numericScale ?? DBNull.Value;
            //add it to the datatable
            testData.Tables[schemaChangeTableName, GetTableSpace(dbName)].Rows.Add(row);
            //commit the change
            //testData.Tables[schemaChangeTableName].AcceptChanges();
        }

        public DataRow GetDataType(string dbName, string table, string schema, string column) {
            string query = "TABLE_SCHEMA = '" + schema + "' AND TABLE_CATALOG = '" + dbName + "'" +
                " AND TABLE_NAME = '" + table + "' AND COLUMN_NAME = '" + column + "'";
            DataRow[] result = testData.Tables["INFORMATION_SCHEMA.COLUMNS", GetTableSpace(dbName)].Select(query);

            if (result == null || result.Length == 0) {
                throw new DoesNotExistException("Column " + column + " does not exist on table " + table + "!");
            }

            return result.CopyToDataTable().Rows[0];
        }

        public void UpdateSyncStopVersion(string dbName, Int64 syncStopVersion, Int64 CTID) {
            DataTable tblCTVersion = testData.Tables["dbo.tblCTVersion", GetTableSpace(dbName)];
            //find the row
            DataRow row = tblCTVersion.Select("CTID = " + Convert.ToString(CTID))[0];
            //edit it
            row["syncStopVersion"] = syncStopVersion;
            //commit it
            //tblCTVersion.AcceptChanges();
        }

        /// <summary>
        /// Check to see if a table exists on the specified server
        /// </summary>
        /// <param name="server">Server to check</param>
        /// <param name="dbName">Database name</param>
        /// <param name="table">Table name to chekc for</param>
        /// <returns>Boolean representing whether or not the table exists.</returns>
        public bool CheckTableExists(string dbName, string table, string schema) {
            if (testData.Tables.Contains(schema + "." + table, GetTableSpace(dbName))) {
                return true;
            }
            return false;
        }


        public IEnumerable<string> GetIntersectColumnList(string dbName, string table1, string schema1, string table2, string schema2) {
            DataTable dt1 = testData.Tables[schema1 + "." + table1, GetTableSpace(dbName)];
            DataTable dt2 = testData.Tables[schema2 + "." + table2, GetTableSpace(dbName)];
            var columns1 = new List<string>();
            var columns2 = new List<string>();
            //create this so that casing changes to columns don't cause problems, just use the lowercase column name
            foreach (Column c in dt1.Columns) {
                columns1.Add(c.Name.ToLower());
            }
            foreach (Column c in dt2.Columns) {
                columns2.Add(c.Name.ToLower());
            }
            return columns1.Intersect(columns2);
        }

        public bool HasPrimaryKey(string dbName, string table, string schema) {
            return (testData.Tables[schema + "." + table, GetTableSpace(dbName)].PrimaryKey.Count() > 0);
        }

        public bool DropTableIfExists(string dbName, string table, string schema) {
            if (table.Contains("tblCT") && server == "MASTER") {
                //workaround for tests to prevent dropping of preloaded CT tables
                return true;
            }
            if (testData.Tables.Contains(schema + "." + table, GetTableSpace(dbName))) {
                testData.Tables.Remove(schema + "." + table, GetTableSpace(dbName));
                //testData.AcceptChanges();
                return true;
            }
            return false;
        }

        public Dictionary<string, bool> GetFieldList(string dbName, string table, string schema) {
            Dictionary<string, bool> dict = new Dictionary<string, bool>();

            if (!testData.Tables.Contains(schema + "." + table, GetTableSpace(dbName))) {
                return dict;
            }

            DataTable dataTable = testData.Tables[schema + "." + table, GetTableSpace(dbName)];

            //loop through columns and add them to the dictionary along with whether they are part of the primary key
            foreach (DataColumn c in dataTable.Columns) {
                dict.Add(c.ColumnName, dataTable.PrimaryKey.Contains(c));
            }

            return dict;
        }

        public void WriteBitWise(string dbName, Int64 CTID, int value, AgentType agentType) {
            DataTable table;
            DataRow row;
            if (agentType.Equals(AgentType.Slave)) {
                //find the table
                table = testData.Tables["dbo.tblCTSlaveVersion", GetTableSpace(dbName)];
                //find the row
                row = table.Select("CTID = " + Convert.ToString(CTID) + " AND slaveIdentifier = '" + Config.slave + "'")[0];
            } else {
                //find the table
                table = testData.Tables["dbo.tblCTVersion", GetTableSpace(dbName)];
                //find the row
                row = table.Select("CTID = " + Convert.ToString(CTID))[0];
            }

            //update the row if it doesn't contain the specified bit
            if ((row.Field<int>("syncBitWise") & value) == 0) {
                row["syncBitWise"] = row.Field<int>("syncBitWise") + value;
                //commit the changes
                //table.AcceptChanges();
            }
        }


        public int ReadBitWise(string dbName, Int64 CTID, AgentType agentType) {
            DataTable table;
            DataRow row;
            if (agentType.Equals(AgentType.Slave)) {
                //find the table
                table = testData.Tables["dbo.tblCTSlaveVersion", GetTableSpace(dbName)];
                //find the row
                row = table.Select("CTID = " + Convert.ToString(CTID) + " AND slaveIdentifier = '" + Config.slave + "'")[0];
            } else {
                //find the table
                table = testData.Tables["dbo.tblCTVersion", GetTableSpace(dbName)];
                //find the row
                row = table.Select("CTID = " + Convert.ToString(CTID))[0];
            }
            return row.Field<int>("syncBitWise");
        }

        public void MarkBatchComplete(string dbName, Int64 CTID, DateTime syncStopTime, string slaveIdentifier) {
            //find the table
            DataTable table = testData.Tables["dbo.tblCTSlaveVersion", GetTableSpace(dbName)];
            //find the row
            DataRow row = table.Select("CTID = " + Convert.ToString(CTID) + " AND slaveIdentifier = '" + slaveIdentifier + "'")[0];

            //update the row 
            row["syncBitWise"] = Enum.GetValues(typeof(SyncBitWise)).Cast<int>().Sum();
            row["syncStopTime"] = syncStopTime;
        }

        public DataTable GetSchemaChanges(string dbName, Int64 CTID) {
            return testData.Tables["dbo.tblCTSchemaChange_" + Convert.ToString(CTID), GetTableSpace(dbName)];
        }

        public Int64 GetTableRowCount(string dbName, string table, string schema) {
            return testData.Tables[schema + "." + table, GetTableSpace(dbName)].Rows.Count;
        }

        public bool IsChangeTrackingEnabled(string dbName, string table, string schema) {
            return true;
        }


        /// <summary>
        /// Compare expected dataset with actual dataset to see if all expected rows are present
        /// </summary>
        /// <param name="expected">DataSet with expected results</param>
        /// <param name="actual">Actual dataset</param>
        /// <returns>Bool indicating whether the two data sets contain the same rows.</returns>
        public static bool CompareDataSets(DataSet expected, DataSet actual) {
            /*
             * The canonical method of doing this would be to merge the two datasets
             * and look for changed rows, but unfortunately due to a bug that method doesn't work at all since
             * even rows that are the same show up with DataRowState.Modified. Since we're comparing small amounts
             * of data, this nested loops approach works as an alternative.
             *
             */
            foreach (DataTable expectedDT in expected.Tables) {
                DataTable actualDT = actual.Tables[expectedDT.TableName, expectedDT.Namespace];
                if (expectedDT.Rows.Count != actualDT.Rows.Count) {
                    //write this to the console so that the reason shows up in unit test output
                    Console.WriteLine("Rowcounts not equal for table " + expectedDT.TableName);
                    return false;
                }

                //for instead of foreach here because we want the index
                for (int i = 0; i < expectedDT.Rows.Count; i++) {
                    var expectedRow = expectedDT.Rows[i];
                    var actualRow = actualDT.Rows[i];
                    if (!expectedRow.ItemArray.SequenceEqual(actualRow.ItemArray)) {
                        Console.WriteLine("Row mismatch for table " + expectedDT.TableName);
                        return false;
                    }
                }
            }
            return true;
        }

        public void LogError(string message) {
            return;
        }

        public DataTable GetUnsentErrors() {
            return new DataTable();
        }


        public void MarkErrorsSent(IEnumerable<int> celIds) {
            return;
        }

        public void RenameColumn(TableConf t, string dbName, string schema, string table,
            string columnName, string newColumnName) {
            DataTable dt = testData.Tables[schema + "." + table, GetTableSpace(dbName)];
            dt.Columns[columnName].ColumnName = newColumnName;
        }

        public void ModifyColumn(TableConf t, string dbName, string schema, string table, string columnName, string dataType) {
            //can't change the datatype of a column in a datatable but since this is just for unit testing, we can just drop and recreate it
            //instead since there is no data to worry about losing       
            DropColumn(t, dbName, schema, table, columnName);
            AddColumn(t, dbName, schema, table, columnName, dataType);
        }

        public void AddColumn(TableConf t, string dbName, string schema, string table, string columnName, string dataType) {
            DataTable dt = testData.Tables[schema + "." + table, GetTableSpace(dbName)];
            Type type;
            //since this is just for unit testing we only need to support a subset of data types     
            switch (dataType) {
                case "int":
                    type = typeof(Int32);
                    break;
                case "bigint":
                    type = typeof(Int64);
                    break;
                case "datetime":
                    type = typeof(DateTime);
                    break;
                default:
                    throw new NotImplementedException("Data type " + dataType + " not supported for testing");
            }
            dt.Columns.Add(columnName, type);
        }

        public void DropColumn(TableConf t, string dbName, string schema, string table, string columnName) {
            DataTable dt = testData.Tables[schema + "." + table, GetTableSpace(dbName)];
            dt.Columns.Remove(columnName);
        }

        public void CreateTableInfoTable(string p, long p_2) {
            DataTable tblCTTableInfo = new DataTable("dbo.tblCTTableInfo_" + Convert.ToString(p_2), GetTableSpace(p));

            DataColumn CscID = tblCTTableInfo.Columns.Add("CtiID", typeof(Int32));
            CscID.AutoIncrement = true;
            CscID.AutoIncrementSeed = 100;
            CscID.AutoIncrementStep = 1;
            tblCTTableInfo.PrimaryKey = new DataColumn[] { CscID };

            tblCTTableInfo.Columns.Add("CtiTableName", typeof(string));
            tblCTTableInfo.Columns.Add("CtiSchemaName", typeof(string));
            tblCTTableInfo.Columns.Add("CtiPKList", typeof(string));
            tblCTTableInfo.Columns.Add("CtiExpectedRows", typeof(int));
            testData.Tables.Add(tblCTTableInfo);
        }

        public void PublishTableInfo(string dbName, TableConf t, long CTID, long expectedRows) {
            DataTable table = testData.Tables["dbo.tblCTTableInfo_" + Convert.ToString(CTID), GetTableSpace(dbName)];
            DataRow row = table.NewRow();
            row["CtiTableName"] = t.Name;
            row["CtiSchemaName"] = t.schemaName;
            row["CtiPKList"] = string.Join(",", t.columns.Where(c => c.isPk));
            row["CtiExpectedRows"] = expectedRows;
            table.Rows.Add(row);
        }


        public void CreateSlaveCTVersion(string dbName, ChangeTrackingBatch ctb, string slaveIdentifier) {
            throw new NotImplementedException();
        }


        public void CreateConsolidatedTable(string tableName, Int64 CTID, string schemaName, string dbName) {
            throw new NotImplementedException("Still need to implement");
        }

        public void Consolidate(string tableName, long CTID, string dbName, string schemaName) {
            throw new NotImplementedException("Still need to implement");
        }

        public void RemoveDuplicatePrimaryKeyChangeRows(string p) {
            throw new NotImplementedException("Still need to implement");
        }


        public void RemoveDuplicatePrimaryKeyChangeRows(TableConf table, string dbName) {
            throw new NotImplementedException();
        }


        public void RemoveDuplicatePrimaryKeyChangeRows(TableConf table, string consolidatedTableName, string dbName) {
            throw new NotImplementedException();
        }


        public void CreateConsolidatedTable(string originalName, string schemaName, string dbName, string consolidatedName) {
            throw new NotImplementedException();
        }

        public void Consolidate(string ctTableName, string consolidatedTableName, string dbName, string schemaName) {
            throw new NotImplementedException();
        }


        public RowCounts ApplyTableChanges(TableConf table, TableConf archiveTable, string dbName, long ctid, string CTDBName) {
            throw new NotImplementedException();
        }


        public void CreateHistoryTable(ChangeTable t, string slaveCTDB) {
            throw new NotImplementedException();
        }

        public void CopyIntoHistoryTable(ChangeTable t, string slaveCTDB) {
            throw new NotImplementedException();
        }


        public Dictionary<string, Dictionary<long, ChangeTrackingBatch>> GetCTBatchMap = new Dictionary<string, Dictionary<long, ChangeTrackingBatch>>();
        public ChangeTrackingBatch GetCTBatch(string dbName, long ctid) {
            if (GetCTBatchMap.ContainsKey(dbName) && GetCTBatchMap[dbName].ContainsKey(ctid)) {
                return GetCTBatchMap[dbName][ctid];
            }
            return new ChangeTrackingBatch(0, 0, 0, 0);
        }

        public void RevertCTBatch(string dbName, long ctid) {
            //no op we should do nothing for any side-effect only operation
        }

        public void MergeCTTable(TableConf table, string destDB, string sourceDB, long CTID) {
            throw new NotImplementedException();
        }


        public void CreateShardCTVersion(string db, long ctid, long startVersion) {
            throw new NotImplementedException();
        }


        public IEnumerable<string> GetPrimaryKeysFromInfoTable(TableConf table, ChangeTrackingBatch batch, string database) {
            throw new NotImplementedException();
        }


        public int GetExpectedRowCounts(string ctDbName, long ctid) {
            throw new NotImplementedException();
        }


        public IEnumerable<TTable> GetTables(string p) {
            throw new NotImplementedException();
        }

        public IEnumerable<long> GetOldCTIDsMaster(string p, DateTime chopDate) {
            throw new NotImplementedException();
        }

        public void DeleteOldCTVersionsMaster(string p, DateTime chopDate, AgentType agentType) {
            throw new NotImplementedException();
        }


        public void DeleteOldCTVersions(string dbName, DateTime chopDate) {
            throw new NotImplementedException();
        }


        public void DeleteOldCTSlaveVersions(string dbName, DateTime chopDate) {
            throw new NotImplementedException();
        }

        public IEnumerable<long> GetOldCTIDsRelay(string dbName, DateTime chopDate) {
            throw new NotImplementedException();
        }


        public void DeleteOldCTSlaveVersions(string dbName, DateTime chopDate, IEnumerable<long> ctids) {
            throw new NotImplementedException();
        }
    }
}
