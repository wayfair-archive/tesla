using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using System.Text;
using System.Data.SqlClient;
using System.Data;
using Microsoft.SqlServer.Management.Smo;
using Microsoft.SqlServer.Management.Common;

namespace TeslaSQL {
    /// <summary>
    /// This class is used for unit tests only as a replacement for DataUtils, so that unit tests don't require an actual database
    /// </summary>
    public class TestDataUtils : IDataUtils {
        //dataset to be filled in by the methods running tests
        public DataSet testData { get; set; }

        public Int64 currentVersion { get; set; }

        public Logger logger;
        public Config config;

        /// <summary>
        /// Constructor for unit tests that use config and logger elements
        /// </summary>
        public TestDataUtils(Config config, Logger logger) {
            this.config = config;
            this.logger = logger;
        }

        /// <summary>
        /// Constructor for unit tests that don't need the config or logger elements
        /// </summary>
        public TestDataUtils() {
        }

        /// <summary>
        /// Helper method for TestDataUtils only for constructing the tablespace from a server identifer and dbName
        /// </summary>
        /// <param name="server">Server identifer</param>
        /// <param name="dbName">Database name</param>
        /// <returns>String representing the tablespace</returns>
        public string GetTableSpace(TServer server, string dbName) {
            return Convert.ToString(server) + "." + dbName;
        }

        public DataRow GetLastCTBatch(TServer server, string dbName, AgentType agentType, string slaveIdentifier = "") {
            //for slave we have to pass the slave identifier in and use tblCTSlaveVersion
            if (agentType.Equals(AgentType.Slave)) {
                DataTable tblCTSlaveVersion = testData.Tables["dbo.tblCTSlaveVersion", GetTableSpace(server, dbName)];
                return tblCTSlaveVersion.Select("slaveIdentifier = '" + slaveIdentifier + "'", "CTID DESC")[0];
            } else {
                DataTable tblCTVersion = testData.Tables["dbo.tblCTVersion", GetTableSpace(server, dbName)];
                return tblCTVersion.Select(null, "CTID DESC")[0];
            }
        }

        public DataTable GetPendingCTVersions(TServer server, string dbName, Int64 CTID, int syncBitWise) {
            /*
             * The below replaces a query like this:
             *
             * SELECT CTID, syncStartVersion, syncStopVersion, syncStartTime, syncBitWise
             * FROM dbo.tblCTVersion WITH(NOLOCK) WHERE CTID > @ctid AND syncBitWise & @syncbitwise > 0
             * ORDER BY CTID ASC
             *
             * But since bitwise operations aren't supported in DataTable expressions, we have to loop through to apply that filter.
             */
            DataTable tblCTVersion = testData.Tables["dbo.tblCTVersion", GetTableSpace(server, dbName)];
            DataRow[] unfilteredResult = tblCTVersion.Select("CTID > " + Convert.ToString(CTID));
            IEnumerable<DataRow> filteredResult = unfilteredResult.Where(item => (item.Field<Int32>("syncBitWise") & syncBitWise) > 0).OrderBy(item => item.Field<Int64>("CTID"));
            var toReturn = new DataTable();

            foreach (DataRow row in filteredResult) {
                toReturn.Rows.Add(row);
            }
            return toReturn;
        }

        public DateTime GetLastStartTime(TServer server, string dbName, Int64 CTID, int syncBitWise) {
            DateTime maxDate = DateTime.Now.AddDays(-1);
            DataTable tblCTVersion = testData.Tables["dbo.tblCTVersion", GetTableSpace(server, dbName)];
            DataRow[] result = tblCTVersion.Select("CTID < " + Convert.ToString(CTID));

            foreach (DataRow row in result) {
                if ((row.Field<Int32>("syncBitWise") & syncBitWise) > 0 && row.Field<DateTime>("syncStartTime") > maxDate) {
                    maxDate = row.Field<DateTime>("syncStartTime");
                }
            }
            return maxDate;
        }

        public Int64 GetCurrentCTVersion(TServer server, string dbName) {
            return currentVersion;
        }

        public Int64 GetMinValidVersion(TServer server, string dbName, string table, string schema) {
            DataTable minValidVersions = testData.Tables["minValidVersions", GetTableSpace(server, dbName)];
            return minValidVersions.Select("table = '" + schema + "." + table + "'")[0].Field<Int64>("version");
        }

        public int SelectIntoCTTable(TServer server, string sourceCTDB, string masterColumnList, string ctTableName,
            string sourceDB, string schema, string tableName, Int64 startVersion, string pkList, Int64 stopVersion, string notNullPkList, int timeout) {
            //no good way to fake this with DataTables so just return and make sure we are also unit testing the
            //methods that generate these sfield lists
            return testData.Tables[schema + "." + ctTableName, GetTableSpace(server, sourceCTDB)].Rows.Count;
        }

        public Int64 CreateCTVersion(TServer server, string dbName, Int64 syncStartVersion, Int64 syncStopVersion) {
            DataTable tblCTVersion = testData.Tables["dbo.tblCTVersion", GetTableSpace(server, dbName)];
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
            //TODO is this required? seems like it may not be
            //tblCTVersion.AcceptChanges();
            return CTID;
        }


        public void CreateSlaveCTVersion(TServer server, string dbName, Int64 CTID, string slaveIdentifier,
            Int64 syncStartVersion, Int64 syncStopVersion, DateTime syncStartTime, Int32 syncBitWise) {

            DataTable tblCTSlaveVersion = testData.Tables["dbo.tblCTSlaveVersion", GetTableSpace(server, dbName)];
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

        public void CreateSchemaChangeTable(TServer server, string dbName, Int64 CTID) {
            DataTable tblCTSchemaChange = new DataTable("dbo.tblCTSchemaChange_" + Convert.ToString(CTID));

            DataTable tblCTSlaveVersion = testData.Tables["dbo.tblCTSlaveVersion", GetTableSpace(server, dbName)];
            DataColumn CscID = tblCTSlaveVersion.Columns.Add("CscID", typeof(Int32));
            CscID.AutoIncrement = true;
            CscID.AutoIncrementSeed = 100;
            CscID.AutoIncrementStep = 1;
            tblCTSlaveVersion.PrimaryKey = new DataColumn[] { CscID };

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

        public DataTable GetDDLEvents(TServer server, string dbName, DateTime afterDate) {
            return testData.Tables["dbo.tblDDLEvent", GetTableSpace(server, dbName)].Select("DdeTime > '" + Convert.ToString(afterDate) + "'").CopyToDataTable();
        }

        public void WriteSchemaChange(TServer server, string dbName, Int64 CTID, int ddeID, string eventType, string schemaName, string tableName,
            string columnName, string newColumnName, string baseType, int? characterMaximumLength, int? numericPrecision, int? numericScale) {
            string schemaChangeTableName = "dbo.tblCTSchemaChange_" + Convert.ToString(CTID);
            DataRow row = testData.Tables[schemaChangeTableName, GetTableSpace(server, dbName)].NewRow();
            //set its values
            row["CscDdeID"] = ddeID;
            row["CscTableName"] = tableName;
            row["CscEventType"] = eventType;
            row["CscSchema"] = schemaName;
            row["CscColumnName"] = columnName;
            row["CscNewColumnName"] = newColumnName;
            row["CscBaseDataType"] = baseType;
            row["CscCharacterMaximumLength"] = characterMaximumLength;
            row["CscNumericPrecision"] = numericPrecision;
            row["CscNumericScale"] = numericScale;
            //add it to the datatable
            testData.Tables[schemaChangeTableName, GetTableSpace(server, dbName)].Rows.Add(row);
            //commit the change
            //testData.Tables[schemaChangeTableName].AcceptChanges();
        }

        public DataRow GetDataType(TServer server, string dbName, string table, string schema, string column) {
            string query = "TABLE_SCHEMA = '" + schema + "' AND TABLE_CATALOG = '" + dbName + "'" +
                " AND TABLE_NAME = '" + table + "' AND COLUMN_NAME = '" + column + "'";
            DataRow[] result = testData.Tables["INFORMATION_SCHEMA.COLUMNS", GetTableSpace(server, dbName)].Select(query);

            if (result == null || result.Length == 0) {
                throw new DoesNotExistException("Column " + column + " does not exist on table " + table + "!");
            }

            return result.CopyToDataTable().Rows[0];
        }

        public void UpdateSyncStopVersion(TServer server, string dbName, Int64 syncStopVersion, Int64 CTID) {
            DataTable tblCTVersion = testData.Tables["dbo.tblCTVersion", GetTableSpace(server, dbName)];
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
        public bool CheckTableExists(TServer server, string dbName, string table, string schema) {
            if (testData.Tables.Contains(schema + "." + table, GetTableSpace(server, dbName))) {
                return true;
            }
            return false;
        }

        public string GetIntersectColumnList(TServer server, string dbName, string table1, string schema1, string table2, string schema2) {
            DataTable dt1 = testData.Tables[schema1 + "." + table1, GetTableSpace(server, dbName)];
            DataTable dt2 = testData.Tables[schema2 + "." + table2, GetTableSpace(server, dbName)];
            string columnList = "";

            //list to hold lowercased column names
            var columns_2 = new List<string>();

            //create this so that casing changes to columns don't cause problems, just use the lowercase column name
            foreach(Column c in dt2.Columns) {
                columns_2.Add(c.Name.ToLower());
            }

            foreach (Column c in dt1.Columns) {
                //case insensitive comparison using ToLower()
                if (columns_2.Contains(c.Name.ToLower())) {
                    if (columnList != "") {
                        columnList += ",";
                    }

                    columnList += "[" + c.Name + "]";
                }
            }
            return columnList;
        }

        public bool HasPrimaryKey(TServer server, string dbName, string table, string schema) {
            return (testData.Tables[schema + "." + table, GetTableSpace(server, dbName)].PrimaryKey.Count() > 0);
        }

        public bool DropTableIfExists(TServer server, string dbName, string table, string schema) {
            if (testData.Tables.Contains(schema + "." + table, GetTableSpace(server, dbName))) {
                testData.Tables.Remove(schema + "." + table, GetTableSpace(server, dbName));
                //testData.AcceptChanges();
                return true;
            }
            return false;
        }

        public void CopyTable(TServer sourceServer, string sourceDB, string table, string schema, TServer destServer, string destDB, int timeout) {
            //create a copy of the source table (data and schema)
            DataTable copy = testData.Tables[schema + "." + table, GetTableSpace(sourceServer, sourceDB)].Copy();
            //change the namespace to be the dest server
            copy.Namespace = GetTableSpace(destServer, destDB);
            //add it to the dataset
            testData.Tables.Add(copy);
            //commit
            //testData.AcceptChanges();
        }

        public Dictionary<string, bool> GetFieldList(TServer server, string dbName, string table, string schema) {
            Dictionary<string, bool> dict = new Dictionary<string, bool>();

            if (!testData.Tables.Contains(schema + "." + table, GetTableSpace(server, dbName))) {
                return dict;
            }

            DataTable dataTable = testData.Tables[schema + "." + table, GetTableSpace(server, dbName)];

            //loop through columns and add them to the dictionary along with whether they are part of the primary key
            foreach (DataColumn c in dataTable.Columns) {
                dict.Add(c.ColumnName, dataTable.PrimaryKey.Contains(c));
            }

            return dict;
        }

        public void WriteBitWise(TServer server, string dbName, Int64 CTID, int value, AgentType agentType) {
            DataTable table;
            DataRow row;
            if (agentType.Equals(AgentType.Slave)) {
                //find the table
                table = testData.Tables["dbo.tblCTSlaveVersion", GetTableSpace(server, dbName)];
                //find the row
                row = table.Select("CTID = " + Convert.ToString(CTID) + " AND slaveIdentifier = '" + config.slave + "'")[0];
            } else {
                //find the table
                table = testData.Tables["dbo.tblCTVersion", GetTableSpace(server, dbName)];
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


        public int ReadBitWise(TServer server, string dbName, Int64 CTID, AgentType agentType) {
            DataTable table;
            DataRow row;
            if (agentType.Equals(AgentType.Slave)) {
                //find the table
                table = testData.Tables["dbo.tblCTSlaveVersion", GetTableSpace(server, dbName)];
                //find the row
                row = table.Select("CTID = " + Convert.ToString(CTID) + " AND slaveIdentifier = '" + config.slave + "'")[0];
            } else {
                //find the table
                table = testData.Tables["dbo.tblCTVersion", GetTableSpace(server, dbName)];
                //find the row
                row = table.Select("CTID = " + Convert.ToString(CTID))[0];
            }
            return row.Field<int>("syncBitWise");
        }

        public void MarkBatchComplete(TServer server, string dbName, Int64 CTID, Int32 syncBitWise, DateTime syncStopTime, AgentType agentType, string slaveIdentifier = "") {
            DataTable table;
            DataRow row;
            if (agentType.Equals(AgentType.Slave)) {
                //find the table
                table = testData.Tables["dbo.tblCTSlaveVersion", GetTableSpace(server, dbName)];
                //find the row
                row = table.Select("CTID = " + Convert.ToString(CTID) + " AND slaveIdentifier = '" + slaveIdentifier + "'")[0];
            } else {
                //find the table
                table = testData.Tables["dbo.tblCTVersion", GetTableSpace(server, dbName)];
                //find the row
                row = table.Select("CTID = " + Convert.ToString(CTID))[0];
            }

            //update the row if it doesn't contain the specified bit
            if ((row.Field<int>("syncBitWise") & syncBitWise) == 0) {
                row["syncBitWise"] = row.Field<int>("syncBitWise") + syncBitWise;
                row["syncStopTime"] = syncStopTime;
                //commit the changes
                //table.AcceptChanges();
            }
        }

        public DataTable GetSchemaChanges(TServer server, string dbName, Int64 CTID) {
            return testData.Tables["dbo.tblCTSchemaChange_" + Convert.ToString(CTID), GetTableSpace(server, dbName)];
        }

        public Int64 GetTableRowCount(TServer server, string dbName, string table, string schema) {
            return testData.Tables[schema + "." + table, GetTableSpace(server, dbName)].Rows.Count;
        }

        public bool IsChangeTrackingEnabled(TServer server, string dbName, string table, string schema) {
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

        public void RenameColumn(TableConf t, TServer server, string dbName, string schema, string table, 
            string columnName, string newColumnName) {
            //TODO implement - can you rename columns on a datatable?
            //should just be able to do dt.Columns[x].ColumnName = newColumnName
            /*
            var cmd = new SqlCommand("EXEC sp_rename @objname, @newname, 'COLUMN'");
            cmd.Parameters.Add("@objname", SqlDbType.VarChar, 500).Value = schema + "." + table + "." + columnName;
            cmd.Parameters.Add("@newname", SqlDbType.VarChar, 500).Value = newColumnName;

            int result = SqlNonQuery(server, dbName, cmd);
            //check for history table, if it is configured we need to modify that too
            if (t.recordHistoryTable) {
                cmd = new SqlCommand("EXEC sp_rename @objname, @newname, 'COLUMN'");
                //TODO verify the _History suffix is correct
                cmd.Parameters.Add("@objname", SqlDbType.VarChar, 500).Value = schema + "." + table + "_History." + columnName;
                cmd.Parameters.Add("@newname", SqlDbType.VarChar, 500).Value = newColumnName;
                result = SqlNonQuery(server, dbName, cmd);
            }
            */
            
        }
    }
}
