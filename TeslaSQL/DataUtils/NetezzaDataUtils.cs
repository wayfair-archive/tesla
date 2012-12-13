using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using System.Text;
using System.Data.SqlClient;
using System.Data;
using Microsoft.SqlServer.Management.Smo;
using Microsoft.SqlServer.Management.Common;
using System.Data.OleDb;
namespace TeslaSQL.DataUtils {
    public class NetezzaDataUtils : IDataUtils {

        public Logger logger;
        public Config config;
        public TServer server;

        public NetezzaDataUtils(Config config, Logger logger, TServer server) {
            this.config = config;
            this.logger = logger;
            this.server = server;
        }

        /// <summary>
        /// Runs a sql query and returns results as requested type
        /// </summary>
        /// <param name="dbName">Database name</param>
        /// <param name="cmd">SqlCommand to run</param>
        /// <param name="timeout">Query timeout</param>
        /// <returns>DataTable object representing the result</returns>
        private DataTable SqlQuery(string dbName, OleDbCommand cmd, int timeout = 30) {
            //build connection string based on server/db info passed in
            string connStr = buildConnString(dbName);

            //using block to avoid resource leaks
            using (OleDbConnection conn = new OleDbConnection(connStr)) {
                //open database connection
                conn.Open();
                cmd.Connection = conn;
                cmd.CommandTimeout = timeout;

                DataSet ds = new DataSet();
                OleDbDataAdapter da = new OleDbDataAdapter(cmd);
                //this is where the query is run
                da.Fill(ds);

                //return the result, which is the first DataTable in the DataSet
                return ds.Tables[0];
            }
        }

        /// <summary>
        /// Runs a sql query and returns first column and row from results as specified type
        /// </summary>
        /// <param name="dbName">Database name</param>
        /// <param name="cmd">SqlCommand to run</param>
        /// <param name="timeout">Query timeout</param>
        /// <returns>The value in the first column and row, as the specified type</returns>
        private T SqlQueryToScalar<T>(string dbName, OleDbCommand cmd, int timeout = 30) {
            DataTable result = SqlQuery(dbName, cmd, timeout);
            //return result in first column and first row as specified type
            return (T)result.Rows[0][0];
        }

        /// <summary>
        /// Runs a query that does not return results (i.e. a write operation)
        /// </summary>
        /// <param name="dbName">Database name</param>
        /// <param name="cmd">SqlCommand to run</param>
        /// <param name="timeout">Timeout (higher than selects since some writes can be large)</param>
        /// <returns>The number of rows affected</returns>
        private int SqlNonQuery(string dbName, OleDbCommand cmd, int timeout = 600) {
            //build connection string based on server/db info passed in
            string connStr = buildConnString(dbName);
            int numrows;
            //using block to avoid resource leaks
            using (OleDbConnection conn = new OleDbConnection(connStr)) {
                try {
                    //open database connection
                    conn.Open();
                    cmd.Connection = conn;
                    cmd.CommandTimeout = timeout;
                    numrows = cmd.ExecuteNonQuery();
                } catch (Exception e) {
                    //TODO figure out what to catch/rethrow
                    throw e;
                }
            }
            return numrows;
        }


        /// <summary>
        /// Builds a connection string for the passed in server identifier using global config values
        /// </summary>
        /// <param name="server">Server identifier</param>
        /// <param name="database">Database name</param>
        /// <returns>An ADO.NET connection string</returns>
        private string buildConnString(string database) {
            string sqlhost = "";
            string sqluser = "";
            string sqlpass = "";

            switch (server) {
                case TServer.SLAVE:
                    sqlhost = config.slave;
                    sqluser = config.slaveUser;
                    sqlpass = (new cTripleDes().Decrypt(config.slavePassword));
                    break;
                default:
                    throw new NotImplementedException("Netezza is only supported as a slave!");
            }
            return "Data Source=" + sqlhost + "; Initial Catalog=" + database + ";User ID=" + sqluser + ";Password=" + sqlpass + "Provider=NZOLEDB;";
        }


        public DataRow GetLastCTBatch(string dbName, AgentType agentType, string slaveIdentifier = "") {
            throw new NotImplementedException("Netezza is only supported as a slave!");
        }


        public DataTable GetPendingCTVersions(string dbName, Int64 CTID, int syncBitWise) {
            throw new NotImplementedException("Netezza is only supported as a slave!");
        }

        public DataTable GetPendingCTSlaveVersions(string dbName) {
            throw new NotImplementedException("Netezza is only supported as a slave!");
        }


        public DateTime GetLastStartTime(string dbName, Int64 CTID, int syncBitWise) {
            throw new NotImplementedException("Netezza is only supported as a slave!");
        }


        public Int64 GetCurrentCTVersion(string dbName) {
            throw new NotImplementedException("Netezza is only supported as a slave!");
        }


        public Int64 GetMinValidVersion(string dbName, string table, string schema) {
            throw new NotImplementedException("Netezza is only supported as a slave!");
        }


        public int SelectIntoCTTable(string sourceCTDB, string masterColumnList, string ctTableName,
            string sourceDB, string schemaName, string tableName, Int64 startVersion, string pkList, Int64 stopVersion, string notNullPkList, int timeout) {
            
            throw new NotImplementedException("Netezza is only supported as a slave!");
        }


        public Int64 CreateCTVersion(string dbName, Int64 syncStartVersion, Int64 syncStopVersion) {
            throw new NotImplementedException("Netezza is only supported as a slave!");
        }


        public void CreateSlaveCTVersion(string dbName, ChangeTrackingBatch ctb, string slaveIdentifier) {
            throw new NotImplementedException("Netezza is only supported as a slave!");
        }


        public void CreateSchemaChangeTable(string dbName, Int64 CTID) {
            throw new NotImplementedException("Netezza is only supported as a slave!");
        }


        public DataTable GetDDLEvents(string dbName, DateTime afterDate) {
            throw new NotImplementedException("Netezza is only supported as a slave!");
        }


        public void WriteSchemaChange(string dbName, Int64 CTID, SchemaChange schemaChange) {
            throw new NotImplementedException("Netezza is only supported as a slave!");
        }


        public DataRow GetDataType(string dbName, string table, string schema, string column) {
            throw new NotImplementedException("Netezza is only supported as a slave!");
        }


        public void UpdateSyncStopVersion(string dbName, Int64 syncStopVersion, Int64 CTID) {
            throw new NotImplementedException("Netezza is only supported as a slave!");
        }


        public bool CheckTableExists(string dbName, string table, string schema = "dbo") {
            throw new NotImplementedException("Still need to implement!");
            /*
            try {
                Table t_smo = GetSmoTable(server, dbName, table, schema);

                if (t_smo != null) {
                    return true;
                }
                return false;
            } catch (DoesNotExistException) {
                return false;
            }
             */
        }


        public IEnumerable<string> GetIntersectColumnList(string dbName, string table1, string schema1, string table2, string schema2) {
            throw new NotImplementedException("Not sure if we need this yet!");
            /*
            Table t_smo_1 = GetSmoTable(server, dbName, table1, schema1);
            Table t_smo_2 = GetSmoTable(server, dbName, table2, schema2);
            string columnList = "";

            //list to hold lowercased column names
            var columns_2 = new List<string>();

            //create this so that casing changes to columns don't cause problems, just use the lowercase column name
            foreach (Column c in t_smo_2.Columns) {
                columns_2.Add(c.Name.ToLower());
            }

            foreach (Column c in t_smo_1.Columns) {
                //case insensitive comparison using ToLower()
                if (columns_2.Contains(c.Name.ToLower())) {
                    if (columnList != "") {
                        columnList += ",";
                    }

                    columnList += "[" + c.Name + "]";
                }
            }
            return columnList;
             */
        }


        public bool HasPrimaryKey(string dbName, string tableName, string schema) {
            throw new NotImplementedException("Netezza is only supported as a slave!");
        }


        public bool DropTableIfExists(string dbName, string table, string schema) {
            throw new NotImplementedException("Still need to implement this!");
            /*
            try {
                Table t_smo = GetSmoTable(server, dbName, table, schema);
                if (t_smo != null) {
                    t_smo.Drop();
                    return true;
                }
                return false;
            } catch (DoesNotExistException) {
                return false;
            }
             */
        }

        public Dictionary<string, bool> GetFieldList(string dbName, string table, string schema) {
            throw new NotImplementedException("This still needs to be implemented!");
            /*Dictionary<string, bool> dict = new Dictionary<string, bool>();
            Table t_smo;

            //attempt to get smo table object
            try {
                t_smo = GetSmoTable(server, dbName, table, schema);
            } catch (DoesNotExistException) {
                //TODO figure out if we also want to throw here
                logger.Log("Unable to get field list for table " + table + " because it does not exist", LogLevel.Error);
                return dict;
            }

            //loop through columns and add them to the dictionary along with whether they are part of the primary key
            foreach (Column c in t_smo.Columns) {
                dict.Add(c.Name, c.InPrimaryKey);
            }

            return dict;
             */
        }


        public void WriteBitWise(string dbName, Int64 CTID, int value, AgentType agentType) {
            throw new NotImplementedException("Netezza is only supported as a slave!");
        }


        public int ReadBitWise(string dbName, Int64 CTID, AgentType agentType) {
            throw new NotImplementedException("Netezza is only supported as a slave!");
        }


        public void MarkBatchComplete(string dbName, Int64 CTID, Int32 syncBitWise, DateTime syncStopTime, AgentType agentType, string slaveIdentifier = "") {
            throw new NotImplementedException("Netezza is only supported as a slave!");
        }


        public DataTable GetSchemaChanges(string dbName, Int64 CTID) {
            throw new NotImplementedException("Netezza is only supported as a slave!");
        }

        public Int64 GetTableRowCount(string dbName, string table, string schema) {
            throw new NotImplementedException("Netezza is only supported as a slave!");
        }

        public bool IsChangeTrackingEnabled(string dbName, string table, string schema) {
            throw new NotImplementedException("Netezza is only supported as a slave!");
        }

        public void LogError(string message) {
            throw new NotImplementedException("Netezza is only supported as a slave!");
        }

        public DataTable GetUnsentErrors() {
            throw new NotImplementedException("Netezza is only supported as a slave!");
        }

        public void MarkErrorsSent(IEnumerable<int> celIds) {
            throw new NotImplementedException("Netezza is only supported as a slave!"); ;
        }

        private bool CheckColumnExists(string dbName, string schema, string table, string column) {
            throw new NotImplementedException("Still need to implement");
            //TODO change to something that queries system tables
            /*
            Table t_smo = GetSmoTable(server, dbName, table, schema);
            if (t_smo.Columns.Contains(column)) {
                return true;
            }
            return false;
             */
        }

        public void RenameColumn(TableConf t, string dbName, string schema, string table,
            string columnName, string newColumnName) {

            throw new NotImplementedException("Still need to implement");
            /*
            SqlCommand cmd;
            //rename the column if it exists
            if (CheckColumnExists(dbName, schema, table, columnName)) {
                cmd = new SqlCommand("EXEC sp_rename @objname, @newname, 'COLUMN'");
                cmd.Parameters.Add("@objname", SqlDbType.VarChar, 500).Value = schema + "." + table + "." + columnName;
                cmd.Parameters.Add("@newname", SqlDbType.VarChar, 500).Value = newColumnName;
                logger.Log("Altering table with command: " + cmd.CommandText, LogLevel.Debug);
                SqlNonQuery(dbName, cmd);
            }
            //check for history table, if it is configured and contains the column we need to modify that too
            if (t.recordHistoryTable && CheckColumnExists(server, dbName, schema, table + "_History", columnName)) {
                cmd = new SqlCommand("EXEC sp_rename @objname, @newname, 'COLUMN'");
                cmd.Parameters.Add("@objname", SqlDbType.VarChar, 500).Value = schema + "." + table + "_History." + columnName;
                cmd.Parameters.Add("@newname", SqlDbType.VarChar, 500).Value = newColumnName;
                logger.Log("Altering history table column with command: " + cmd.CommandText, LogLevel.Debug);
                SqlNonQuery(cmd);
            }
             * */
        }

        public void ModifyColumn(TableConf t, string dbName, string schema, string table,
            string columnName, string baseType, int? characterMaximumLength, int? numericPrecision, int? numericScale) {

            throw new NotImplementedException("Still need to implement");
            /*
            var typesUsingMaxLen = new string[4] { "varchar", "nvarchar", "char", "nchar" };
            var typesUsingScale = new string[2] { "numeric", "decimal" };
            string suffix = "";
            string query;
            SqlCommand cmd;
            if (typesUsingMaxLen.Contains(baseType) && characterMaximumLength != null) {
                //(n)varchar(max) types stored with a maxlen of -1, so change that to max
                suffix = "(" + (characterMaximumLength == -1 ? "max" : Convert.ToString(characterMaximumLength)) + ")";
            } else if (typesUsingScale.Contains(baseType) && numericPrecision != null && numericScale != null) {
                suffix = "(" + numericPrecision + ", " + numericScale + ")";
            }

            //Modify the column if it exists
            if (CheckColumnExists(server, dbName, schema, table, columnName)) {
                query = "ALTER TABLE " + schema + "." + table + " ALTER COLUMN " + columnName + " " + baseType;
                cmd = new SqlCommand(query + suffix);
                logger.Log("Altering table column with command: " + cmd.CommandText, LogLevel.Debug);
                SqlNonQuery(dbName, cmd);
            }
             * */
        }

        public void AddColumn(TableConf t, string dbName, string schema, string table,
            string columnName, string baseType, int? characterMaximumLength, int? numericPrecision, int? numericScale) {
            
            throw new NotImplementedException("Still need to implement");
            /*
            string query;
            SqlCommand cmd;
            var typesUsingMaxLen = new string[4] { "varchar", "nvarchar", "char", "nchar" };
            var typesUsingScale = new string[2] { "numeric", "decimal" };

            string suffix = "";
            if (typesUsingMaxLen.Contains(baseType) && characterMaximumLength != null) {
                //(n)varchar(max) types stored with a maxlen of -1, so change that to max
                suffix = "(" + (characterMaximumLength == -1 ? "max" : Convert.ToString(characterMaximumLength)) + ")";
            } else if (typesUsingScale.Contains(baseType) && numericPrecision != null && numericScale != null) {
                suffix = "(" + numericPrecision + ", " + numericScale + ")";
            }
            //add column if it doesn't exist
            if (!CheckColumnExists(server, dbName, schema, table, columnName)) {
                query = "ALTER TABLE " + schema + "." + table + " ADD " + columnName + " " + baseType;
                cmd = new SqlCommand(query + suffix);
                logger.Log("Altering table with command: " + cmd.CommandText, LogLevel.Debug);
                SqlNonQuery(server, dbName, cmd);
            }
             */
        }

        public void DropColumn(TableConf t, string dbName, string schema, string table, string columnName) {
            throw new NotImplementedException("Still need to implement");
            /*
            SqlCommand cmd;
            //drop column if it exists
            if (CheckColumnExists(server, dbName, schema, table, columnName)) {
                cmd = new SqlCommand("ALTER TABLE " + schema + "." + table + " DROP COLUMN " + columnName);
                logger.Log("Altering table with command: " + cmd.CommandText, LogLevel.Debug);
                SqlNonQuery(server, dbName, cmd);
            }
             */
        }



        public void CreateTableInfoTable(string p, long p_2) {
            throw new NotImplementedException();
        }

        public void PublishTableInfo(string dbName, TableConf t, long CTID, long expectedRows) {
            throw new NotImplementedException("Netezza is only supported as a slave!");
        }

        public void ApplyTableChanges(TableConf table, TableConf archiveTable, string dbName, long ctid) {
            throw new NotImplementedException();
        }

        public void CreateConsolidatedTable(string tableName, long CTID, string schemaName, string dbName) {
            throw new NotImplementedException();
        }

        public void Consolidate(string tableName, long CTID, string dbName, string schemaName) {
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


        public void ApplyTableChanges(TableConf table, TableConf archiveTable, string dbName, long ctid, string CTDBName) {
            throw new NotImplementedException();
        }
    }
}

