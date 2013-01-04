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
        internal DataTable SqlQuery(string dbName, OleDbCommand cmd, int timeout = 30) {
            string connStr = buildConnString(dbName);

            using (OleDbConnection conn = new OleDbConnection(connStr)) {
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
        internal int SqlNonQuery(string dbName, OleDbCommand cmd, int timeout = 600) {
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
            return "Data Source=" + sqlhost + "; Initial Catalog=" + database + ";User ID=" + sqluser + ";Password=" + sqlpass + ";Provider=NZOLEDB;";
        }

        public Dictionary<string, bool> GetFieldList(string dbName, string table, string schema) {
            var cols = new Dictionary<string, bool>();
            using (var con = new OleDbConnection(buildConnString(dbName))) {
                con.Open();
                //this dark magic is (sort of) documented here 
                //http://msdn.microsoft.com/en-us/library/system.data.oledb.oledbschemaguid.tables.aspx
                var t = con.GetOleDbSchemaTable(OleDbSchemaGuid.Columns, new object[] { null, null, table, null });
                foreach (DataRow row in t.Rows) {
                    cols[row.Field<string>("COLUMN_NAME")] = false;
                }
            }
            return cols;
        }

        public bool DropTableIfExists(string dbName, string table, string schema) {
            if (CheckTableExists(dbName, table, schema)) {
                string drop = string.Format("DROP TABLE {0};", table);
                var cmd = new OleDbCommand(drop);
                return SqlNonQuery(dbName, cmd) > 0;
            }
            return false;

        }

        public bool CheckTableExists(string dbName, string table, string schema = "dbo") {
            string sql = "SELECT * FROM _v_table WHERE TABLENAME = @tablename";
            var cmd = new OleDbCommand(sql);
            cmd.Parameters.Add("@tablename", OleDbType.VarChar, 200).Value = table.ToUpper();
            var res = SqlQuery(dbName, cmd);
            return res.Rows.Count > 0;
        }
        public void CopyIntoHistoryTable(ChangeTable t, string slaveCTDB) {
            throw new NotImplementedException();
        }
        public void ApplyTableChanges(TableConf table, TableConf archiveTable, string dbName, long ctid) {
            throw new NotImplementedException();
        }
        public void RenameColumn(TableConf t, string dbName, string schema, string table,
            string columnName, string newColumnName) {
            throw new NotImplementedException("Still need to implement");
        }
        public void ModifyColumn(TableConf t, string dbName, string schema, string table, string columnName, string dataType) {

            throw new NotImplementedException("Still need to implement");
        }
        public void AddColumn(TableConf t, string dbName, string schema, string table, string columnName, string dataType) {
            throw new NotImplementedException("Still need to implement");

        }
        public void DropColumn(TableConf t, string dbName, string schema, string table, string columnName) {
            throw new NotImplementedException("Still need to implement");
        }


        #region unimplemented



        public DataRow GetLastCTBatch(string dbName, AgentType agentType, string slaveIdentifier = "") {
            throw new NotImplementedException("Netezza is only supported as a slave!");
        }


        public IEnumerable<string> GetIntersectColumnList(string dbName, string table1, string schema1, string table2, string schema2) {
            throw new NotImplementedException("Not sure if we need this yet!");
        }

        private bool CheckColumnExists(string dbName, string schema, string table, string column) {
            throw new NotImplementedException("Still need to implement");
        }

        public DataTable GetPendingCTVersions(string dbName, Int64 CTID, int syncBitWise) {
            throw new NotImplementedException("Netezza is only supported as a slave!");
        }

        public DataTable GetPendingCTSlaveVersions(string dbName) {
            throw new NotImplementedException("Netezza is only supported as a slave!");
        }


        public DateTime GetLastStartTime(string dbName, Int64 CTID, int syncBitWise, AgentType type) {
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


        public bool HasPrimaryKey(string dbName, string tableName, string schema) {
            throw new NotImplementedException("Netezza is only supported as a slave!");
        }


        public void WriteBitWise(string dbName, Int64 CTID, int value, AgentType agentType) {
            throw new NotImplementedException("Netezza is only supported as a slave!");
        }


        public int ReadBitWise(string dbName, Int64 CTID, AgentType agentType) {
            throw new NotImplementedException("Netezza is only supported as a slave!");
        }


        public void MarkBatchComplete(string dbName, Int64 CTID, DateTime syncStopTime, string slaveIdentifier) {
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
            throw new NotImplementedException("Netezza is only supported as a slave!");
        }


        public void CreateTableInfoTable(string p, long p_2) {
            throw new NotImplementedException("Netezza is only supported as a slave!");
        }

        public void PublishTableInfo(string dbName, TableConf t, long CTID, long expectedRows) {
            throw new NotImplementedException("Netezza is only supported as a slave!");
        }


        public void CreateConsolidatedTable(string tableName, long CTID, string schemaName, string dbName) {
            throw new NotImplementedException();
        }

        public void Consolidate(string tableName, long CTID, string dbName, string schemaName) {
            throw new NotImplementedException();
        }

        public void RemoveDuplicatePrimaryKeyChangeRows(TableConf table, string consolidatedTableName, string dbName) {
            throw new NotImplementedException("Netezza is only supported as a slave!");
        }


        public void CreateConsolidatedTable(string originalName, string schemaName, string dbName, string consolidatedName) {
            throw new NotImplementedException("Netezza is only supported as a slave!");
        }

        public void Consolidate(string ctTableName, string consolidatedTableName, string dbName, string schemaName) {
            throw new NotImplementedException("Netezza is only supported as a slave!");
        }


        public void ApplyTableChanges(TableConf table, TableConf archiveTable, string dbName, long ctid, string CTDBName) {
            throw new NotImplementedException();
        }


        public void CreateHistoryTable(ChangeTable t, string slaveCTDB) {
            throw new NotImplementedException();
        }


        public ChangeTrackingBatch GetCTBatch(string dbName, long ctid) {
            throw new NotImplementedException();
        }

        public void RevertCTBatch(string dbName, long ctid) {
            throw new NotImplementedException();
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
        #endregion
    }
}

