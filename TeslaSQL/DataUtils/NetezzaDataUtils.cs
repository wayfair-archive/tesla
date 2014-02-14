using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using System.Text;
using System.Data.SqlClient;
using System.Data;
using System.Data.OleDb;
namespace TeslaSQL.DataUtils {
    public class NetezzaDataUtils : IDataUtils {

        public Logger logger;
        public TServer server;

        public NetezzaDataUtils(Logger logger, TServer server) {
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
        internal DataTable SqlQuery(string dbName, OleDbCommand cmd, int? timeout = null) {
            int commandTimeout = timeout ?? Config.QueryTimeout;
            string connStr = buildConnString(dbName);

            using (OleDbConnection conn = new OleDbConnection(connStr)) {
                conn.Open();

                cmd.Connection = conn;
                cmd.CommandTimeout = commandTimeout;

                LogCommand(cmd);
                DataSet ds = new DataSet();
                OleDbDataAdapter da = new OleDbDataAdapter(cmd);
                //this is where the query is run
                da.Fill(ds);

                //return the result, which is the first DataTable in the DataSet
                return ds.Tables[0];
            }
        }

        /// <summary>
        /// Runs a query that does not return results (i.e. a write operation)
        /// </summary>
        /// <param name="dbName">Database name</param>
        /// <param name="cmd">SqlCommand to run</param>
        /// <param name="timeout">Timeout (higher than selects since some writes can be large)</param>
        /// <returns>The number of rows affected</returns>
        internal int SqlNonQuery(string dbName, OleDbCommand cmd, int? timeout = null) {
            int commandTimeout = timeout ?? Config.QueryTimeout;
            //build connection string based on server/db info passed in
            string connStr = buildConnString(dbName);
            int numrows;
            //using block to avoid resource leaks
            using (OleDbConnection conn = new OleDbConnection(connStr)) {
                //open database connection
                conn.Open();
                cmd.Connection = conn;
                cmd.CommandTimeout = commandTimeout;
                LogCommand(cmd);
                numrows = cmd.ExecuteNonQuery();
            }
            return numrows;
        }

        private void LogCommand(OleDbCommand cmd) {
            string query = cmd.CommandText;

            foreach (OleDbParameter p in cmd.Parameters) {
                query = query.Replace(p.ParameterName, "'" + p.Value.ToString() + "'");
            }

            logger.Log("Executing query: " + query, LogLevel.Debug);
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
                    sqlhost = Config.Slave;
                    sqluser = Config.SlaveUser;
                    sqlpass = (new cTripleDes().Decrypt(Config.SlavePassword));
                    break;
                default:
                    throw new NotImplementedException("Netezza is only supported as a slave!");
            }
            return "Data Source=" + sqlhost + "; Initial Catalog=" + database + ";User ID=" + sqluser + ";Password=" + sqlpass + ";Provider=NZOLEDB;Connect Timeout=60;";
        }

        public List<TColumn> GetFieldList(string dbName, string table, string schema) {
            var cols = new List<TColumn>();
            using (var con = new OleDbConnection(buildConnString(dbName))) {
                con.Open();
                //this dark magic is (sort of) documented here 
                //http://msdn.microsoft.com/en-us/library/system.data.oledb.oledbschemaguid.tables.aspx
                var t = con.GetOleDbSchemaTable(OleDbSchemaGuid.Columns, new object[] { null, null, table, null });
                foreach (DataRow row in t.Rows) {
                    cols.Add(new TColumn(row.Field<string>("COLUMN_NAME"), false, null, true));
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

        public RowCounts ApplyTableChanges(TableConf table, TableConf archiveTable, string dbName, long CTID, string CTDBName, bool isConsolidated) {
            var cmds = new List<InsertDelete>();
            cmds.Add(BuildApplyCommand(table, dbName, CTDBName, CTID));
            if (archiveTable != null) {
                cmds.Add(BuildApplyCommand(archiveTable, dbName, CTDBName, CTID));
            }
            var connStr = buildConnString(dbName);
            var rowCounts = new RowCounts(0, 0);
            using (var conn = new OleDbConnection(connStr)) {
                conn.Open();
                var trans = conn.BeginTransaction();
                foreach (var id in cmds) {
                    id.delete.Transaction = trans;
                    id.delete.Connection = conn;
                    id.delete.CommandTimeout = Config.QueryTimeout;
                    logger.Log(id.delete.CommandText, LogLevel.Trace);
                    int deleted = id.delete.ExecuteNonQuery();
                    logger.Log(new { Table = table.Name, message = "Rows deleted: " + deleted }, LogLevel.Info);
                    id.insert.Transaction = trans;
                    id.insert.Connection = conn;
                    id.insert.CommandTimeout = Config.QueryTimeout;
                    logger.Log(id.insert.CommandText, LogLevel.Trace);
                    int inserted = id.insert.ExecuteNonQuery();
                    logger.Log(new { Table = table.Name, message = "Rows inserted: " + inserted }, LogLevel.Info);
                    rowCounts = new RowCounts(rowCounts.Inserted + inserted, rowCounts.Deleted + deleted);
                }
                trans.Commit();
            }
            return rowCounts;
        }

        class InsertDelete {
            public readonly OleDbCommand insert;
            public readonly OleDbCommand delete;
            public InsertDelete(OleDbCommand insert, OleDbCommand delete) {
                this.insert = insert;
                this.delete = delete;
            }
        }
        private InsertDelete BuildApplyCommand(TableConf table, string dbName, string CTDBName, long CTID) {
            string delete = string.Format(@"DELETE FROM {0} P
                                          WHERE EXISTS (SELECT 1 FROM {1}..{2} CT WHERE {3});",
                                          table.Name, CTDBName, table.ToCTName(CTID), table.PkList);

            string insert = string.Format(@"INSERT INTO {0} ({1}) 
                              SELECT {1} FROM {2}..{3} CT WHERE NOT EXISTS (SELECT 1 FROM {0} P WHERE {4}) AND CT.sys_change_operation IN ( 'I', 'U' );",
                                          table.Name, table.NetezzaColumnList, CTDBName, table.ToCTName(CTID), table.PkList);
            var deleteCmd = new OleDbCommand(delete);
            var insertCmd = new OleDbCommand(insert);
            return new InsertDelete(insertCmd, deleteCmd);
        }

        public void CopyIntoHistoryTable(ChangeTable t, string dbName, bool isConsolidated) {
            string sql;
            var fields = GetFieldList(dbName, t.ctName, t.schemaName);
            string insertColumns = "CTHistID, " + string.Join(",", fields.Select(col => col.name));
            string selectColumns = "CAST(" + t.CTID + " AS BIGINT) AS CTHistID, " + string.Join(",", fields.Select(col => col.name));

            if (CheckTableExists(dbName, t.historyName, t.schemaName)) {
                logger.Log("table " + t.historyName + " already exists; selecting into it", LogLevel.Trace);
                sql = string.Format("INSERT INTO {0} ({1}) SELECT {2} FROM {3}", t.historyName, insertColumns, selectColumns, t.ctName);
                logger.Log(sql, LogLevel.Debug);
            } else {
                logger.Log("table " + t.historyName + " does not exist, inserting into it", LogLevel.Trace);
                sql = string.Format("CREATE TABLE {0} AS SELECT {1} FROM {2}", t.historyName, selectColumns, t.ctName);
                logger.Log(sql, LogLevel.Debug);
            }
            var cmd = new OleDbCommand(sql);
            SqlNonQuery(dbName, cmd);
        }

        public void RenameColumn(TableConf t, string dbName, string columnName, string newColumnName, string historyDB) {
            logger.Log("Unable to apply rename of column " + columnName + " to " + newColumnName + " on " 
                + dbName + "." + t.FullName + " for slave " + Config.Slave, LogLevel.Error);
        }

        public void ModifyColumn(TableConf t, string dbName, string columnName, DataType dataType, string historyDB) {
            logger.Log("Unable to apply modify of column " + columnName + " to type " + dataType.ToString() + " on "
                + dbName + "." + t.FullName + " for slave " + Config.Slave, LogLevel.Error);
        }

        public void AddColumn(TableConf t, string dbName, string columnName, DataType dataType, string historyDB) {
            // NOTE: We changed the signature of this method to use "DataType dataType" instead of "string dataType"
            // but currently, we are still using the generic DataType.MapDataType(), passing in dataType.ToString().
            // The generic DataType.MapDataType() method is likely to have problem mapping source column data types to
            // destination (in this case, Netezza) column data types.  We should implement a MapColumnTypeName() method
            // within this DataUtil class to handle the specific column data type mapping from different sources to Netezza.
            // Check out VerticaDataUtil for an example.
            columnName = MapReservedWord(columnName);
            if (!CheckColumnExists(dbName, t.SchemaName, t.Name, columnName)) {
                //The "max" string doesn't exist on netezza, we can just replace it with the NetezzaStringLength after mapping it.
                //In practice this only impacts varchar and nvarchar, since other data types would be mapped to something else by the MapDataType
                //function (i.e. varbinary). This is the only place we do this special string-based handling because we wanted to keep Netezza specific logic
                //out of the DataType class. Outside of this case, the "max" is handled appropriately in the netezza data copy class.
                string destDataType = DataType.MapDataType(Config.RelayType, SqlFlavor.Netezza, dataType.ToString()).Replace("max", Config.NetezzaStringLength.ToString());
                string sql = string.Format("ALTER TABLE {0} ADD {1} {2}; GROOM TABLE {0} VERSIONS;", t.Name, columnName, destDataType);
                var cmd = new OleDbCommand(sql);
                SqlNonQuery(dbName, cmd);
                RefreshViews(dbName, t.Name);
            }
            if (t.RecordHistoryTable && CheckTableExists(historyDB, t.HistoryName, t.SchemaName) && !CheckColumnExists(historyDB, t.SchemaName, t.HistoryName, columnName)) {
                string sql = string.Format("ALTER TABLE {0} ADD {1} {2}; GROOM TABLE {0} VERSIONS;", t.HistoryName, columnName, dataType.ToString());
                var cmd = new OleDbCommand(sql);
                logger.Log("Altering history table column with command: " + cmd.CommandText, LogLevel.Debug);
                SqlNonQuery(historyDB, cmd);
                RefreshViews(historyDB, t.HistoryName);
            }
        }

        public void DropColumn(TableConf t, string dbName, string columnName, string historyDB) {
            columnName = MapReservedWord(columnName);
            if (CheckColumnExists(dbName, t.SchemaName, t.Name, columnName)) {
                string sql = string.Format("ALTER TABLE {0} DROP COLUMN {1} RESTRICT; GROOM TABLE {0} VERSIONS;", t.Name, columnName);
                var cmd = new OleDbCommand(sql);
                SqlNonQuery(dbName, cmd);
                RefreshViews(dbName, t.Name);
            }

            if (t.RecordHistoryTable && CheckTableExists(dbName, t.HistoryName, t.SchemaName) && CheckColumnExists(dbName, t.SchemaName, t.HistoryName, columnName)) {
                string sql = string.Format("ALTER TABLE {0} DROP COLUMN {1} RESTRICT; GROOM TABLE {0} VERSIONS;", t.HistoryName, columnName);
                var cmd = new OleDbCommand(sql);
                logger.Log("Altering history table column with command: " + cmd.CommandText, LogLevel.Debug);
                SqlNonQuery(historyDB, cmd);
                RefreshViews(historyDB, t.HistoryName);
            }
        }

        private void RefreshViews(string dbName, string tableName) {
            var refresh = Config.RefreshViews.Where(r => r.Db.ToLower() == dbName.ToLower() && r.TableName.ToLower() == tableName.ToLower()).FirstOrDefault();
            if (refresh == null) {
                return;
            }
            string sql = refresh.Command;
            var cmd = new OleDbCommand(sql);
            try {
                SqlNonQuery(refresh.Db, cmd);
            } catch (Exception) {
                throw new Exception("Please check any pending schema changes to be applied on Netezza before refreshing the view::" + dbName + ".." + refresh.ViewName);
            }

        }

        public bool CheckColumnExists(string dbName, string schema, string table, string column) {
            string sql = string.Format("SELECT 1 FROM _v_relation_column_def WHERE LOWER(name) = LOWER('{0}') AND type='TABLE' AND LOWER(attname) = LOWER('{1}')",
                                       table, column);
            var cmd = new OleDbCommand(sql);
            var res = SqlQuery(dbName, cmd);
            return res.Rows.Count > 0;
        }

        public static string MapReservedWord(string col) {
            var NetezzaReservedWords = new Dictionary<string, string>() {
                 {"CTID", "CT_ID"},
                 {"OID", "O_ID"},
                 {"XMIN", "X_MIN"},
                 {"CMIN", "C_MIN"},
                 {"XMAX", "X_MAX"},
                 {"CMAX", "C_MAX"},
                 {"TABLEOID", "TABLE_O_ID"}
                 };
            if (NetezzaReservedWords.ContainsKey(col.ToUpper())) {
                return NetezzaReservedWords[col.ToUpper()];
            } else {
                return col;
            }
        }

        public IEnumerable<TTable> GetTables(string dbName) {
            var tables = new List<TTable>();
            using (var con = new OleDbConnection(buildConnString(dbName))) {
                con.Open();
                //this dark magic is (sort of) documented here 
                //http://msdn.microsoft.com/en-us/library/system.data.oledb.oledbschemaguid.tables.aspx
                var t = con.GetOleDbSchemaTable(OleDbSchemaGuid.Tables, new object[] { null, null, null, "TABLE" });
                foreach (DataRow row in t.Rows) {
                    string tableName = row.Field<string>("TABLE_NAME");
                    string schema = row.Field<string>("TABLE_SCHEMA");
                    tables.Add(new TTable(tableName, schema));
                }
            }
            return tables;
        }

        #region unimplemented

        public IEnumerable<long> GetOldCTIDsSlave(string dbName, DateTime chopDate, string slaveIdentifier) {
            throw new NotImplementedException();
        }

        public DataRow GetLastCTBatch(string dbName, AgentType agentType, string slaveIdentifier = "") {
            throw new NotImplementedException("Netezza is only supported as a slave!");
        }


        public IEnumerable<string> GetIntersectColumnList(string dbName, string table1, string schema1, string table2, string schema2) {
            throw new NotImplementedException("Not sure if we need this yet!");
        }


        public DataTable GetPendingCTVersions(string dbName, Int64 CTID, int syncBitWise) {
            throw new NotImplementedException("Netezza is only supported as a slave!");
        }

        public DataTable GetPendingCTSlaveVersions(string dbName, string slaveIdentifier, int bitwise) {
            throw new NotImplementedException("Netezza is only supported as a slave!");
        }


        public DateTime GetLastStartTime(string dbName, Int64 CTID, int syncBitWise, AgentType type, string slaveIdentifier = null) {
            throw new NotImplementedException("Netezza is only supported as a slave!");
        }


        public Int64 GetCurrentCTVersion(string dbName) {
            throw new NotImplementedException("Netezza is only supported as a slave!");
        }


        public Int64 GetMinValidVersion(string dbName, string table, string schema) {
            throw new NotImplementedException("Netezza is only supported as a slave!");
        }




        public ChangeTrackingBatch CreateCTVersion(string dbName, Int64 syncStartVersion, Int64 syncStopVersion) {
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

        public void WriteBitWise(string dbName, Int64 CTID, int value, AgentType agentType) {
            throw new NotImplementedException("Netezza is only supported as a slave!");
        }


        public int ReadBitWise(string dbName, Int64 CTID, AgentType agentType) {
            throw new NotImplementedException("Netezza is only supported as a slave!");
        }


        public void MarkBatchComplete(string dbName, Int64 CTID, DateTime syncStopTime, string slaveIdentifier) {
            throw new NotImplementedException("Netezza is only supported as a slave!");
        }


        public void MarkBatchesComplete(string dbName, IEnumerable<long> CTIDs, DateTime syncStopTime, string slaveIdentifier) {
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

        public void LogError(string message, string headers) {
            throw new NotImplementedException("Netezza is only supported as a slave!");
        }

        public IEnumerable<TError> GetUnsentErrors() {
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

        public void CreateHistoryTable(ChangeTable t, string slaveCTDB) {
            throw new NotImplementedException();
        }

        public ChangeTrackingBatch GetCTBatch(string dbName, long CTID) {
            throw new NotImplementedException();
        }

        public void RevertCTBatch(string dbName, long CTID) {
            throw new NotImplementedException();
        }

        public void MergeCTTable(TableConf table, string destDB, string sourceDB, long CTID) {
            throw new NotImplementedException();
        }


        public void CreateShardCTVersion(string db, long CTID, long startVersion) {
            throw new NotImplementedException();
        }


        public IEnumerable<string> GetPrimaryKeysFromInfoTable(TableConf table, long CTID, string database) {
            throw new NotImplementedException();
        }

        public int GetExpectedRowCounts(string ctDbName, long CTID) {
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


        public int SelectIntoCTTable(string sourceCTDB, TableConf table, string sourceDB, ChangeTrackingBatch batch, int timeout, long? startVersionOverride) {
            throw new NotImplementedException("Netezza is only supported as a slave!");
        }

        public bool IsBeingInitialized(string sourceCTDB, TableConf table) {
            throw new NotImplementedException();
        }

        public long? GetInitializeStartVersion(string sourceCTDB, TableConf table) {
            throw new NotImplementedException();
        }


        public void CleanUpInitializeTable(string dbName, DateTime syncStartTime) {
            throw new NotImplementedException();
        }

        public DataTable GetTablesWithChanges(string dbName, IList<ChangeTrackingBatch> batches) {
            throw new NotImplementedException();
        }
        #endregion


        public Dictionary<TableConf, IList<TColumn>> GetAllFields(string dbName, Dictionary<TableConf, string> tableConfCTTableName) {
            throw new NotImplementedException();
        }

        public Dictionary<TableConf, IList<string>> GetAllPrimaryKeys(string dbName, IEnumerable<TableConf> tables, ChangeTrackingBatch batch) {
            throw new NotImplementedException();
        }


        public Dictionary<TableConf, IEnumerable<string>> GetAllPrimaryKeysMaster(string database, IEnumerable<TableConf> tableConfss) {
            throw new NotImplementedException();
        }

        public void MergeInfoTable(string shardDB, string consolidatedDB, long CTID) {
            throw new NotImplementedException();
        }
    }
}

