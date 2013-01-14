using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using System.Text;
using System.Data.SqlClient;
using System.Data;
using Microsoft.SqlServer.Management.Smo;
using Microsoft.SqlServer.Management.Common;
using TeslaSQL.Agents;
using System.Diagnostics;

namespace TeslaSQL.DataUtils {
    public class MSSQLDataUtils : IDataUtils {

        public Logger logger;
        public TServer server;

        public MSSQLDataUtils(Logger logger, TServer server) {
            this.logger = logger;
            this.server = server;
        }

        /// <summary>
        /// Runs a sql query and returns results as a datatable
        /// </summary>
        /// <param name="dbName">Database name</param>
        /// <param name="cmd">SqlCommand to run</param>
        /// <param name="timeout">Query timeout</param>
        /// <returns>DataTable object representing the result</returns>
        private DataTable SqlQuery(string dbName, SqlCommand cmd, int? timeout = null) {
            int commandTimeout = timeout ?? Config.queryTimeout;
            foreach (IDataParameter p in cmd.Parameters) {
                if (p.Value == null)
                    p.Value = DBNull.Value;
            }
            //build connection string based on server/db info passed in
            string connStr = buildConnString(dbName);

            //using block to avoid resource leaks
            using (SqlConnection conn = new SqlConnection(connStr)) {
                //open database connection
                conn.Open();
                cmd.Connection = conn;
                cmd.CommandTimeout = commandTimeout;
                LogCommand(cmd);
                DataSet ds = new DataSet();
                SqlDataAdapter da = new SqlDataAdapter(cmd);
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
        private T SqlQueryToScalar<T>(string dbName, SqlCommand cmd, int? timeout = null) {
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
        internal int SqlNonQuery(string dbName, SqlCommand cmd, int? timeout = null) {
            int commandTimeout = timeout ?? Config.queryTimeout;
            foreach (IDataParameter p in cmd.Parameters) {
                if (p.Value == null)
                    p.Value = DBNull.Value;
            }
            //build connection string based on server/db info passed in
            string connStr = buildConnString(dbName);
            int numrows;
            //using block to avoid resource leaks
            using (SqlConnection conn = new SqlConnection(connStr)) {
                conn.Open();
                cmd.Connection = conn;
                cmd.CommandTimeout = commandTimeout;
                LogCommand(cmd);
                numrows = cmd.ExecuteNonQuery();
            }
            return numrows;
        }


        /// <summary>
        /// Log the command to be run
        /// </summary>
        /// <param name="cmd">SqlCommand to be run</param>
        private void LogCommand(SqlCommand cmd) {
            string query = cmd.CommandText;

            foreach (SqlParameter p in cmd.Parameters) {
                query = query.Replace(p.ParameterName, "'" + p.Value.ToString() + "'");
            }

            logger.Log("Executing query: " + query, LogLevel.Debug);
        }


        /// <summary>
        /// Builds a connection string for the passed in database name
        /// </summary>
        /// <param name="database">Database name</param>
        /// <returns>An ADO.NET connection string</returns>
        private string buildConnString(string database) {
            string sqlhost = "";
            string sqluser = "";
            string sqlpass = "";

            switch (server) {
                case TServer.MASTER:
                    sqlhost = Config.master;
                    sqluser = Config.masterUser;
                    sqlpass = (new cTripleDes().Decrypt(Config.masterPassword));
                    break;
                case TServer.SLAVE:
                    sqlhost = Config.slave;
                    sqluser = Config.slaveUser;
                    sqlpass = (new cTripleDes().Decrypt(Config.slavePassword));
                    break;
                case TServer.RELAY:
                    sqlhost = Config.relayServer;
                    sqluser = Config.relayUser;
                    sqlpass = (new cTripleDes().Decrypt(Config.relayPassword));
                    break;
            }

            return "Data Source=" + sqlhost + "; Initial Catalog=" + database + ";User ID=" + sqluser + ";Password=" + sqlpass + ";Connection Timeout=60;";
        }


        public DataRow GetLastCTBatch(string dbName, AgentType agentType, string slaveIdentifier = "") {
            SqlCommand cmd;
            //for slave we have to pass the slave identifier in and use tblCTSlaveVersion
            if (agentType.Equals(AgentType.Slave)) {
                cmd = new SqlCommand("SELECT TOP 1 CTID, syncStartVersion, syncStopVersion, syncBitWise, syncStartTime" +
                    " FROM dbo.tblCTSlaveVersion WITH(NOLOCK) WHERE slaveIdentifier = @slave ORDER BY CTID DESC");
                cmd.Parameters.Add("@slave", SqlDbType.VarChar, 100).Value = slaveIdentifier;
            } else {
                cmd = new SqlCommand("SELECT TOP 1 CTID, syncStartVersion, syncStopVersion, syncBitWise, syncStartTime FROM dbo.tblCTVersion ORDER BY CTID DESC");
            }

            DataTable result = SqlQuery(dbName, cmd);
            return result.Rows.Count > 0 ? result.Rows[0] : null;
        }


        public DataTable GetPendingCTVersions(string dbName, Int64 CTID, int syncBitWise) {
            string query = ("SELECT CTID, syncStartVersion, syncStopVersion, syncStartTime, syncBitWise" +
                " FROM dbo.tblCTVersion WITH(NOLOCK) WHERE CTID > @ctid AND syncBitWise & @syncbitwise > 0" +
                " ORDER BY CTID ASC");
            SqlCommand cmd = new SqlCommand(query);
            cmd.Parameters.Add("@ctid", SqlDbType.BigInt).Value = CTID;
            cmd.Parameters.Add("@syncbitwise", SqlDbType.Int).Value = syncBitWise;

            //get query results as a datatable since there can be multiple rows
            return SqlQuery(dbName, cmd);
        }


        public DateTime GetLastStartTime(string dbName, Int64 CTID, int syncBitWise, AgentType type) {
            string tableName = type == AgentType.Slave ? "tblCTSlaveVersion" : "tblCTVersion";
            SqlCommand cmd = new SqlCommand(
                "select MAX(syncStartTime) as maxStart FROM dbo." + tableName + " WITH(NOLOCK)"
                + " WHERE syncBitWise & @syncbitwise > 0 AND CTID < @CTID");
            cmd.Parameters.Add("@syncbitwise", SqlDbType.Int).Value = syncBitWise;
            cmd.Parameters.Add("@CTID", SqlDbType.BigInt).Value = CTID;
            DateTime? lastStartTime = SqlQueryToScalar<DateTime?>(dbName, cmd);
            if (lastStartTime == null) {
                return DateTime.Now.AddDays(-1);
            }
            return (DateTime)lastStartTime;
        }


        public Int64 GetCurrentCTVersion(string dbName) {
            SqlCommand cmd = new SqlCommand("SELECT CHANGE_TRACKING_CURRENT_VERSION();");
            return SqlQueryToScalar<Int64>(dbName, cmd);
        }


        public Int64 GetMinValidVersion(string dbName, string table, string schema) {
            SqlCommand cmd = new SqlCommand("SELECT CHANGE_TRACKING_MIN_VALID_VERSION(OBJECT_ID(@tablename))");
            cmd.Parameters.Add("@tablename", SqlDbType.VarChar, 500).Value = schema + "." + table;
            return SqlQueryToScalar<Int64>(dbName, cmd);
        }


        public int SelectIntoCTTable(string sourceCTDB, TableConf table, string sourceDB, ChangeTrackingBatch ctb, int queryTimeout, long? overrideStartVersion = null) {
            /*
             * There is no way to have column lists or table names be parametrized/dynamic in sqlcommands other than building the string
             * manually like this. However, the table name and column list fields are trustworthy because they have already been compared to
             * actual database objects at this point. The database names are also validated to be legal database identifiers.
             * Only the start and stop versions are actually parametrizable.
             */
            string query = "SELECT " + table.masterColumnList + ", CT.SYS_CHANGE_VERSION, CT.SYS_CHANGE_OPERATION ";
            query += " INTO " + table.schemaName + "." + table.ToCTName(ctb.CTID);
            query += " FROM CHANGETABLE(CHANGES " + sourceDB + "." + table.schemaName + "." + table.name + ", @startversion) CT";
            query += " LEFT OUTER JOIN " + sourceDB + "." + table.schemaName + "." + table.name + " P ON " + table.pkList;
            query += " WHERE (SYS_CHANGE_VERSION <= @stopversion OR SYS_CHANGE_CREATION_VERSION <= @stopversion)";
            query += " AND (SYS_CHANGE_OPERATION = 'D' OR " + table.notNullPKList + ")";

            SqlCommand cmd = new SqlCommand(query);

            cmd.Parameters.Add("@startversion", SqlDbType.BigInt).Value = (overrideStartVersion.HasValue ? overrideStartVersion.Value : ctb.syncStartVersion);
            cmd.Parameters.Add("@stopversion", SqlDbType.BigInt).Value = ctb.syncStopVersion;

            return SqlNonQuery(sourceCTDB, cmd, 1200);
        }


        public Int64 CreateCTVersion(string dbName, Int64 syncStartVersion, Int64 syncStopVersion) {
            //create new row in tblCTVersion, output the CTID
            string query = "INSERT INTO dbo.tblCTVersion (syncStartVersion, syncStopVersion, syncStartTime, syncBitWise)";
            query += " OUTPUT inserted.CTID";
            query += " VALUES (@startVersion, @stopVersion, GETDATE(), 0)";

            SqlCommand cmd = new SqlCommand(query);

            cmd.Parameters.Add("@startVersion", SqlDbType.BigInt).Value = syncStartVersion;
            cmd.Parameters.Add("@stopVersion", SqlDbType.BigInt).Value = syncStopVersion;

            return SqlQueryToScalar<Int64>(dbName, cmd);
        }

        public void CreateShardCTVersion(string dbName, long ctid, long startVersion) {
            string query = "INSERT INTO dbo.tblCTVersion (ctid, syncStartVersion, syncStartTime, syncBitWise)";
            query += " VALUES (@ctid,@syncStartVersion, GETDATE(), 0)";

            SqlCommand cmd = new SqlCommand(query);

            cmd.Parameters.Add("@ctid", SqlDbType.BigInt).Value = ctid;
            cmd.Parameters.Add("@syncStartVersion", SqlDbType.BigInt).Value = startVersion;

            SqlNonQuery(dbName, cmd);
        }


        public void CreateSlaveCTVersion(string dbName, ChangeTrackingBatch ctb, string slaveIdentifier) {

            string query = "INSERT INTO dbo.tblCTSlaveVersion (CTID, slaveIdentifier, syncStartVersion, syncStopVersion, syncStartTime, syncBitWise)";
            query += " VALUES (@ctid, @slaveidentifier, @startversion, @stopversion, @starttime, @syncbitwise)";

            SqlCommand cmd = new SqlCommand(query);

            cmd.Parameters.Add("@ctid", SqlDbType.BigInt).Value = ctb.CTID;
            cmd.Parameters.Add("@slaveidentifier", SqlDbType.VarChar, 100).Value = slaveIdentifier;
            cmd.Parameters.Add("@startversion", SqlDbType.BigInt).Value = ctb.syncStartVersion;
            cmd.Parameters.Add("@stopversion", SqlDbType.BigInt).Value = ctb.syncStopVersion;
            cmd.Parameters.Add("@starttime", SqlDbType.DateTime).Value = ctb.syncStartTime;
            cmd.Parameters.Add("@syncbitwise", SqlDbType.Int).Value = ctb.syncBitWise;

            int result = SqlNonQuery(dbName, cmd, 30);
        }


        public void CreateSchemaChangeTable(string dbName, Int64 CTID) {
            //drop the table on the relay server if it exists
            bool tExisted = DropTableIfExists(dbName, "tblCTSchemaChange_" + Convert.ToString(CTID), "dbo");

            //can't parametrize the CTID since it's part of a table name, but it is an Int64 so it's not an injection risk
            string query = "CREATE TABLE [dbo].[tblCTSchemaChange_" + CTID + "] (";
            query += @"
            [CscID] [int] NOT NULL IDENTITY(1,1) PRIMARY KEY,
	        [CscDdeID] [int] NOT NULL,
	        [CscTableName] [varchar](500) NOT NULL,
            [CscEventType] [varchar](50) NOT NULL,
            [CscSchema] [varchar](100) NOT NULL,
            [CscColumnName] [varchar](500) NOT NULL,
            [CscNewColumnName] [varchar](500) NULL,
            [CscBaseDataType] [varchar](100) NULL,
            [CscCharacterMaximumLength] [int] NULL,
            [CscNumericPrecision] [int] NULL,
            [CscNumericScale] [int] NULL
            )";

            SqlCommand cmd = new SqlCommand(query);

            int result = SqlNonQuery(dbName, cmd);
        }


        public DataTable GetDDLEvents(string dbName, DateTime afterDate) {
            if (!CheckTableExists(dbName, "tblDDLEvent")) {
                throw new Exception("tblDDLEvent does not exist on the source database, unable to check for schema changes. Please create the table and the trigger that populates it!");
            }

            string query = "SELECT DdeID, DdeEventData FROM dbo.tblDDLEvent WHERE DdeTime > @afterdate";

            SqlCommand cmd = new SqlCommand(query);
            cmd.Parameters.Add("@afterdate", SqlDbType.DateTime).Value = afterDate;

            return SqlQuery(dbName, cmd);
        }


        public void WriteSchemaChange(string dbName, Int64 CTID, SchemaChange schemaChange) {
            string query = "INSERT INTO dbo.tblCTSchemaChange_" + Convert.ToString(CTID) +
                " (CscDdeID, CscTableName, CscEventType, CscSchema, CscColumnName, CscNewColumnName, " +
                " CscBaseDataType, CscCharacterMaximumLength, CscNumericPrecision, CscNumericScale) " +
                " VALUES (@ddeid, @tablename, @eventtype, @schema, @columnname, @newcolumnname, " +
                " @basedatatype, @charactermaximumlength, @numericprecision, @numericscale)";

            var cmd = new SqlCommand(query);
            cmd.Parameters.Add("@ddeid", SqlDbType.Int).Value = schemaChange.ddeID;
            cmd.Parameters.Add("@tablename", SqlDbType.VarChar, 500).Value = schemaChange.tableName;
            cmd.Parameters.Add("@eventtype", SqlDbType.VarChar, 50).Value = schemaChange.eventType;
            cmd.Parameters.Add("@schema", SqlDbType.VarChar, 100).Value = schemaChange.schemaName;
            cmd.Parameters.Add("@columnname", SqlDbType.VarChar, 500).Value = schemaChange.columnName;
            cmd.Parameters.Add("@newcolumnname", SqlDbType.VarChar, 500).Value = schemaChange.newColumnName;
            if (schemaChange.dataType != null) {
                cmd.Parameters.Add("@basedatatype", SqlDbType.VarChar, 100).Value = schemaChange.dataType.baseType;
                cmd.Parameters.Add("@charactermaximumlength", SqlDbType.Int).Value = schemaChange.dataType.characterMaximumLength;
                cmd.Parameters.Add("@numericprecision", SqlDbType.Int).Value = schemaChange.dataType.numericPrecision;
                cmd.Parameters.Add("@numericscale", SqlDbType.Int).Value = schemaChange.dataType.numericScale;
            } else {
                if (schemaChange.eventType == SchemaChangeType.Add) {
                    throw new Exception("Cannot add a schema change without a valid datatype");
                }
                cmd.Parameters.Add("@basedatatype", SqlDbType.VarChar, 100).Value = DBNull.Value;
                cmd.Parameters.Add("@charactermaximumlength", SqlDbType.Int).Value = DBNull.Value;
                cmd.Parameters.Add("@numericprecision", SqlDbType.Int).Value = DBNull.Value;
                cmd.Parameters.Add("@numericscale", SqlDbType.Int).Value = DBNull.Value;
            }
            foreach (IDataParameter p in cmd.Parameters) {
                if (p.Value == null)
                    p.Value = DBNull.Value;
            }
            int result = SqlNonQuery(dbName, cmd);
        }


        public DataRow GetDataType(string dbName, string table, string schema, string column) {
            var cmd = new SqlCommand("SELECT DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE " +
                "FROM INFORMATION_SCHEMA.COLUMNS WITH(NOLOCK) WHERE TABLE_SCHEMA = @schema AND TABLE_CATALOG = @db " +
                "AND TABLE_NAME = @table AND COLUMN_NAME = @column");
            cmd.Parameters.Add("@db", SqlDbType.VarChar, 500).Value = dbName;
            cmd.Parameters.Add("@table", SqlDbType.VarChar, 500).Value = table;
            cmd.Parameters.Add("@schema", SqlDbType.VarChar, 500).Value = schema;
            cmd.Parameters.Add("@column", SqlDbType.VarChar, 500).Value = column;

            DataTable result = SqlQuery(dbName, cmd);

            if (result == null || result.Rows.Count == 0) {
                throw new DoesNotExistException("Column " + column + " does not exist on table " + table + "!");
            }

            return result.Rows[0];
        }


        public void UpdateSyncStopVersion(string dbName, Int64 syncStopVersion, Int64 CTID) {
            string query = "UPDATE dbo.tblCTVersion set syncStopVersion = @stopversion WHERE CTID = @ctid";
            SqlCommand cmd = new SqlCommand(query);

            cmd.Parameters.Add("@stopversion", SqlDbType.BigInt).Value = syncStopVersion;
            cmd.Parameters.Add("@ctid", SqlDbType.BigInt).Value = CTID;

            int res = SqlNonQuery(dbName, cmd);
        }


        /// <summary>
        /// Retrieves an SMO table object if the table exists, throws exception if not.
        /// </summary>
        /// <param name="dbName">Database name</param>
        /// <param name="table">Table Name</param>
        /// <returns>Smo.Table object representing the table</returns>
        public Table GetSmoTable(string dbName, string table, string schema = "dbo") {
            logger.Log(string.Format("SmoTable: {3}: {0}.{1}.{2}", dbName, schema, table, server), LogLevel.Trace);
            var db = GetSmoDatabase(dbName);
            if (db.Tables.Contains(table)) {
                return db.Tables[table, schema];
            } else {
                throw new DoesNotExistException("Table " + table + " does not exist");
            }
        }

        /// <summary>
        /// Retrieves an SMO table object if the table exists, throws exception if not.
        /// </summary>
        /// <param name="dbName">Database name</param>
        /// <param name="view">Table Name</param>
        /// <returns>Smo.Table object representing the table</returns>
        public View GetSmoView(string dbName, string view, string schema = "dbo") {
            var db = GetSmoDatabase(dbName);
            if (db.Views.Contains(view)) {
                return db.Views[view, schema];
            } else {
                throw new DoesNotExistException("Table " + view + " does not exist");
            }
        }

        public IEnumerable<TTable> GetTables(string dbName) {
            var tables = new List<TTable>();
            var db = GetSmoDatabase(dbName);
            foreach (Table t in db.Tables) {
                tables.Add(new TTable(t.Name, t.Schema));
            }
            return tables;
        }

        private Database GetSmoDatabase(string dbName) {
            using (SqlConnection sqlconn = new SqlConnection(buildConnString(dbName))) {
                ServerConnection serverconn = new ServerConnection(sqlconn);
                Server svr = new Server(serverconn);
                Database db = new Database();
                if (svr.Databases.Contains(dbName) && svr.Databases[dbName].IsAccessible) {
                    db = svr.Databases[dbName];
                    return db;
                } else {
                    throw new Exception("Database " + dbName + " does not exist or is inaccessible");
                }
            }
        }

        public bool CheckTableExists(string dbName, string table, string schema = "dbo") {
            try {
                Table t_smo = GetSmoTable(dbName, table, schema);
                if (table != null) {
                    return true;
                }
                return false;
            } catch (DoesNotExistException) {
                return false;
            }
        }


        public IEnumerable<string> GetIntersectColumnList(string dbName, string tableName1, string schema1, string tableName2, string schema2) {
            Table table1 = GetSmoTable(dbName, tableName1, schema1);
            Table table2 = GetSmoTable(dbName, tableName2, schema2);
            var columns1 = new List<string>();
            var columns2 = new List<string>();
            //create this so that casing changes to columns don't cause problems, just use the lowercase column name
            foreach (Column c in table1.Columns) {
                columns1.Add(c.Name.ToLower());
            }
            foreach (Column c in table2.Columns) {
                columns2.Add(c.Name.ToLower());
            }
            return columns1.Intersect(columns2);
        }


        public bool HasPrimaryKey(string dbName, string tableName, string schema) {
            Table table = GetSmoTable(dbName, tableName, schema);
            foreach (Index i in table.Indexes) {
                if (i.IndexKeyType == IndexKeyType.DriPrimaryKey) {
                    return true;
                }
            }
            return false;
        }


        public bool DropTableIfExists(string dbName, string table, string schema) {
            try {
                Table t_smo = GetSmoTable(dbName, table, schema);
                if (t_smo != null) {
                    t_smo.Drop();
                    return true;
                }
                return false;
            } catch (DoesNotExistException) {
                return false;
            }
        }


        public Dictionary<string, bool> GetFieldList(string dbName, string table, string schema) {
            Dictionary<string, bool> dict = new Dictionary<string, bool>();
            Table t_smo;

            //attempt to get smo table object
            try {
                t_smo = GetSmoTable(dbName, table, schema);
            } catch (DoesNotExistException) {
                logger.Log("Unable to get field list for " + dbName + "." + schema + "." + table + " because it does not exist", LogLevel.Debug);
                return dict;
            }

            //loop through columns and add them to the dictionary along with whether they are part of the primary key
            foreach (Column c in t_smo.Columns) {
                dict.Add(c.Name, c.InPrimaryKey);
            }

            return dict;
        }


        public void WriteBitWise(string dbName, Int64 CTID, int value, AgentType agentType) {
            string query;
            SqlCommand cmd;
            if (agentType.Equals(AgentType.Slave)) {
                query = "UPDATE dbo.tblCTSlaveVersion SET SyncBitWise = SyncBitWise + @syncbitwise";
                query += " WHERE slaveIdentifier = @slaveidentifier AND CTID = @ctid AND SyncBitWise & @syncbitwise = 0";
                cmd = new SqlCommand(query);
                cmd.Parameters.Add("@syncbitwise", SqlDbType.Int).Value = value;
                cmd.Parameters.Add("@slaveidentifier", SqlDbType.VarChar, 100).Value = Config.slave;
                cmd.Parameters.Add("@ctid", SqlDbType.BigInt).Value = CTID;
            } else {
                query = "UPDATE dbo.tblCTVersion SET SyncBitWise = SyncBitWise + @syncbitwise";
                query += " WHERE CTID = @ctid AND SyncBitWise & @syncbitwise = 0";
                cmd = new SqlCommand(query);
                cmd.Parameters.Add("@syncbitwise", SqlDbType.Int).Value = value;
                cmd.Parameters.Add("@ctid", SqlDbType.BigInt).Value = CTID;
            }
            int result = SqlNonQuery(dbName, cmd);
        }


        public int ReadBitWise(string dbName, Int64 CTID, AgentType agentType) {
            string query;
            SqlCommand cmd;
            if (agentType.Equals(AgentType.Slave)) {
                query = "SELECT syncBitWise from dbo.tblCTSlaveVersion WITH(NOLOCK)";
                query += " WHERE slaveIdentifier = @slaveidentifier AND CTID = @ctid";
                cmd = new SqlCommand(query);
                cmd.Parameters.Add("@slaveidentifier", SqlDbType.VarChar, 100).Value = Config.slave;
                cmd.Parameters.Add("@ctid", SqlDbType.BigInt).Value = CTID;
            } else {
                query = "SELECT syncBitWise from dbo.tblCTVersion WITH(NOLOCK)";
                query += " WHERE CTID = @ctid";
                cmd = new SqlCommand(query);
                cmd.Parameters.Add("@ctid", SqlDbType.BigInt).Value = CTID;
            }
            return SqlQueryToScalar<Int32>(dbName, cmd);
        }


        public void MarkBatchComplete(string dbName, Int64 CTID, DateTime syncStopTime, string slaveIdentifier) {
            string query;
            SqlCommand cmd;
            Enum.GetValues(typeof(SyncBitWise)).Cast<int>().Sum();
            query = "UPDATE dbo.tblCTSlaveVersion SET syncBitWise = @syncbitwise, syncStopTime = @syncstoptime";
            query += " WHERE slaveIdentifier = @slaveidentifier AND CTID = @ctid";
            cmd = new SqlCommand(query);
            cmd.Parameters.Add("@syncbitwise", SqlDbType.Int).Value = Enum.GetValues(typeof(SyncBitWise)).Cast<int>().Sum();
            cmd.Parameters.Add("@syncstoptime", SqlDbType.DateTime).Value = syncStopTime;
            cmd.Parameters.Add("@slaveidentifier", SqlDbType.VarChar, 100).Value = slaveIdentifier;
            cmd.Parameters.Add("@ctid", SqlDbType.BigInt).Value = CTID;
            int result = SqlNonQuery(dbName, cmd);
        }


        public DataTable GetSchemaChanges(string dbName, Int64 CTID) {
            SqlCommand cmd = new SqlCommand("SELECT CscID, CscDdeID, CscTableName, CscEventType, CscSchema, CscColumnName" +
             ", CscNewColumnName, CscBaseDataType, CscCharacterMaximumLength, CscNumericPrecision, CscNumericScale" +
             " FROM dbo.tblCTSchemaChange_" + Convert.ToString(CTID));
            return SqlQuery(dbName, cmd);
        }


        public Int64 GetTableRowCount(string dbName, string table, string schema) {
            Table t_smo = GetSmoTable(dbName, table, schema);
            return t_smo.RowCount;
        }

        public bool IsChangeTrackingEnabled(string dbName, string table, string schema) {
            Table t_smo = GetSmoTable(dbName, table, schema);
            return t_smo.ChangeTrackingEnabled;
        }

        public void LogError(string message) {
            SqlCommand cmd = new SqlCommand("INSERT INTO tblCtError (CelError) VALUES ( @error )");
            cmd.Parameters.Add("@error", SqlDbType.VarChar, -1).Value = message;
            SqlNonQuery(Config.errorLogDB, cmd);
        }

        public DataTable GetUnsentErrors() {
            SqlCommand cmd = new SqlCommand("SELECT CelError, CelId FROM tblCtError WHERE CelSent = 0");
            return SqlQuery(Config.errorLogDB, cmd);
        }

        public void MarkErrorsSent(IEnumerable<int> celIds) {
            SqlCommand cmd = new SqlCommand("UPDATE tblCtError SET CelSent = 1 WHERE CelId IN (" + string.Join(",", celIds) + ")");
            SqlNonQuery(Config.errorLogDB, cmd);
        }

        private bool CheckColumnExists(string dbName, string schema, string table, string column) {
            Table t_smo = GetSmoTable(dbName, table, schema);
            if (t_smo.Columns.Contains(column)) {
                return true;
            }
            return false;
        }

        public void RenameColumn(TableConf t, string dbName, string schema, string table,
            string columnName, string newColumnName) {
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
            if (t.recordHistoryTable && CheckTableExists(dbName, schema, table + "_History") && CheckColumnExists(dbName, schema, table + "_History", columnName)) {
                cmd = new SqlCommand("EXEC sp_rename @objname, @newname, 'COLUMN'");
                cmd.Parameters.Add("@objname", SqlDbType.VarChar, 500).Value = schema + "." + table + "_History." + columnName;
                cmd.Parameters.Add("@newname", SqlDbType.VarChar, 500).Value = newColumnName;
                logger.Log("Altering history table column with command: " + cmd.CommandText, LogLevel.Debug);
                SqlNonQuery(dbName, cmd);
            }
        }

        public void ModifyColumn(TableConf t, string dbName, string schema, string table, string columnName, string dataType) {
            SqlCommand cmd;
            //Modify the column if it exists
            if (CheckColumnExists(dbName, schema, table, columnName)) {
                cmd = new SqlCommand("ALTER TABLE " + schema + "." + table + " ALTER COLUMN " + columnName + " " + dataType);
                logger.Log("Altering table column with command: " + cmd.CommandText, LogLevel.Debug);
                SqlNonQuery(dbName, cmd);
            }
            //modify on history table if that exists too
            if (t.recordHistoryTable && CheckTableExists(dbName, schema, table + "_History") && CheckColumnExists(dbName, schema, table + "_History", columnName)) {
                cmd = new SqlCommand("ALTER TABLE " + schema + "." + table + "_History ALTER COLUMN " + columnName + " " + dataType);
                logger.Log("Altering history table column with command: " + cmd.CommandText, LogLevel.Debug);
                SqlNonQuery(dbName, cmd);
            }
        }

        public void AddColumn(TableConf t, string dbName, string schema, string table, string columnName, string dataType) {
            SqlCommand cmd;
            //add column if it doesn't exist
            if (!CheckColumnExists(dbName, schema, table, columnName)) {
                cmd = new SqlCommand("ALTER TABLE " + schema + "." + table + " ADD " + columnName + " " + dataType);
                logger.Log("Altering table with command: " + cmd.CommandText, LogLevel.Debug);
                SqlNonQuery(dbName, cmd);
            }
            //add column to history table if the table exists and the column doesn't
            if (t.recordHistoryTable && CheckTableExists(dbName, schema, table + "_History") && !CheckColumnExists(dbName, schema, table + "_History", columnName)) {
                cmd = new SqlCommand("ALTER TABLE " + schema + "." + table + "_History ADD " + columnName + " " + dataType);
                logger.Log("Altering history table column with command: " + cmd.CommandText, LogLevel.Debug);
                SqlNonQuery(dbName, cmd);
            }
        }

        public void DropColumn(TableConf t, string dbName, string schema, string table, string columnName) {
            SqlCommand cmd;
            //drop column if it exists
            if (CheckColumnExists(dbName, schema, table, columnName)) {
                cmd = new SqlCommand("ALTER TABLE " + schema + "." + table + " DROP COLUMN " + columnName);
                logger.Log("Altering table with command: " + cmd.CommandText, LogLevel.Debug);
                SqlNonQuery(dbName, cmd);
            }
            //if history table exists and column exists, drop it there too
            if (t.recordHistoryTable && CheckTableExists(dbName, schema, table + "_History") && CheckColumnExists(dbName, schema, table + "_History", columnName)) {
                cmd = new SqlCommand("ALTER TABLE " + schema + "." + table + "_History DROP COLUMN " + columnName);
                logger.Log("Altering history table column with command: " + cmd.CommandText, LogLevel.Debug);
                SqlNonQuery(dbName, cmd);
            }
        }

        public void CreateTableInfoTable(string dbName, Int64 CTID) {
            //drop the table on the relay server if it exists
            bool tExisted = DropTableIfExists(dbName, "tblCTTableInfo_" + CTID, "dbo");

            //can't parametrize the CTID since it's part of a table name, but it is an Int64 so it's not an injection risk
            string query = "CREATE TABLE [dbo].[tblCTTableInfo_" + CTID + "] (";
            query += @"
            [CtiID] [int] NOT NULL IDENTITY(1,1) PRIMARY KEY,
	        [CtiTableName] [varchar](500) NOT NULL,
            [CtiSchemaName] [varchar](100) NOT NULL,
            [CtiPKList] [varchar](500) NOT NULL,
            [CtiExpectedRows] [int] NOT NULL,
            )";

            SqlCommand cmd = new SqlCommand(query);

            int result = SqlNonQuery(dbName, cmd);
        }


        public void PublishTableInfo(string dbName, TableConf t, long CTID, long expectedRows) {
            SqlCommand cmd = new SqlCommand(
               String.Format(@"INSERT INTO tblCTTableInfo_{0} (CtiTableName, CtiSchemaName, CtiPKList, CtiExpectedRows)
                  VALUES (@tableName, @schemaName, @pkList, @expectedRows)", CTID));
            cmd.Parameters.Add("@tableName", SqlDbType.VarChar, 500).Value = t.name;
            cmd.Parameters.Add("@schemaName", SqlDbType.VarChar, 500).Value = t.schemaName;
            cmd.Parameters.Add("@pkList", SqlDbType.VarChar, 500).Value = string.Join(",", t.columns.Where(c => c.isPk));
            cmd.Parameters.Add("@expectedRows", SqlDbType.Int).Value = expectedRows;

            SqlNonQuery(dbName, cmd);
        }

        /// <summary>
        /// Runs a sql query and returns a DataReader
        /// </summary>
        /// <param name="dbName">Database name</param>
        /// <param name="cmd">SqlCommand to run</param>
        /// <param name="timeout">Query timeout</param>
        /// <returns>DataReader object representing the result</returns>
        public SqlDataReader ExecuteReader(string dbName, SqlCommand cmd, int timeout = 1200) {
            SqlConnection sourceConn = new SqlConnection(buildConnString(dbName));
            sourceConn.Open();
            cmd.Connection = sourceConn;
            cmd.CommandTimeout = timeout;
            SqlDataReader reader = cmd.ExecuteReader();
            return reader;
        }

        /// <summary>
        /// Writes data from the given stream reader to a destination database
        /// </summary>
        /// <param name="reader">SqlDataReader object to stream input from</param>
        /// <param name="dbName">Database name</param>
        /// <param name="schema">Schema of the table to write to</param>
        /// <param name="table">Table name to write to</param>
        /// <param name="timeout">Timeout</param>
        public void BulkCopy(SqlDataReader reader, string dbName, string schema, string table, int timeout) {
            SqlBulkCopy bulkCopy = new SqlBulkCopy(buildConnString(dbName), SqlBulkCopyOptions.KeepIdentity);
            bulkCopy.BulkCopyTimeout = timeout;
            bulkCopy.DestinationTableName = schema + ".[" + table + "]";
            bulkCopy.NotifyAfter = 1000;
            var sw = Stopwatch.StartNew();
            bulkCopy.SqlRowsCopied += new SqlRowsCopiedEventHandler((s, e) => logger.Log("Copied " + e.RowsCopied + " rows so far, in " + sw.Elapsed, LogLevel.Debug));
            bulkCopy.WriteToServer(reader);
        }

        public RowCounts ApplyTableChanges(TableConf table, TableConf archiveTable, string dbName, Int64 CTID, string CTDBName) {
            var tableSql = new List<SqlCommand>();
            tableSql.Add(BuildMergeQuery(table, dbName, CTID, CTDBName));
            if (archiveTable != null) {
                tableSql.Add(BuildMergeQuery(archiveTable, dbName, CTID, CTDBName));
            }
            var s = TransactionQuery(tableSql, dbName, Config.queryTimeout);
            int inserted = s[0].Rows[0].Field<int>("insertcount");
            int deleted = s[0].Rows[0].Field<int>("deletecount");
            logger.Log("table " + table.name + ": insert: " + inserted + " | delete: " + deleted, LogLevel.Info);
            var rowCounts = new RowCounts(inserted, deleted);
            if (archiveTable != null) {
                inserted = s[1].Rows[0].Field<int>("insertcount");
                deleted = s[1].Rows[0].Field<int>("deletecount");
                logger.Log("table " + archiveTable.name + ": insert: " + inserted + " | delete: " + deleted, LogLevel.Info);
                rowCounts = new RowCounts(rowCounts.Inserted + inserted, rowCounts.Deleted + deleted);
            }
            return rowCounts;
        }

        private SqlCommand BuildMergeQuery(TableConf table, string dbName, Int64 ctid, string CTDBName) {
            string sql = string.Format(
                @"DECLARE @rowcounts TABLE (mergeaction nvarchar(10));
                  DECLARE @insertcount int, @deletecount int;
                  MERGE [{0}].[{7}].[{1}] WITH (ROWLOCK) AS P
                  USING (SELECT * FROM [{8}].{2}) AS CT
                  ON ({3})
                  WHEN MATCHED AND CT.SYS_CHANGE_OPERATION = 'D'
                      THEN DELETE
                  WHEN MATCHED AND CT.SYS_CHANGE_OPERATION IN ('I', 'U')
                      THEN UPDATE SET {4}
                  WHEN NOT MATCHED BY TARGET AND CT.SYS_CHANGE_OPERATION IN ('I', 'U') THEN
                      INSERT ({5}) VALUES ({6})
                  OUTPUT $action INTO @rowcounts;
                  SELECT @insertcount = COUNT(*) FROM @rowcounts WHERE mergeaction IN ('INSERT', 'UPDATE'); 
                  SELECT @deletecount = COUNT(*) FROM @rowcounts WHERE mergeaction IN ('DELETE', 'UPDATE');",
                          dbName,
                          table.name,
                          table.ToFullCTName(ctid),
                          table.pkList,
                          table.mergeUpdateList.Length > 2 ? table.mergeUpdateList : table.pkList.Replace("AND", ","),
                          table.simpleColumnList,
                          table.masterColumnList.Replace("P.", "CT."),
                          table.schemaName,
                          CTDBName
                          );
            string baseSql = sql.Replace("'", "''");
            sql += "\nDELETE @rowcounts;\n";
            sql += "SELECT @insertcount AS insertcount, @deletecount AS deletecount\n";
            return new SqlCommand(sql);
        }

        private IList<DataTable> TransactionQuery(IList<SqlCommand> commands, string dbName, int timeout) {
            var connStr = buildConnString(dbName);
            var tables = new List<DataTable>();
            using (var conn = new SqlConnection(connStr)) {
                conn.Open();
                var trans = conn.BeginTransaction();
                foreach (var cmd in commands) {
                    logger.Log(cmd.CommandText, LogLevel.Trace);
                    cmd.Transaction = trans;
                    cmd.CommandTimeout = timeout;
                    cmd.Connection = conn;
                    DataSet ds = new DataSet();
                    SqlDataAdapter da = new SqlDataAdapter(cmd);
                    da.Fill(ds);
                    tables.Add(ds.Tables[0]);
                }
                trans.Commit();
            }
            return tables;
        }


        /// <summary>
        /// Scripts out a table as CREATE TABLE
        /// </summary>
        /// <param name="server">Server identifier to connect to</param>
        /// <param name="dbName">Database name</param>
        /// <param name="table">Table name</param>
        /// <param name="schema">Table's schema</param>
        /// <returns>The CREATE TABLE script as a string</returns>
        public string ScriptTable(string dbName, string table, string schema) {
            //initialize scriptoptions variable
            ScriptingOptions scriptOptions = new ScriptingOptions();
            scriptOptions.ScriptBatchTerminator = true;
            scriptOptions.NoCollation = true;
            //get smo table object
            Table t_smo = GetSmoTable(dbName, table, schema);

            //script out the table, it comes back as a StringCollection object with one string per query batch
            StringCollection scriptResults = t_smo.Script(scriptOptions);

            //ADO.NET does not allow multiple batches in one query, but we don't really need the
            //SET ANSI_NULLS ON etc. statements, so just find the CREATE TABLE statement and return that
            foreach (string s in scriptResults) {
                if (s.StartsWith("CREATE")) {
                    return s;
                }
            }
            return "";
        }

        public void Consolidate(string ctTableName, string consolidatedTableName, string dbName, string schemaName) {
            var columns = GetIntersectColumnList(dbName, ctTableName, schemaName, consolidatedTableName, schemaName);
            var query = string.Format(
                "INSERT INTO [{0}] ({1}) SELECT {1} FROM [{2}]",
                consolidatedTableName, string.Join(",", columns), ctTableName);
            var cmd = new SqlCommand(query);
            SqlNonQuery(dbName, cmd);
        }

        public void RemoveDuplicatePrimaryKeyChangeRows(TableConf table, string ctTableName, string dbName) {

            var pks = table.columns.Where(c => c.isPk);
            var zipped = pks.Zip(pks, (a, b) => "a." + a + " = b." + b);
            string whereCondition = string.Join(" AND ", zipped);

            string delete = string.Format(
                            @"DELETE a FROM [{0}] a 
                              WHERE EXISTS (
                                SELECT 1 FROM [{0}] b WHERE {1} AND a.sys_change_version < b.sys_change_version
                              ) ",
                              ctTableName, whereCondition);
            logger.Log(delete, LogLevel.Trace);
            SqlCommand cmd = new SqlCommand(delete);
            SqlNonQuery(dbName, cmd);
        }


        public DataTable GetPendingCTSlaveVersions(string dbName, string slaveIdentifier) {
            string query = @"SELECT * FROM tblCTSlaveVersion
                            WHERE slaveIdentifier = @slaveidentifier AND CTID >
                            (
                            	SELECT MAX(ctid) FROM tblCTSlaveVersion WHERE slaveIdentifier = @slaveidentifier AND syncBitWise = @bitwise
                            )";
            SqlCommand cmd = new SqlCommand(query);
            cmd.Parameters.Add("@bitwise", SqlDbType.Int).Value = Enum.GetValues(typeof(SyncBitWise)).Cast<int>().Sum();
            cmd.Parameters.Add("@slaveidentifier", SqlDbType.VarChar, 500).Value = slaveIdentifier;
            logger.Log("Running query: " + cmd.CommandText + "... slaveidentifiers is " + slaveIdentifier, LogLevel.Debug);
            return SqlQuery(dbName, cmd);
        }


        public void CreateHistoryTable(ChangeTable t, string slaveCTDB) {
            if (CheckTableExists(slaveCTDB, t.historyName, t.schemaName)) {
                return;
            }
            string create = ScriptTable(slaveCTDB, t.ctName, t.schemaName);
        }

        public void CopyIntoHistoryTable(ChangeTable t, string dbName) {
            string sql;
            if (CheckTableExists(dbName, t.historyName, t.schemaName)) {
                logger.Log("table " + t.historyName + " already exists; selecting into it", LogLevel.Trace);
                sql = string.Format("INSERT INTO {0} SELECT {1} AS CTHistID, * FROM {2}", t.historyName, t.ctid, t.ctName);
                logger.Log(sql, LogLevel.Debug);
            } else {
                logger.Log("table " + t.historyName + " does not exist, inserting into it", LogLevel.Trace);
                sql = string.Format("SELECT {0} AS CTHistID, * INTO {1} FROM {2}", t.ctid, t.historyName, t.ctName);
                logger.Log(sql, LogLevel.Debug);
            }
            var cmd = new SqlCommand(sql);
            SqlNonQuery(dbName, cmd);
        }


        public ChangeTrackingBatch GetCTBatch(string dbName, Int64 ctid) {
            SqlCommand cmd;
            cmd = new SqlCommand("SELECT TOP 1 CTID, syncStartVersion, syncStopVersion, syncBitWise FROM dbo.tblCTVersion WHERE ctid = @ctid");
            cmd.Parameters.Add("@ctid", SqlDbType.BigInt).Value = ctid;
            DataTable result = SqlQuery(dbName, cmd);
            return result.Rows.Count > 0 ? new ChangeTrackingBatch(result.Rows[0]) : null;
        }

        public void RevertCTBatch(string dbName, Int64 ctid) {
            SqlCommand cmd;
            cmd = new SqlCommand("UPDATE dbo.tblCTVersion SET SyncBitWise = 0 WHERE ctid = @ctid");
            cmd.Parameters.Add("@ctid", SqlDbType.BigInt).Value = ctid;
            SqlNonQuery(dbName, cmd);
        }


        public void MergeCTTable(TableConf table, string destDB, string sourceDB, long CTID) {
            //TODO this is probably unnecessary; maybe just use full column list in all conditions.
            var mergeList = table.columns.Any(c => !c.isPk) ? table.mergeUpdateList : string.Join(",", table.columns.Select(c => String.Format("P.{0} = CT.{0}", c.name)));
            var columnList = string.Join(",", table.columns.Select(c => c.name));
            var insertList = string.Join(",", table.columns.Select(c => "CT." + c.name));

            string sql =
                string.Format(@"MERGE dbo.{0} WITH(ROWLOCK) AS P
	               USING (SELECT * FROM {1}.dbo.{0}) AS CT
	               ON ({2})
	               WHEN MATCHED AND P.SYS_CHANGE_OPERATION = 'D' AND CT.SYS_CHANGE_OPERATION IN ('I', 'U')
	                 THEN UPDATE SET {3}, P.SYS_CHANGE_OPERATION = CT.SYS_CHANGE_OPERATION, P.SYS_CHANGE_VERSION = CT.SYS_CHANGE_VERSION
	               WHEN NOT MATCHED
	                 THEN INSERT ({4},SYS_CHANGE_VERSION, SYS_CHANGE_OPERATION) VALUES ({5}, CT.SYS_CHANGE_VERSION, CT.SYS_CHANGE_OPERATION);",
                   table.ToCTName(CTID), sourceDB, table.pkList, mergeList, columnList, insertList);
            SqlCommand cmd = new SqlCommand(sql);
            SqlNonQuery(destDB, cmd);
        }

        public IEnumerable<string> GetPrimaryKeysFromInfoTable(TableConf table, ChangeTrackingBatch batch, string database) {
            string sql = string.Format(@"SELECT CtipkList FROM {0} WHERE CtiTableName = @tableName", batch.infoTable);
            SqlCommand cmd = new SqlCommand(sql);
            cmd.Parameters.Add("@tableName", SqlDbType.VarChar, 5000).Value = table.name;
            var res = SqlQueryToScalar<string>(database, cmd);
            return res.Split(new char[] { ',' });
        }

        public void RecreateView(string dbName, string viewName, string viewSelect) {
            try {
                var view = GetSmoView(dbName, viewName);
                view.Drop();
            } catch (DoesNotExistException) {

            }
            var cmd = new SqlCommand(string.Format("CREATE VIEW {0} AS {1}", viewName, viewSelect));
            SqlNonQuery(dbName, cmd);
        }

        public int GetExpectedRowCounts(string ctDbName, long ctid) {
            string sql = string.Format("SELECT SUM(CtiExpectedRows) FROM tblCTTableInfo_{0};", ctid);
            var cmd = new SqlCommand(sql);
            return SqlQueryToScalar<int>(ctDbName, cmd);
        }

        public IEnumerable<long> GetOldCTIDsMaster(string dbName, DateTime chopDate) {
            string sql = "SELECT ctid FROM tblCTVersion WHERE syncStartTime < @chopDate";
            var cmd = new SqlCommand(sql);
            cmd.Parameters.Add("@chopDate", SqlDbType.DateTime).Value = chopDate;
            var res = SqlQuery(dbName, cmd);
            var ctids = new List<long>();
            foreach (DataRow row in res.Rows) {
                ctids.Add(row.Field<long>("ctid"));
            }
            return ctids;
        }
        public IEnumerable<long> GetOldCTIDsRelay(string dbName, DateTime chopDate) {
            string sql = @"SELECT ctid, MAX(syncstoptime) AS maxstop
                           FROM [CT_csn_cttest].[dbo].[tblCTSlaveVersion]
                           GROUP BY CTID
                           HAVING MAX(syncstoptime) < @chopDate";
            var cmd = new SqlCommand(sql);
            cmd.Parameters.Add("@chopDate", SqlDbType.DateTime).Value = chopDate;
            var res = SqlQuery(dbName, cmd);
            var ctids = new List<long>();
            foreach (DataRow row in res.Rows) {
                ctids.Add(row.Field<long>("ctid"));
            }
            return ctids;
        }

        public IEnumerable<long> GetOldCTIDsSlave(string dbName, DateTime chopDate, string slaveIdentifier) {
            string sql = @"SELECT ctid 
                           FROM [CT_csn_cttest].[dbo].[tblCTSlaveVersion]
                           WHERE slaveIdentifier = @slaveIdentifier
                           AND syncstoptime < @chopDate";
            var cmd = new SqlCommand(sql);
            cmd.Parameters.Add("@slaveIdentifier", SqlDbType.VarChar, 500).Value = slaveIdentifier;
            cmd.Parameters.Add("@chopDate", SqlDbType.DateTime).Value = chopDate;
            var res = SqlQuery(dbName, cmd);
            var ctids = new List<long>();
            foreach (DataRow row in res.Rows) {
                ctids.Add(row.Field<long>("ctid"));
            }
            return ctids;
        }


        public void DeleteOldCTVersions(string dbName, DateTime chopDate) {
            string sql = "DELETE FROM tblCTVersion WHERE syncStartTime < @chopDate";
            var cmd = new SqlCommand(sql);
            cmd.Parameters.Add("@chopDate", SqlDbType.DateTime).Value = chopDate;
            SqlNonQuery(dbName, cmd);
        }

        public void DeleteOldCTSlaveVersions(string dbName, DateTime chopDate) {
            string sql = "DELETE FROM tblCTSlaveVersion WHERE ISNULL(syncStopTime,syncStartTime) < @chopDate";
            var cmd = new SqlCommand(sql);
            cmd.Parameters.Add("@chopDate", SqlDbType.DateTime).Value = chopDate;
            SqlNonQuery(dbName, cmd);
        }

        public bool IsBeingInitialized(string sourceCTDB, TableConf table) {
            string sql = string.Format(@"SELECT 1 FROM tblCTInitialize WHERE tableName = @tableName AND inProgress = 1",
                                       sourceCTDB);
            var cmd = new SqlCommand(sql);
            cmd.Parameters.Add("@tableName", SqlDbType.VarChar, 500).Value = table.name;
            var res = SqlQuery(sourceCTDB, cmd);
            return res.Rows.Count > 0;
        }

        public long? GetInitializeStartVersion(string sourceCTDB, TableConf table) {
            string sql = @"SELECT nextSynchVersion FROM tblCTInitialize WHERE tableName = @tableName";
            var cmd = new SqlCommand(sql);
            cmd.Parameters.Add("@tableName", SqlDbType.VarChar, 500).Value = table.name;
            var res = SqlQuery(sourceCTDB, cmd);
            if (res.Rows.Count == 0) {
                return null;
            } else {
                return res.Rows[0].Field<long>("nextSynchVersion");
            }
        }

        public void CleanUpInitializeTable(string dbName, long syncStartVersion) {
            //            string sql = @"DELETE FROM tblCTInitialize
            //                           WHERE inProgress = 0 
            //                           AND nextSynchVersion < @syncStartVersion
            //                           AND iniFinishTime < (
            //                               SELECT MAX(syncStartTime) FROM tblCTVersion
            //                               WHERE syncStopTime IS NOT NULL
            //                           )";
            //            var cmd = new SqlCommand(sql);
            //            cmd.Parameters.Add("@syncStartVersion", SqlDbType.BigInt).Value = syncStartVersion;
        }
    }
}
