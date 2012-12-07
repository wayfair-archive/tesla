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
    public class DataUtils : IDataUtils {

        public Logger logger;
        public Config config;

        public DataUtils(Config config, Logger logger) {
            this.config = config;
            this.logger = logger;
        }

        /// <summary>
        /// Runs a sql query and returns results as requested type
        /// </summary>
        /// <param name="server">Server to run the query on</param>
        /// <param name="dbName">Database name</param>
        /// <param name="cmd">SqlCommand to run</param>
        /// <param name="timeout">Query timeout</param>
        /// <returns>DataTable object representing the result</returns>
        private DataTable SqlQuery(TServer server, string dbName, SqlCommand cmd, int timeout = 30) {
            //build connection string based on server/db info passed in
            string connStr = buildConnString(server, dbName);

            //using block to avoid resource leaks
            using (SqlConnection conn = new SqlConnection(connStr)) {
                //open database connection
                conn.Open();
                cmd.Connection = conn;                    
                cmd.CommandTimeout = timeout;

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
        /// <param name="server">Server to run the query on</param>
        /// <param name="dbName">Database name</param>
        /// <param name="cmd">SqlCommand to run</param>
        /// <param name="timeout">Query timeout</param>
        /// <returns>The value in the first column and row, as the specified type</returns>
        private T SqlQueryToScalar<T>(TServer server, string dbName, SqlCommand cmd, int timeout = 30) {
            DataTable result = SqlQuery(server, dbName, cmd, timeout);
            //return result in first column and first row as specified type
            return (T)result.Rows[0][0];
        }

        /// <summary>
        /// Runs a query that does not return results (i.e. a write operation)
        /// </summary>
        /// <param name="server">Server to run on</param>
        /// <param name="dbName">Database name</param>
        /// <param name="cmd">SqlCommand to run</param>
        /// <param name="timeout">Timeout (higher than selects since some writes can be large)</param>
        /// <returns>The number of rows affected</returns>
        private int SqlNonQuery(TServer server, string dbName, SqlCommand cmd, int timeout = 600) {
            //build connection string based on server/db info passed in
            string connStr = buildConnString(server, dbName);
            int numrows;
            //using block to avoid resource leaks
            using (SqlConnection conn = new SqlConnection(connStr)) {
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
        private string buildConnString(TServer server, string database) {
            string sqlhost = "";
            string sqluser = "";
            string sqlpass = "";

            switch (server) {
                case TServer.MASTER:
                    sqlhost = config.master;
                    sqluser = config.masterUser;
                    sqlpass = (new cTripleDes().Decrypt(config.masterPassword));
                    break;
                case TServer.SLAVE:
                    sqlhost = config.slave;
                    sqluser = config.slaveUser;
                    sqlpass = (new cTripleDes().Decrypt(config.slavePassword));
                    break;
                case TServer.RELAY:
                    sqlhost = config.relayServer;
                    sqluser = config.relayUser;
                    sqlpass = (new cTripleDes().Decrypt(config.relayPassword));
                    break;
            }

            return "Data Source=" + sqlhost + "; Initial Catalog=" + database + ";User ID=" + sqluser + ";Password=" + sqlpass;
        }


        /// <summary>
        /// Gets information on the last CT batch relevant to this agent
        /// </summary>
        /// <param name="server">Server identifier</param>
        /// <param name="dbName">Database name</param>
        /// <param name="agentType">We need to query a different table for master vs. slave</param>
        /// <param name="slaveIdentifier">Hostname of the slave if applicable</param>
        public DataRow GetLastCTBatch(TServer server, string dbName, AgentType agentType, string slaveIdentifier = "") {
            SqlCommand cmd;
            //for slave we have to pass the slave identifier in and use tblCTSlaveVersion
            if (agentType.Equals(AgentType.Slave)) {
                cmd = new SqlCommand("SELECT TOP 1 CTID, syncStartVersion, syncStopVersion, syncBitWise" + 
                    " FROM dbo.tblCTSlaveVersion WITH(NOLOCK) WHERE slaveIdentifier = @slave ORDER BY CTID DESC");
                cmd.Parameters.Add("@slave", SqlDbType.VarChar, 100).Value = slaveIdentifier;
            } else {
                cmd = new SqlCommand("SELECT TOP 1 CTID, syncStartVersion, syncStopVersion, syncBitWise FROM dbo.tblCTVersion ORDER BY CTID DESC");
            }

            DataTable result = SqlQuery(server, dbName, cmd);
            return result.Rows[0];
        }


        /// <summary>
        /// Gets CT versions that are greater than the passed in CTID and have the passed in bitwise value
        /// </summary>
        /// <param name="server">Server to check</param>
        /// <param name="dbName">Database name to check</param>
        /// <param name="CTID">Pull CTIDs greater than this one</param>
        /// <param name="syncBitWise">Only include versions containing this bit</param>
        public DataTable GetPendingCTVersions(TServer server, string dbName, Int64 CTID, int syncBitWise) {
            string query = ("SELECT CTID, syncStartVersion, syncStopVersion, syncStartTime, syncBitWise" +
                " FROM dbo.tblCTVersion WITH(NOLOCK) WHERE CTID > @ctid AND syncBitWise & @syncbitwise > 0" +
                " ORDER BY CTID ASC");
            SqlCommand cmd = new SqlCommand(query);
            cmd.Parameters.Add("@ctid", SqlDbType.BigInt).Value = CTID;
            cmd.Parameters.Add("@syncbitwise", SqlDbType.Int).Value = syncBitWise;

            //get query results as a datatable since there can be multiple rows
            return SqlQuery(server, dbName, cmd);
        }


        /// <summary>
        /// Gets the start time of the last successful CT batch before the specified CTID
        /// </summary>
        /// <param name="server">Server identifier</param>
        /// <param name="dbName">Database name</param>
        /// <param name="CTID">Current CTID</param>
        /// <param name="syncBitWise">syncBitWise value to compare against</param>
        /// <returns>Datetime representing last succesful run</returns>
        public DateTime GetLastStartTime(TServer server, string dbName, Int64 CTID, int syncBitWise) {
            SqlCommand cmd = new SqlCommand("select MAX(syncStartTime) as maxStart FROM dbo.tblCTVersion WITH(NOLOCK)"
                + " WHERE syncBitWise & @syncbitwise > 0 AND CTID < @CTID");
            cmd.Parameters.Add("@syncbitwise", SqlDbType.Int).Value = syncBitWise;
            cmd.Parameters.Add("@CTID", SqlDbType.BigInt).Value = CTID;
            DateTime? lastStartTime = SqlQueryToScalar<DateTime?>(server, dbName, cmd);
            if (lastStartTime == null) {
                return DateTime.Now.AddDays(-1);
            }
            return (DateTime)lastStartTime;
        }

        
        /// <summary>
        /// Gets the CHANGE_TRACKING_CURRENT_VERSION() for a database
        /// </summary>
        /// <param name="server">Server identifer</param>
        /// <param name="dbName">Database name</param>
        /// <returns>Current change tracking version</returns>
        public Int64 GetCurrentCTVersion(TServer server, string dbName) {
            SqlCommand cmd = new SqlCommand("SELECT CHANGE_TRACKING_CURRENT_VERSION();");
            return SqlQueryToScalar<Int64>(server, dbName, cmd);
        }


        /// <summary>
        /// Gets the minimum valid CT version for a table
        /// </summary>
        /// <param name="server">Server identifier</param>
        /// <param name="dbName">Database name</param>
        /// <param name="table">Table name</param>
        /// <returns>Minimum valid version</returns>
        public Int64 GetMinValidVersion(TServer server, string dbName, string table) {
            SqlCommand cmd = new SqlCommand("SELECT CHANGE_TRACKING_MIN_VALID_VERSION(OBJECT_ID(@tablename))");
            cmd.Parameters.Add("@tablename", SqlDbType.VarChar, 500).Value = table;
            return SqlQueryToScalar<Int64>(server, dbName, cmd);
        }
        

        /// <summary>
        /// Generates and runs SELECT INTO query to create a changetable
        /// </summary>
        /// <param name="server">Server identifer</param>
        /// <param name="sourceCTDB">Source CT database name</param>
        /// <param name="masterColumnList">column list for the select statement</param>
        /// <param name="ctTableName">CT table name</param>
        /// <param name="sourceDB">Source database name</param>
        /// <param name="tableName">Table name</param>
        /// <param name="startVersion">syncStartVersion for the batch</param>
        /// <param name="pkList">Primary key list for join condition</param>
        /// <param name="stopVersion">syncStopVersion for the batch</param>
        /// <param name="notNullPkList">Primary key list for where clause</param>
        /// <param name="timeout">How long this is allowed to run for (seconds)</param>
        /// <returns>Int representing the number of rows affected (number of changes captured)</returns>
        public int SelectIntoCTTable(TServer server, string sourceCTDB, string masterColumnList, string ctTableName,
            string sourceDB, string tableName, Int64 startVersion, string pkList, Int64 stopVersion, string notNullPkList, int timeout) {
            /*
             * There is no way to have column lists or table names be parametrized/dynamic in sqlcommands other than building the string
             * manually like this. However, the table name and column list fields are trustworthy because they have already been compared to 
             * actual database objects at this point. The database names are also validated to be legal database identifiers.
             * Only the start and stop versions are actually parametrizable.
             */            
            string query = "SELECT " + masterColumnList + ", CT.SYS_CHANGE_VERSION, CT.SYS_CHANGE_OPERATION ";
            query += " INTO " + ctTableName;
            query += " FROM CHANGETABLE(CHANGES " + sourceDB + ".dbo." + tableName + ", @startVersion) CT";
            query += " LEFT OUTER JOIN " + sourceDB + ".dbo." + tableName + " P ON " + pkList;
            query += " WHERE (SYS_CHANGE_VERSION <= @stopVersion OR SYS_CHANGE_CREATION_VERSION <= @stopversion)";            
            query += " AND (SYS_CHANGE_OPERATION = 'D' OR " + notNullPkList + ")";

            SqlCommand cmd = new SqlCommand(query);

            cmd.Parameters.Add("@startVersion", SqlDbType.BigInt).Value = startVersion;
            cmd.Parameters.Add("@stopVersion", SqlDbType.BigInt).Value = stopVersion;

            return SqlNonQuery(server, sourceCTDB, cmd, 1200);
        }

        /// <summary>
        /// Creates a new row in tblCTVersion
        /// </summary>
        /// <param name="server">Server identifier</param>
        /// <param name="dbName">Database name</param>
        /// <param name="syncStartVersion">Version number the batch starts at</param>
        /// <param name="syncStopVersion">Version number the batch ends at</param>
        /// <returns>CTID generated by the database</returns>
        public Int64 CreateCTVersion(TServer server, string dbName, Int64 syncStartVersion, Int64 syncStopVersion) {            
            //create new row in tblCTVersion, output the CTID
            string query = "INSERT INTO dbo.tblCTVersion (syncStartVersion, syncStopVersion, syncStartTime, syncBitWise)";
            query += " OUTPUT inserted.CTID";
            query += " VALUES (@startVersion, @stopVersion, GETDATE(), 0)";

            SqlCommand cmd = new SqlCommand(query);

            cmd.Parameters.Add("@startVersion", SqlDbType.BigInt).Value = syncStartVersion;
            cmd.Parameters.Add("@stopVersion", SqlDbType.BigInt).Value = syncStopVersion;

            return SqlQueryToScalar<Int64>(server, dbName, cmd);
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="server">Server identifier to write to</param>
        /// <param name="dbName">Database name to write to</param>
        /// <param name="slaveIdentifier">Slave identifier string (usually hostname)</param>
        /// <param name="CTID">Batch number (generated on master)</param>
        /// <param name="syncStartVersion">Version number the batch starts at</param>
        /// <param name="syncStopVersion">Version number the batch ends at</param>
        /// <param name="syncBitWise">Current bitwise value for the batch</param>
        /// <param name="syncStartTime">Time the batch started on the master</param>
        public void CreateSlaveCTVersion(TServer server, string dbName, Int64 CTID, string slaveIdentifier,
            Int64 syncStartVersion, Int64 syncStopVersion, DateTime syncStartTime, Int32 syncBitWise) {

            //create new row in tblCTVersion, output the CTID
                string query = "INSERT INTO dbo.tblCTSlaveVersion (CTID, slaveIdentifier, syncStartVersion, syncStopVersion, syncStartTime, syncBitWise)";
                query += " VALUES (@ctid, @slaveidentifier, @startversion, @stopversion, @starttime, @syncbitwise)";

            SqlCommand cmd = new SqlCommand(query);

            cmd.Parameters.Add("@ctid", SqlDbType.BigInt).Value = CTID;
            cmd.Parameters.Add("@slaveidentifier", SqlDbType.VarChar, 100).Value = slaveIdentifier;
            cmd.Parameters.Add("@startversion", SqlDbType.BigInt).Value = syncStartVersion;
            cmd.Parameters.Add("@stopversion", SqlDbType.BigInt).Value = syncStopVersion;
            cmd.Parameters.Add("@starttime", SqlDbType.DateTime).Value = syncStartTime;
            cmd.Parameters.Add("@syncbitwise", SqlDbType.Int).Value = syncBitWise;

            int result = SqlNonQuery(server, dbName, cmd, 30);
        }

        /// <summary>
        /// Create the tblCTSchemaChange_(version) table on the relay server, dropping if it already exists       
        /// </summary>
        /// <param name="dbName">Database to run on</param>
        /// <param name="ct_id">CT version number</param>
        public void CreateSchemaChangeTable(TServer server, string dbName, Int64 ct_id) {
            //drop the table on the relay server if it exists
            bool tExisted = DropTableIfExists(server, dbName, "tblCTSchemaChange_" + Convert.ToString(ct_id));

            //can't parametrize the CTID since it's part of a table name, but it is an Int64 so it's not an injection risk
            string query = "CREATE TABLE [dbo].[tblCTSchemaChange_" + Convert.ToString(ct_id) + "] (";
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

            int result = SqlNonQuery(server, dbName, cmd);
        }
       

        /// <summary>
        /// Get DDL events from tblDDLEvent that occurred after the specified date
        /// </summary>
        /// <param name="server">Server identifier</param>
        /// <param name="dbName">Database name</param>
        /// <param name="afterDate">Date to start from</param>
        /// <returns>DataTable object representing the events</returns>
        public DataTable GetDDLEvents(TServer server, string dbName, DateTime afterDate) {
            if (!CheckTableExists(server, dbName, "tblDDLEvent")) {
                throw new Exception("tblDDLEvent does not exist on the source database, unable to check for schema changes. Please create the table and the trigger that populates it!");
            }

            string query = "SELECT DdeID, DdeEventData FROM dbo.tblDDLEvent WHERE DdeTime > @afterdate";

            SqlCommand cmd = new SqlCommand(query);
            cmd.Parameters.Add("@afterdate", SqlDbType.DateTime).Value = afterDate;

            return SqlQuery(server, dbName, cmd);
        }


        /// <summary>
        /// Writes a schema change record to the appropriate schema change table
        /// </summary>
        /// <param name="server">Server identifier</param>
        /// <param name="dbName">Database name</param>
        /// <param name="CTID">Batch ID</param>
        /// <param name="ddeID">DDL event identifier from source database</param>
        /// <param name="eventType">Type of schema change ( i.e. add/drop/modify/rename)</param>
        /// <param name="schemaName">Schema name this applies to (usually dbo)</param>
        /// <param name="tableName">Table name this schema change applies to</param>
        /// <param name="columnName">Column name</param>
        /// <param name="previousColumnName">Previous column name (applicable only to renames)</param>
        /// <param name="baseType">Basic data type of the column (applicable to add and modify)</param>
        /// <param name="characterMaximumLength">Maximum length for string columns (i.e. varchar, nvarchar)</param>
        /// <param name="numericPrecision">Numeric precision (for decimal/numeric columns)</param>
        /// <param name="numericScale">Numeric scale (for decimal/numeric columns)</param>
        public void WriteSchemaChange(TServer server, string dbName, Int64 CTID, int ddeID, string eventType, string schemaName, string tableName,
            string columnName, string newColumnName, string baseType, int? characterMaximumLength, int? numericPrecision, int? numericScale) {

            string query = "INSERT INTO dbo.tblCTSchemaChange_" + Convert.ToString(CTID) + 
                " (CscDdeID, CscTableName, CscEventType, CscSchema, CscColumnName, CscNewColumnName, " +
                " CscBaseDataType, CscCharacterMaximumLength, CscNumericPrecision, CscNumericScale) " +
                " VALUES (@ddeid, @tablename, @eventtype, @schema, @columnname, @newcolumnname, " + 
                " @basedatatype, @charactermaximumlength, @numericprecision, @numericscale)";

            var cmd = new SqlCommand(query);
            cmd.Parameters.Add("@ddeid", SqlDbType.Int).Value = ddeID;
            cmd.Parameters.Add("@tablename", SqlDbType.VarChar, 500).Value = tableName;
            cmd.Parameters.Add("@eventtype", SqlDbType.VarChar, 50).Value = eventType;
            cmd.Parameters.Add("@schema", SqlDbType.VarChar, 100).Value = schemaName;
            cmd.Parameters.Add("@columnname", SqlDbType.VarChar, 500).Value = columnName;
            cmd.Parameters.Add("@newcolumnname", SqlDbType.VarChar, 500).Value = newColumnName;
            cmd.Parameters.Add("@basedatatype", SqlDbType.VarChar, 100).Value = baseType;
            cmd.Parameters.Add("@charactermaximumlength", SqlDbType.Int).Value = characterMaximumLength;
            cmd.Parameters.Add("@numericprecision", SqlDbType.Int).Value = numericPrecision;
            cmd.Parameters.Add("@numericscale", SqlDbType.Int).Value = numericScale;
            foreach (IDataParameter p in cmd.Parameters) {
                if (p.Value == null) p.Value = DBNull.Value;
            }
            int result = SqlNonQuery(server, dbName, cmd);
        }


        /// <summary>
        /// Gets a column's data type
        /// </summary>
        /// <param name="server"></param>
        /// <param name="dbName"></param>
        /// <param name="table"></param>
        /// <param name="column"></param>
        /// <returns>DataRow representing the data type</returns>
        public DataRow GetDataType(TServer server, string dbName, string table, string column) {
            var cmd = new SqlCommand("SELECT DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE " +
                "FROM INFORMATION_SCHEMA.COLUMNS WITH(NOLOCK) WHERE TABLE_SCHEMA = 'dbo' AND TABLE_CATALOG = @db " +
                "AND TABLE_NAME = @table AND COLUMN_NAME = @column");
            cmd.Parameters.Add("@db", SqlDbType.VarChar, 500).Value = dbName;
            cmd.Parameters.Add("@table", SqlDbType.VarChar, 500).Value = table;
            cmd.Parameters.Add("@column", SqlDbType.VarChar, 500).Value = column;

            DataTable result = SqlQuery(server, dbName, cmd);

            if (result == null || result.Rows.Count == 0) {
                throw new DoesNotExistException("Column " + column + " does not exist on table " + table + "!");
            }

            return result.Rows[0];
        }


        /// <summary>
        /// Updates the syncStopVersion in tblCTVersion to the specified value for the specified CTID
        /// </summary>
        /// <param name="server">Server identifier</param>
        /// <param name="dbName">Database name</param>
        /// <param name="syncStopVersion">New syncStopVersion</param>
        /// <param name="CTID">Batch identifier</param>
        public void UpdateSyncStopVersion(TServer server, string dbName, Int64 syncStopVersion, Int64 CTID) {

            string query = "UPDATE dbo.tblCTVersion set syncStopVersion = @stopversion WHERE CTID = @ctid";
            SqlCommand cmd = new SqlCommand(query);
            
            cmd.Parameters.Add("@stopVersion", SqlDbType.BigInt).Value = syncStopVersion;
            cmd.Parameters.Add("@ctid", SqlDbType.BigInt).Value = CTID;
            
            int res = SqlNonQuery(server, dbName, cmd);
        }

        //TODO turn this into a proper unit test, mocking the sql results?
        /*
        public int RunTests() {
            int retval = 0;

            //test query with results as datarow
            DataRow dr = SqlQuery(TServer.RELAY, config.relayDB, "select @@servername as srvname, GETDATE() as thedate", 30, ResultType.DATAROW) as DataRow;
            if (dr == null || string.IsNullOrEmpty(dr["srvname"].ToString()) || string.IsNullOrEmpty(dr["thedate"].ToString()))
                retval = 1;

            //test query with results as datatable
            DataTable dt = SqlQuery(TServer.RELAY, config.relayDB, "select @@servername as srvname, GETDATE() as thedate", 30, ResultType.DATATABLE) as DataTable;
            if (dt == null || string.IsNullOrEmpty(dt.Rows[0]["srvname"].ToString()) || string.IsNullOrEmpty(dt.Rows[0]["thedate"].ToString()))
                retval = 1;

            //test result as dataset
            DataSet ds = SqlQuery(TServer.RELAY, config.relayDB, "select @@servername as srvname, GETDATE() as thedate", 30, ResultType.DATASET) as DataSet;
            if (ds == null || string.IsNullOrEmpty(ds.Tables[0].Rows[0]["srvname"].ToString()) || string.IsNullOrEmpty(ds.Tables[0].Rows[0]["thedate"].ToString()))
                retval = 1;

            //test result as 32 bit int
            Int32 int_32 = (Int32)SqlQuery(TServer.RELAY, config.relayDB, "select cast(10 as int) as myint", 30, ResultType.INT32);
            if (int_32 != 10)
                retval = 1;

            //test result as 64 bit int
            Int64 int_64 = (Int64)SqlQuery(TServer.RELAY, config.relayDB, "select cast(10000000000 as bigint) as mybigint", 30, ResultType.INT64);
            if (int_64 != 10000000000)
                retval = 1;

            //test result as string
            String s_result = (String)SqlQuery(TServer.RELAY, config.relayDB, "select 'test' as mystring", 30, ResultType.STRING);
            if (s_result != "test")
                retval = 1;

            //test result as datetime
            DateTime? dt_result = (DateTime?)SqlQuery(TServer.RELAY, config.relayDB, "select CAST('2000-01-01' AS DATETIME)", 30, ResultType.DATETIME);
            if (!dt_result.Equals(new DateTime(2000, 1, 1)))
                retval = 1;

            return retval;
        }
        */

        /// <summary>
        /// Retrieves an SMO table object if the table exists, throws exception if not.
        /// </summary>
        /// <param name="server">Server idenitfier</param>
        /// <param name="dbName">Database name</param>
        /// <param name="table">Table Name</param>
        /// <returns>Smo.Table object representing the table</returns>
        private Table GetSmoTable(TServer server, string dbName, string table) {
            using (SqlConnection sqlconn = new SqlConnection(buildConnString(server, dbName))) {
                ServerConnection serverconn = new ServerConnection(sqlconn);
                Server svr = new Server(serverconn);
                Database db = new Database();
                if (svr.Databases.Contains(dbName) && svr.Databases[dbName].IsAccessible) {
                    db = svr.Databases[dbName];
                } else {
                    throw new Exception("Database " + dbName + " does not exist or is inaccessible");
                }
                if (db.Tables.Contains(table)) {
                    return db.Tables[table];
                } else {
                    throw new DoesNotExistException("Table " + table + " does not exist");
                }
            }
        }


        /// <summary>
        /// Check to see if a table exists on the specified server
        /// </summary>
        /// <param name="server">Server to check</param>
        /// <param name="dbName">Database name</param>
        /// <param name="table">Table name to chekc for</param>
        /// <returns>Boolean representing whether or not the table exists.</returns>
        public bool CheckTableExists(TServer server, string dbName, string table) {
            try {
                Table t_smo = GetSmoTable(server, dbName, table);
                
                if (t_smo != null) {
                    return true;
                }
                return false;
            } catch (DoesNotExistException) {
                return false;
            }
        }


        /// <summary>
        /// Compares two tables and retrieves a column list that is an intersection of the columns they contain
        /// </summary>
        /// <param name="server">Server identifier</param>
        /// <param name="dbName">Database name</param>
        /// <param name="table1">First table</param>
        /// <param name="table2">Second table (order doesn't matter)</param>
        /// <returns>String containing the resulting intersect column list</returns>
        public string GetIntersectColumnList(TServer server, string dbName, string table1, string table2) {
            Table t_smo_1 = GetSmoTable(server, dbName, table1);
            Table t_smo_2 = GetSmoTable(server, dbName, table2);
            string columnList = "";

            //list to hold lowercased column names
            var columns_2 = new List<string>();
            
            //create this so that casing changes to columns don't cause problems, just use the lowercase column name
            foreach(Column c in t_smo_2.Columns) {
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
        }


        /// <summary>
        /// Check whether an SMO table has a primary key by looping through its indexes
        /// </summary>
        /// <param name="t_smo">SMO table object</param>
        public bool HasPrimaryKey(TServer server, string dbName, string table) {
            Table t_smo = GetSmoTable(server, dbName, table);            
            foreach (Index i in t_smo.Indexes) {
                if (i.IndexKeyType == IndexKeyType.DriPrimaryKey) {
                    return true;
                }
            }
            return false;
        }

        

        
        /// <summary>
        /// Checks to see if a table exists on the specified server and drops it if so.
        /// </summary>
        /// <param name="server">Server identifier</param>
        /// <param name="dbName">Database name</param>
        /// <param name="table">Table name</param>
        /// <returns>Boolean specifying whether or not the table existed</returns>
        public bool DropTableIfExists(TServer server, string dbName, string table) {
            try {
                Table t_smo = GetSmoTable(server, dbName, table);
                if (t_smo != null) {
                    t_smo.Drop();
                    return true;
                }
                return false;
            } catch (DoesNotExistException) {
                return false;
            }
        }

        /// <summary>
        /// Runs a query on the source server and copies the resulting data to the destination
        /// </summary>
        /// <param name="sourceServer">Source server identifier</param>
        /// <param name="sourceDB">Source database name</param>
        /// <param name="destServer">Destination server identifier</param>
        /// <param name="destDB">Destination database name</param>
        /// <param name="cmd">Query to get the data from</param>
        /// <param name="destinationTable">Table to write to on the destination (must already exist)</param>
        /// <param name="queryTimeout">How long the query on the source can run for</param>
        /// <param name="bulkCopyTimeout">How long writing to the destination can take</param>
        public void CopyDataFromQuery(TServer sourceServer, string sourceDB, TServer destServer, string destDB, SqlCommand cmd, string destinationTable, int queryTimeout = 36000, int bulkCopyTimeout = 36000) {
            using (SqlConnection sourceConn = new SqlConnection(buildConnString(sourceServer, sourceDB))) {
                sourceConn.Open();
                cmd.Connection = sourceConn;                
                cmd.CommandTimeout = queryTimeout;
                SqlDataReader reader = cmd.ExecuteReader();

                SqlBulkCopy bulkCopy = new SqlBulkCopy(buildConnString(destServer, destDB), SqlBulkCopyOptions.KeepIdentity);
                bulkCopy.BulkCopyTimeout = bulkCopyTimeout;
                bulkCopy.DestinationTableName = destinationTable;
                bulkCopy.WriteToServer(reader);
            }
        }

        /// <summary>
        /// Copy the contents of a table from source to destination
        /// </summary>
        /// <param name="sourceServer">Source server identifier</param>
        /// <param name="sourceDB">Source database name</param>
        /// <param name="table">Table name</param>
        /// <param name="destServer">Destination server identifier</param>
        /// <param name="destDB">Destination database name</param>  
        /// <param name="timeout">Used as timeout for both the query and the bulk copy</param>
        public void CopyTable(TServer sourceServer, string sourceDB, string table, TServer destServer, string destDB, int timeout) {
            //drop table at destination and create from source schema
            CopyTableDefinition(sourceServer, sourceDB, table, destServer, destDB);

            //can't parametrize table name but it has already been validated against the server so it's safe
            SqlCommand cmd = new SqlCommand("SELECT * FROM " + table);
            CopyDataFromQuery(sourceServer, sourceDB, destServer, destDB, cmd, table, timeout, timeout);
        }


        /// <summary>
        /// Copies the schema of a table from one server to another, dropping it first if it exists at the destination.
        /// </summary>
        /// <param name="sourceServer">Source server identifier</param>
        /// <param name="sourceDB">Source database name</param>
        /// <param name="table">Table name</param>
        /// <param name="destServer">Destination server identifier</param>
        /// <param name="destDB">Destination database name</param>    
        public void CopyTableDefinition(TServer sourceServer, string sourceDB, string table, TServer destServer, string destDB) {
            //script out the table at the source
            string createScript = ScriptTable(sourceServer, sourceDB, table);
            SqlCommand cmd = new SqlCommand(createScript);

            //drop it if it exists at the destination
            bool didExist = DropTableIfExists(destServer, destDB, table);

            //create it at the destination
            int result = SqlNonQuery(destServer, destDB, cmd);
        }


        /// <summary>
        /// Scripts out a table as CREATE TABLE
        /// </summary>
        /// <param name="server">Server identifier to connect to</param>
        /// <param name="dbName">Database name</param>
        /// <param name="table">Table name</param>
        /// <returns>The CREATE TABLE script as a string</returns>
        public string ScriptTable(TServer server, string dbName, string table) {
            //initialize scriptoptions variable
            ScriptingOptions scriptOptions = new ScriptingOptions();
            scriptOptions.ScriptBatchTerminator = true;
            scriptOptions.NoCollation = true;
            
            //get smo table object
            Table t_smo = GetSmoTable(server, dbName, table);

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


        /// <summary>
        /// Gets a dictionary of columns for a table
        /// </summary>
        /// <param name="server">Server identifier</param>
        /// <param name="dbName">Database name</param>
        /// <param name="table">Table name</param>
        /// <returns>Dictionary with column name as key and a bool representing whether it's part of the primary key as value</returns>
        public Dictionary<string, bool> GetFieldList(TServer server, string dbName, string table) {
            Dictionary<string, bool> dict = new Dictionary<string, bool>();
            Table t_smo;

            //attempt to get smo table object
            try {
                t_smo = GetSmoTable(server, dbName, table);
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
        }


        /// <summary>
        /// Adds the specified bit to the syncBitWise column in tblCTVersion/tblCTSlaveVersion
        /// </summary>
        /// <param name="server">Server identifier to write to</param>
        /// <param name="dbName">Database name</param>
        /// <param name="ct_id">CT version number</param>
        /// <param name="value">Bit to add</param>
        /// <param name="agentType">Agent type running this request (if it's slave we use tblCTSlaveVersion)</param>
        public void WriteBitWise(TServer server, string dbName, Int64 ct_id, int value, AgentType agentType) {
            string query;
            SqlCommand cmd;
            if (agentType.Equals(AgentType.Slave)) {
                query = "UPDATE dbo.tblCTSlaveVersion SET SyncBitWise = SyncBitWise + @syncbitwise";
                query += " WHERE slaveIdentifier = @slaveidentifier AND CTID = @ctid AND SyncBitWise & @syncbitwise = 0";
                cmd = new SqlCommand(query);
                cmd.Parameters.Add("@syncbitwise", SqlDbType.Int).Value = value;
                cmd.Parameters.Add("@slaveidentifier", SqlDbType.VarChar, 100).Value = config.slave;
                cmd.Parameters.Add("@ctid", SqlDbType.BigInt).Value = ct_id;
            } else {
                query = "UPDATE dbo.tblCTVersion SET SyncBitWise = SyncBitWise + @syncbitwise";
                query += " WHERE CTID = @ctid AND SyncBitWise & @syncbitwise = 0"; 
                cmd = new SqlCommand(query);
                cmd.Parameters.Add("@syncbitwise", SqlDbType.Int).Value = value;
                cmd.Parameters.Add("@ctid", SqlDbType.BigInt).Value = ct_id;
            }
            int result = SqlNonQuery(server, dbName, cmd);
        }
        

        /// <summary>
        /// Gets syncbitwise for specified CT version table
        /// </summary>
        /// <param name="server">Server identifier to read from</param>
        /// <param name="dbName">Database name</param>
        /// <param name="ct_id">CT version number to check</param>
        /// <param name="agentType">Agent type running this request (if it's slave we use tblCTSlaveVersion)</param>
        public int ReadBitWise(TServer server, string dbName, Int64 ct_id, AgentType agentType) {
            string query;
            SqlCommand cmd;
            if (agentType.Equals(AgentType.Slave)) {
                query = "SELECT syncBitWise from dbo.tblCTSlaveVersion WITH(NOLOCK)";
                query += " WHERE slaveIdentifier = @slaveidentifier AND CTID = @ctid";
                cmd = new SqlCommand(query);
                cmd.Parameters.Add("@slaveidentifier", SqlDbType.VarChar, 100).Value = config.slave;
                cmd.Parameters.Add("@ctid", SqlDbType.BigInt).Value = ct_id;
            } else {
                query = "SELECT syncBitWise from dbo.tblCTVersion WITH(NOLOCK)";
                query += " WHERE CTID = @ctid";
                cmd = new SqlCommand(query);
                cmd.Parameters.Add("@ctid", SqlDbType.BigInt).Value = ct_id;
            }
            return SqlQueryToScalar<Int32>(server, dbName, cmd);
        }


        /// <summary>
        /// Marks a CT batch as complete
        /// </summary>
        /// <param name="server">Server identifier</param>
        /// <param name="dbName">Database name</param>
        /// <param name="ct_id">CT batch ID</param>
        /// <param name="syncBitWise">Final bitwise value to write</param>
        /// <param name="syncStopTime">Stop time to write</param>
        /// <param name="agentType">config.AgentType calling this</param>
        /// <param name="slaveIdentifier">For slave agents, the slave hostname or ip</param>
        public void MarkBatchComplete(TServer server, string dbName, Int64 ct_id, Int32 syncBitWise, DateTime syncStopTime, AgentType agentType, string slaveIdentifier = "") {
            string query;
            SqlCommand cmd;
            if (agentType.Equals(AgentType.Slave)) {
                query = "UPDATE dbo.tblCTSlaveVersion SET syncBitWise += @syncbitwise, syncStopTime = @syncstoptime";
                query += " WHERE slaveIdentifier = @slaveidentifier AND CTID = @ctid";
                cmd = new SqlCommand(query);
                cmd.Parameters.Add("@syncbitwise", SqlDbType.Int).Value = syncBitWise;
                cmd.Parameters.Add("@syncstoptime", SqlDbType.DateTime).Value = syncStopTime;
                cmd.Parameters.Add("@slaveidentifier", SqlDbType.VarChar, 100).Value = slaveIdentifier;
                cmd.Parameters.Add("@ctid", SqlDbType.BigInt).Value = ct_id;
            } else {
                query = "UPDATE dbo.tblCTVersion SET SyncBitWise = SyncBitWise + @syncbitwise";
                query += " WHERE CTID = @ctid AND SyncBitWise & @syncbitwise = 0";
                cmd = new SqlCommand(query);
                cmd.Parameters.Add("@syncbitwise", SqlDbType.Int).Value = syncBitWise;
                cmd.Parameters.Add("@syncstoptime", SqlDbType.DateTime).Value = syncStopTime;
                cmd.Parameters.Add("@ctid", SqlDbType.BigInt).Value = ct_id;
            }
            int result = SqlNonQuery(server, dbName, cmd);
        }


        /// <summary>
        /// Pulls the list of schema changes for a CTID 
        /// </summary>
        /// <param name="server">Server identifier</param>
        /// <param name="dbName">Database name</param>
        /// <param name="CTID">change tracking batch ID</param>
        /// <returns>DataTable object containing the query results</returns>
        public DataTable GetSchemaChanges(TServer server, string dbName, Int64 CTID) {
            SqlCommand cmd = new SqlCommand("SELECT DdeID, DdeTime, DdeEvent, DdeTable, DdeEventData FROM dbo.tblCTSchemaChange_" + Convert.ToString(CTID));
            return SqlQuery(server, dbName, cmd);
        }


        /// <summary>
        /// Gets the rowcounts for a table
        /// </summary>
        /// <param name="server">Server identifier</param>
        /// <param name="dbName">Database name</param>
        /// <param name="table">Table name</param>
        /// <returns>The number of rows in the table</returns>
        public Int64 GetTableRowCount(TServer server, string dbName, string table) {
            Table t_smo = GetSmoTable(server, dbName, table);
            return t_smo.RowCount;                  
        }

        /// <summary>
        /// Checks whether change tracking is enabled on a table
        /// </summary>
        /// <param name="server">Server identifier</param>
        /// <param name="dbName">Database name</param>
        /// <param name="table">Table name</param>
        /// <returns>True if it is enabled, false if it's not.</returns>
        public bool IsChangeTrackingEnabled(TServer server, string dbName, string table) {
            Table t_smo = GetSmoTable(server, dbName, table);
            return t_smo.ChangeTrackingEnabled;        
        }
    }
}
