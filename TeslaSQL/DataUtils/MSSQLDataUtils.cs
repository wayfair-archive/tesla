using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using System.Text;
using System.Data.SqlClient;
using System.Data;
using TeslaSQL.Agents;
using System.Diagnostics;
using MySql.Data.MySqlClient;

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
            int commandTimeout = timeout ?? Config.QueryTimeout;
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
            T toRet;
            try {
                toRet = (T)result.Rows[0][0];
            } catch (InvalidCastException) {
                throw new Exception("Unable to cast value " + result.Rows[0][0].ToString() + " to type " + typeof(T) +
                    " when running query: " + ParseCommand(cmd));
            }

            return toRet;
        }

        /// <summary>
        /// Runs a query that does not return results (i.e. a write operation)
        /// </summary>
        /// <param name="dbName">Database name</param>
        /// <param name="cmd">SqlCommand to run</param>
        /// <param name="timeout">Timeout (higher than selects since some writes can be large)</param>
        /// <returns>The number of rows affected</returns>
        internal int SqlNonQuery(string dbName, SqlCommand cmd, int? timeout = null) {
            int commandTimeout = timeout ?? Config.QueryTimeout;
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
            logger.Log("Executing query: " + ParseCommand(cmd), LogLevel.Debug);
        }

        /// <summary>
        /// Parse a SQL query, substituting parameters for their values.
        /// </summary>
        private string ParseCommand(SqlCommand cmd) {
            string query = cmd.CommandText;

            foreach (SqlParameter p in cmd.Parameters) {
                query = query.Replace(p.ParameterName, "'" + p.Value.ToString() + "'");
            }

            return query;
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
                    sqlhost = Config.Master;
                    sqluser = Config.MasterUser;
                    sqlpass = (new cTripleDes().Decrypt(Config.MasterPassword));
                    break;
                case TServer.SLAVE:
                    sqlhost = Config.Slave;
                    sqluser = Config.SlaveUser;
                    sqlpass = (new cTripleDes().Decrypt(Config.SlavePassword));
                    break;
                case TServer.RELAY:
                    sqlhost = Config.RelayServer;
                    sqluser = Config.RelayUser;
                    sqlpass = (new cTripleDes().Decrypt(Config.RelayPassword));
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


        public DateTime GetLastStartTime(string dbName, Int64 CTID, int syncBitWise, AgentType type, string slaveIdentifier = null) {
            SqlCommand cmd;
            if (slaveIdentifier != null) {
                cmd = new SqlCommand(
                "select MAX(syncStartTime) as maxStart FROM dbo.tblCTSlaveVersion WITH(NOLOCK)"
                + " WHERE syncBitWise & @syncbitwise > 0 AND CTID < @CTID and slaveIdentifier = @slaveidentifier");
                cmd.Parameters.Add("@syncbitwise", SqlDbType.Int).Value = syncBitWise;
                cmd.Parameters.Add("@CTID", SqlDbType.BigInt).Value = CTID;
                cmd.Parameters.Add("@slaveidentifier", SqlDbType.VarChar, 500).Value = slaveIdentifier;
            } else {
                cmd = new SqlCommand(
                "select MAX(syncStartTime) as maxStart FROM dbo.tblCTVersion WITH(NOLOCK)"
                + " WHERE syncBitWise & @syncbitwise > 0 AND CTID < @CTID");
                cmd.Parameters.Add("@syncbitwise", SqlDbType.Int).Value = syncBitWise;
                cmd.Parameters.Add("@CTID", SqlDbType.BigInt).Value = CTID;
            }
            DataTable result = SqlQuery(dbName, cmd);
            if (result.Rows.Count < 1)
            {
                return DateTime.Now.AddDays(-1);
            }
            DateTime? lastStartTime = result.Rows[0].Field<DateTime?>("maxStart");
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
            SqlCommand cmd = new SqlCommand("SELECT ISNULL(CHANGE_TRACKING_MIN_VALID_VERSION(OBJECT_ID(@tablename)), 0)");
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
            string query = "SELECT " + table.ModifiedMasterColumnList + ", CT.SYS_CHANGE_VERSION, CT.SYS_CHANGE_OPERATION ";
            query += " INTO " + table.SchemaName + "." + table.ToCTName(ctb.CTID);
            query += " FROM CHANGETABLE(CHANGES " + sourceDB + "." + table.SchemaName + "." + table.Name + ", @startversion) CT";
            query += " LEFT OUTER JOIN " + sourceDB + "." + table.SchemaName + "." + table.Name + " P ON " + table.PkList;
            query += " WHERE (SYS_CHANGE_VERSION <= @stopversion OR SYS_CHANGE_CREATION_VERSION <= @stopversion)";
            query += " AND (SYS_CHANGE_OPERATION = 'D' OR " + table.NotNullPKList + ")";
            /*
             * This last segment works around a bug in MSSQL. if you have an identity column that is not part of the table's
             * primary key, and a delete happens on that table, it would break the above with this error:
             * "Attempting to set a non-NULL-able column's value to NULL."
             * The workaround is to add this no-op UNION ALL, which prevents SQL from putting the identity column as not-nullable
             * On the destination table, for some reason.
             */
            query += " UNION ALL SELECT " + table.SimpleColumnList + ", NULL, NULL FROM " + sourceDB + "." + table.SchemaName + "." + table.Name + " WHERE 1 = 0";

            SqlCommand cmd = new SqlCommand(query);

            cmd.Parameters.Add("@startversion", SqlDbType.BigInt).Value = (overrideStartVersion.HasValue ? overrideStartVersion.Value : ctb.SyncStartVersion);
            cmd.Parameters.Add("@stopversion", SqlDbType.BigInt).Value = ctb.SyncStopVersion;

            return SqlNonQuery(sourceCTDB, cmd, Config.QueryTimeout);
        }


        public ChangeTrackingBatch CreateCTVersion(string dbName, Int64 syncStartVersion, Int64 syncStopVersion) {
            //create new row in tblCTVersion, output the CTID
            string query = "INSERT INTO dbo.tblCTVersion (syncStartVersion, syncStopVersion, syncStartTime, syncBitWise)";
            query += " OUTPUT inserted.CTID, inserted.syncStartTime";
            query += " VALUES (@startVersion, @stopVersion, GETDATE(), 0)";

            SqlCommand cmd = new SqlCommand(query);

            cmd.Parameters.Add("@startVersion", SqlDbType.BigInt).Value = syncStartVersion;
            cmd.Parameters.Add("@stopVersion", SqlDbType.BigInt).Value = syncStopVersion;
            var res = SqlQuery(dbName, cmd);
            return new ChangeTrackingBatch(
                res.Rows[0].Field<Int64>("CTID"),
                syncStartVersion,
                syncStopVersion,
                0,
                res.Rows[0].Field<DateTime>("syncStartTime")
                );
        }

        public void CreateShardCTVersion(string dbName, Int64 CTID, long startVersion) {
            string query = "INSERT INTO dbo.tblCTVersion (ctid, syncStartVersion, syncStartTime, syncBitWise)";
            query += " VALUES (@ctid,@syncStartVersion, GETDATE(), 0)";

            SqlCommand cmd = new SqlCommand(query);

            cmd.Parameters.Add("@ctid", SqlDbType.BigInt).Value = CTID;
            cmd.Parameters.Add("@syncStartVersion", SqlDbType.BigInt).Value = startVersion;

            SqlNonQuery(dbName, cmd);
        }


        public void CreateSlaveCTVersion(string dbName, ChangeTrackingBatch ctb, string slaveIdentifier) {

            string query = "INSERT INTO dbo.tblCTSlaveVersion (CTID, slaveIdentifier, syncStartVersion, syncStopVersion, syncStartTime, syncBitWise)";
            query += " VALUES (@ctid, @slaveidentifier, @startversion, @stopversion, @starttime, @syncbitwise)";

            SqlCommand cmd = new SqlCommand(query);

            cmd.Parameters.Add("@ctid", SqlDbType.BigInt).Value = ctb.CTID;
            cmd.Parameters.Add("@slaveidentifier", SqlDbType.VarChar, 100).Value = slaveIdentifier;
            cmd.Parameters.Add("@startversion", SqlDbType.BigInt).Value = ctb.SyncStartVersion;
            cmd.Parameters.Add("@stopversion", SqlDbType.BigInt).Value = ctb.SyncStopVersion;
            cmd.Parameters.Add("@starttime", SqlDbType.DateTime).Value = ctb.SyncStartTime;
            cmd.Parameters.Add("@syncbitwise", SqlDbType.Int).Value = ctb.SyncBitWise;

            SqlNonQuery(dbName, cmd, 30);
        }


        public void CreateSchemaChangeTable(string dbName, Int64 CTID) {
            //drop the table on the relay server if it exists
            DropTableIfExists(dbName, "tblCTSchemaChange_" + Convert.ToString(CTID), "dbo");

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

            SqlNonQuery(dbName, cmd);
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
            cmd.Parameters.Add("@ddeid", SqlDbType.Int).Value = schemaChange.DdeID;
            cmd.Parameters.Add("@tablename", SqlDbType.VarChar, 500).Value = schemaChange.TableName;
            cmd.Parameters.Add("@eventtype", SqlDbType.VarChar, 50).Value = schemaChange.EventType;
            cmd.Parameters.Add("@schema", SqlDbType.VarChar, 100).Value = schemaChange.SchemaName;
            cmd.Parameters.Add("@columnname", SqlDbType.VarChar, 500).Value = schemaChange.ColumnName;
            cmd.Parameters.Add("@newcolumnname", SqlDbType.VarChar, 500).Value = schemaChange.NewColumnName;
            if (schemaChange.DataType != null) {
                cmd.Parameters.Add("@basedatatype", SqlDbType.VarChar, 100).Value = schemaChange.DataType.BaseType;
                cmd.Parameters.Add("@charactermaximumlength", SqlDbType.Int).Value = schemaChange.DataType.CharacterMaximumLength;
                cmd.Parameters.Add("@numericprecision", SqlDbType.Int).Value = schemaChange.DataType.NumericPrecision;
                cmd.Parameters.Add("@numericscale", SqlDbType.Int).Value = schemaChange.DataType.NumericScale;
            } else {
                if (schemaChange.EventType == SchemaChangeType.Add) {
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
            SqlNonQuery(dbName, cmd);
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

            SqlNonQuery(dbName, cmd);
        }

        public IEnumerable<TTable> GetTables(string dbName) {
            string sql = "SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'";
            var cmd = new SqlCommand(sql);
            var res = SqlQuery(dbName, cmd);
            var tables = new List<TTable>();
            foreach (DataRow row in res.Rows) {
                tables.Add(new TTable(row.Field<string>("TABLE_NAME"), row.Field<string>("TABLE_SCHEMA")));
            }
            return tables;
        }

        public bool CheckTableExists(string dbName, string table, string schema = "dbo") {
            var cmd = new SqlCommand(
                @"SELECT 1 as TableExists FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_NAME = @tablename AND TABLE_SCHEMA = @schema AND TABLE_TYPE = 'BASE TABLE'");
            cmd.Parameters.Add("@tablename", SqlDbType.VarChar, 500).Value = table;
            cmd.Parameters.Add("@schema", SqlDbType.VarChar, 500).Value = schema;
            var result = SqlQuery(dbName, cmd);
            return result.Rows.Count > 0 && result.Rows[0].Field<int>("TableExists") == 1;
        }


        public IEnumerable<string> GetIntersectColumnList(string dbName, string tableName1, string schema1, string tableName2, string schema2) {
            var fields1 = GetFieldList(dbName, tableName1, schema1);
            var fields2 = GetFieldList(dbName, tableName2, schema2);
            var columns1 = new List<string>();
            var columns2 = new List<string>();
            //create this so that casing changes to columns don't cause problems, just use the lowercase column name
            foreach (TColumn c in fields1) {
                columns1.Add(c.name.ToLower());
            }
            foreach (TColumn c in fields2) {
                columns2.Add(c.name.ToLower());
            }
            return columns1.Intersect(columns2);
        }

        public bool DropTableIfExists(string dbName, string table, string schema) {
            var cmd = new SqlCommand(string.Format(@"
                IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{0}' AND TABLE_SCHEMA = '{1}' AND TABLE_TYPE = 'BASE TABLE')
	                BEGIN
	                DROP TABLE [{1}].[{0}];
	                SELECT CAST(1 AS BIT) AS Existed
	                END
                ELSE
	                SELECT CAST(0 AS BIT) AS Existed", table, schema));
            return SqlQueryToScalar<bool>(dbName, cmd);
        }


        public List<TColumn> GetFieldList(string dbName, string table, string schema) {
            string sql = @"SELECT c.COLUMN_NAME, DATA_TYPE,
                CHARACTER_MAXIMUM_LENGTH, NUMERIC_SCALE, NUMERIC_PRECISION, IS_NULLABLE,
                CASE WHEN EXISTS (
	                SELECT 1 FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE cu
	                WHERE cu.COLUMN_NAME = c.COLUMN_NAME AND cu.TABLE_NAME = c.TABLE_NAME
	                AND EXISTS (
		                SELECT 1 FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
		                WHERE tc.TABLE_NAME = c.TABLE_NAME
		                AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
                        AND tc.CONSTRAINT_NAME = cu.CONSTRAINT_NAME
                        )
                    ) THEN CAST(1 AS BIT) ELSE CAST(0 AS BIT) END AS InPrimaryKey
                FROM INFORMATION_SCHEMA.COLUMNS c
                WHERE c.TABLE_NAME = @table AND c.TABLE_SCHEMA = @schema";
            var cmd = new SqlCommand(sql);
            cmd.Parameters.Add("@table", SqlDbType.VarChar, 500).Value = table;
            cmd.Parameters.Add("@schema", SqlDbType.VarChar, 500).Value = schema;
            var res = SqlQuery(dbName, cmd);
            var columns = new List<TColumn>();
            if (res.Rows.Count == 0) {
                logger.Log("Unable to get field list for " + dbName + "." + schema + "." + table + " because it does not exist", LogLevel.Debug);
                return columns;
            }
            foreach (DataRow row in res.Rows) {
                columns.Add(new TColumn(
                    row.Field<string>("COLUMN_NAME"),
                    row.Field<bool>("InPrimaryKey"),
                    DataType.ParseDataType(row),
                    //for some reason IS_NULLABLE is a varchar(3) rather than a bool or bit
                    row.Field<string>("IS_NULLABLE") == "YES" ? true : false));
            }
            return columns;
        }

        /// <summary>
        /// Overload for GetFieldList which is used by datacopy to copy changetables.
        /// </summary>
        public List<TColumn> GetFieldList(string dbName, string table, string schema, string originalTableName, IList<string> includeColumns) {
            //get actual field list on the source table
            var columns = GetFieldList(dbName, table, schema);

            if (columns.Count == 0) {
                //table doesn't exist
                throw new DoesNotExistException();
            }

            //get the table config object
            var t = Config.TableByName(originalTableName ?? table);

            //this will be null when copying a table that isn't in the config,
            //such as tblCTTableInfo or tblCTSchemaChange
            if (t == null) {
                return columns;
            }

            //only include columns in the column list if it is configured, plus the list of includeColumns
            return columns.Where(c => t.ColumnList == null
                || includeColumns.Contains(c.name, StringComparer.OrdinalIgnoreCase)
                || t.ColumnList.Contains(c.name, StringComparer.OrdinalIgnoreCase)
                ).ToList();
        }

        public Dictionary<TableConf, IList<TColumn>> GetAllFields(string dbName, Dictionary<TableConf, string> t) {
            if (t.Count == 0) {
                return new Dictionary<TableConf, IList<TColumn>>();
            }
            var placeHolders = t.Select((_, i) => "@table" + i);
            string sql = string.Format(@"SELECT c.COLUMN_NAME, c.TABLE_NAME, DATA_TYPE, 
                CHARACTER_MAXIMUM_LENGTH, NUMERIC_SCALE, NUMERIC_PRECISION, IS_NULLABLE,
                CASE WHEN EXISTS (
	                SELECT 1 FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE cu
	                WHERE cu.COLUMN_NAME = c.COLUMN_NAME AND cu.TABLE_NAME = c.TABLE_NAME
	                AND EXISTS (
		                SELECT 1 FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
		                WHERE tc.TABLE_NAME = c.TABLE_NAME
		                AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
                        AND tc.CONSTRAINT_NAME = cu.CONSTRAINT_NAME
                        )
                    ) THEN CAST(1 AS BIT) ELSE CAST(0 AS BIT) END AS InPrimaryKey
                FROM INFORMATION_SCHEMA.COLUMNS c
                WHERE c.TABLE_NAME IN ( {0} );",
                                       string.Join(",", placeHolders));
            var cmd = new SqlCommand(sql);
            foreach (var ph in placeHolders.Zip(t.Values, (ph, tn) => Tuple.Create(ph, tn))) {
                cmd.Parameters.Add(ph.Item1, SqlDbType.VarChar, 500).Value = ph.Item2;
            }
            var res = SqlQuery(dbName, cmd);
            var fields = new Dictionary<TableConf, IList<TColumn>>();
            foreach (DataRow row in res.Rows) {
                var tableName = row.Field<string>("TABLE_NAME");
                var tc = t.Keys.FirstOrDefault(table => t[table].Equals(tableName, StringComparison.OrdinalIgnoreCase));
                if (tc == null) { continue; }
                if (!fields.ContainsKey(tc)) {
                    fields[tc] = new List<TColumn>();
                }
                fields[tc].Add(new TColumn(
                    row.Field<string>("COLUMN_NAME"),
                    row.Field<bool>("InPrimaryKey"),
                    DataType.ParseDataType(row),
                    //for some reason IS_NULLABLE is a varchar(3) rather than a bool or bit
                    row.Field<string>("IS_NULLABLE") == "YES" ? true : false));
            }
            return fields;
        }


        public void WriteBitWise(string dbName, Int64 CTID, int value, AgentType agentType) {
            string query;
            SqlCommand cmd;
            if (agentType.Equals(AgentType.Slave)) {
                query = "UPDATE dbo.tblCTSlaveVersion SET SyncBitWise = SyncBitWise + @syncbitwise";
                query += " WHERE slaveIdentifier = @slaveidentifier AND CTID = @ctid AND SyncBitWise & @syncbitwise = 0";
                cmd = new SqlCommand(query);
                cmd.Parameters.Add("@syncbitwise", SqlDbType.Int).Value = value;
                cmd.Parameters.Add("@slaveidentifier", SqlDbType.VarChar, 100).Value = Config.Slave;
                cmd.Parameters.Add("@ctid", SqlDbType.BigInt).Value = CTID;
            } else {
                query = "UPDATE dbo.tblCTVersion SET SyncBitWise = SyncBitWise + @syncbitwise";
                query += " WHERE CTID = @ctid AND SyncBitWise & @syncbitwise = 0";
                cmd = new SqlCommand(query);
                cmd.Parameters.Add("@syncbitwise", SqlDbType.Int).Value = value;
                cmd.Parameters.Add("@ctid", SqlDbType.BigInt).Value = CTID;
            }
            SqlNonQuery(dbName, cmd);
        }


        public int ReadBitWise(string dbName, Int64 CTID, AgentType agentType) {
            string query;
            SqlCommand cmd;
            if (agentType.Equals(AgentType.Slave)) {
                query = "SELECT syncBitWise from dbo.tblCTSlaveVersion WITH(NOLOCK)";
                query += " WHERE slaveIdentifier = @slaveidentifier AND CTID = @ctid";
                cmd = new SqlCommand(query);
                cmd.Parameters.Add("@slaveidentifier", SqlDbType.VarChar, 100).Value = Config.Slave;
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
            SqlCommand cmd;
            Enum.GetValues(typeof(SyncBitWise)).Cast<int>().Sum();
            string query = "UPDATE dbo.tblCTSlaveVersion SET syncBitWise = @syncbitwise, syncStopTime = @syncstoptime";
            query += " WHERE slaveIdentifier = @slaveidentifier AND CTID = @ctid";
            cmd = new SqlCommand(query);
            cmd.Parameters.Add("@syncbitwise", SqlDbType.Int).Value = Enum.GetValues(typeof(SyncBitWise)).Cast<int>().Sum();
            cmd.Parameters.Add("@syncstoptime", SqlDbType.DateTime).Value = syncStopTime;
            cmd.Parameters.Add("@slaveidentifier", SqlDbType.VarChar, 100).Value = slaveIdentifier;
            cmd.Parameters.Add("@ctid", SqlDbType.BigInt).Value = CTID;
            SqlNonQuery(dbName, cmd);
        }


        public void MarkBatchesComplete(string dbName, IEnumerable<long> CTIDs, DateTime syncStopTime, string slaveIdentifier) {
            var inParams = CTIDs.Select((CTID, i) => "@ctid" + i);
            Enum.GetValues(typeof(SyncBitWise)).Cast<int>().Sum();
            string query = string.Format(@"UPDATE dbo.tblCTSlaveVersion SET syncBitWise = @syncbitwise, syncStopTime = @syncstoptime
                      WHERE slaveIdentifier = @slaveidentifier AND CTID IN ({0})",
                                   string.Join(",", inParams));
            SqlCommand cmd = new SqlCommand(query);
            cmd.Parameters.Add("@syncbitwise", SqlDbType.Int).Value = Enum.GetValues(typeof(SyncBitWise)).Cast<int>().Sum();
            cmd.Parameters.Add("@syncstoptime", SqlDbType.DateTime).Value = syncStopTime;
            cmd.Parameters.Add("@slaveidentifier", SqlDbType.VarChar, 100).Value = slaveIdentifier;
            foreach (var pair in CTIDs.Zip(inParams, (CTID, inp) => Tuple.Create(inp, CTID))) {
                cmd.Parameters.Add(pair.Item1, SqlDbType.BigInt).Value = pair.Item2;
            }
            SqlNonQuery(dbName, cmd);
        }

        public DataTable GetSchemaChanges(string dbName, Int64 CTID) {
            SqlCommand cmd = new SqlCommand("SELECT CscID, CscDdeID, CscTableName, CscEventType, CscSchema, CscColumnName" +
             ", CscNewColumnName, CscBaseDataType, CscCharacterMaximumLength, CscNumericPrecision, CscNumericScale" +
             " FROM dbo.tblCTSchemaChange_" + Convert.ToString(CTID));
            return SqlQuery(dbName, cmd);
        }


        public Int64 GetTableRowCount(string dbName, string table, string schema) {
            var cmd = new SqlCommand(string.Format("SELECT COUNT_BIG(*) FROM [{0}].[{1}]", schema, table));
            return SqlQueryToScalar<long>(dbName, cmd);
        }

        public bool IsChangeTrackingEnabled(string dbName, string table, string schema) {
            var cmd = new SqlCommand(string.Format(@"
                SELECT 1 as IsEnabled FROM sys.change_tracking_tables WHERE OBJECT_ID = OBJECT_ID('{0}.{1}')",
                schema, table));
            var result = SqlQuery(dbName, cmd);
            return result.Rows.Count > 0 && result.Rows[0].Field<int>("IsEnabled") == 1;
        }

        public void LogError(string message, string headers) {
            SqlCommand cmd = new SqlCommand("INSERT INTO tblCTError (CelError, CelHeaders) VALUES ( @error, @headers )");
            cmd.Parameters.Add("@error", SqlDbType.VarChar, -1).Value = message;
            cmd.Parameters.Add("@headers", SqlDbType.VarChar, -1).Value = headers;
            SqlNonQuery(Config.ErrorLogDB, cmd);
        }

        public IEnumerable<TError> GetUnsentErrors() {
            SqlCommand cmd = new SqlCommand("SELECT CelError, CelId, CelHeaders, CelLogDate FROM tblCTError WHERE CelSent = 0");
            var res = SqlQuery(Config.ErrorLogDB, cmd);
            var errors = new List<TError>();
            foreach (DataRow row in res.Rows) {
                errors.Add(new TError(row.Field<string>("CelHeaders"), row.Field<string>("CelError"), row.Field<DateTime>("CelLogDate"), row.Field<int>("CelId")));
            }
            return errors;
        }

        public void MarkErrorsSent(IEnumerable<int> celIds) {
            SqlCommand cmd = new SqlCommand("UPDATE tblCTError SET CelSent = 1 WHERE CelId IN (" + string.Join(",", celIds) + ")");
            SqlNonQuery(Config.ErrorLogDB, cmd);
        }

        private bool CheckColumnExists(string dbName, string schema, string table, string column) {
            var cmd = new SqlCommand(@"
            SELECT 1 as ColumnExists FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_NAME = @table AND TABLE_SCHEMA = @schema AND COLUMN_NAME = @column");
            cmd.Parameters.Add("@table", SqlDbType.VarChar, 500).Value = table;
            cmd.Parameters.Add("@schema", SqlDbType.VarChar, 500).Value = schema;
            cmd.Parameters.Add("@column", SqlDbType.VarChar, 500).Value = column;
            var result = SqlQuery(dbName, cmd);
            return result.Rows.Count > 0 && result.Rows[0].Field<int>("ColumnExists") == 1;
        }

        public void RenameColumn(TableConf t, string dbName, string columnName, string newColumnName, string historyDB) {
            SqlCommand cmd;
            //rename the column if it exists
            if (CheckColumnExists(dbName, t.SchemaName, t.Name, columnName)) {
                cmd = new SqlCommand("EXEC sp_rename @objname, @newname, 'COLUMN'");
                cmd.Parameters.Add("@objname", SqlDbType.VarChar, 500).Value = t.FullName + "." + columnName;
                cmd.Parameters.Add("@newname", SqlDbType.VarChar, 500).Value = newColumnName;
                logger.Log("Altering table with command: " + cmd.CommandText, LogLevel.Debug);
                SqlNonQuery(dbName, cmd);
            }
            //check for history table, if it is configured and contains the column we need to modify that too
            if (t.RecordHistoryTable && CheckTableExists(historyDB, t.HistoryName, t.SchemaName) && CheckColumnExists(historyDB, t.SchemaName, t.HistoryName, columnName)) {
                cmd = new SqlCommand("EXEC sp_rename @objname, @newname, 'COLUMN'");
                cmd.Parameters.Add("@objname", SqlDbType.VarChar, 500).Value = t.SchemaName + "." + t.HistoryName + "." + columnName;
                cmd.Parameters.Add("@newname", SqlDbType.VarChar, 500).Value = newColumnName;
                logger.Log("Altering history table column with command: " + cmd.CommandText, LogLevel.Debug);
                SqlNonQuery(historyDB, cmd);
            }
        }

        public void ModifyColumn(TableConf t, string dbName, string columnName, string dataType, string historyDB) {
            SqlCommand cmd;
            //Modify the column if it exists
            if (CheckColumnExists(dbName, t.SchemaName, t.Name, columnName)) {
                cmd = new SqlCommand("ALTER TABLE " + t.FullName + " ALTER COLUMN " + columnName + " " + dataType);
                logger.Log("Altering table column with command: " + cmd.CommandText, LogLevel.Debug);
                SqlNonQuery(dbName, cmd);
            }
            //modify on history table if that exists too
            if (t.RecordHistoryTable && CheckTableExists(historyDB, t.HistoryName, t.SchemaName) && CheckColumnExists(historyDB, t.SchemaName, t.HistoryName, columnName)) {
                cmd = new SqlCommand("ALTER TABLE " + t.SchemaName + "." + t.HistoryName + " ALTER COLUMN " + columnName + " " + dataType);
                logger.Log("Altering history table column with command: " + cmd.CommandText, LogLevel.Debug);
                SqlNonQuery(historyDB, cmd);
            }
        }

        public void AddColumn(TableConf t, string dbName, string columnName, string dataType, string historyDB) {
            SqlCommand cmd;
            //add column if it doesn't exist
            if (!CheckColumnExists(dbName, t.SchemaName, t.Name, columnName)) {
                cmd = new SqlCommand("ALTER TABLE " + t.FullName + " ADD " + columnName + " " + dataType);
                logger.Log("Altering table with command: " + cmd.CommandText, LogLevel.Debug);
                SqlNonQuery(dbName, cmd);
            }
            //add column to history table if the table exists and the column doesn't
            if (t.RecordHistoryTable && CheckTableExists(historyDB, t.HistoryName, t.SchemaName) && !CheckColumnExists(historyDB, t.SchemaName, t.HistoryName, columnName)) {
                cmd = new SqlCommand("ALTER TABLE " + t.SchemaName + "." + t.HistoryName + " ADD " + columnName + " " + dataType);
                logger.Log("Altering history table column with command: " + cmd.CommandText, LogLevel.Debug);
                SqlNonQuery(historyDB, cmd);
            }
        }

        public void DropColumn(TableConf t, string dbName, string columnName, string historyDB) {
            SqlCommand cmd;
            //drop column if it exists
            if (CheckColumnExists(dbName, t.SchemaName, t.Name, columnName)) {
                cmd = new SqlCommand("ALTER TABLE " + t.FullName + " DROP COLUMN " + columnName);
                logger.Log("Altering table with command: " + cmd.CommandText, LogLevel.Debug);
                SqlNonQuery(dbName, cmd);
            }
            //if history table exists and column exists, drop it there too
            if (t.RecordHistoryTable && CheckTableExists(historyDB, t.HistoryName, t.SchemaName) && CheckColumnExists(historyDB, t.SchemaName, t.HistoryName, columnName)) {
                cmd = new SqlCommand("ALTER TABLE " + t.SchemaName + "." + t.HistoryName + " DROP COLUMN " + columnName);
                logger.Log("Altering history table column with command: " + cmd.CommandText, LogLevel.Debug);
                SqlNonQuery(historyDB, cmd);
            }
        }

        public void CreateTableInfoTable(string dbName, Int64 CTID) {
            //drop the table on the relay server if it exists
            DropTableIfExists(dbName, "tblCTTableInfo_" + CTID, "dbo");

            //can't parametrize the CTID since it's part of a table name, but it is an Int64 so it's not an injection risk
            string query = "CREATE TABLE [dbo].[tblCTTableInfo_" + CTID + "] (";
            query += @"
            [CtiID] [int] NOT NULL IDENTITY(1,1) PRIMARY KEY,
	        [CtiTableName] [varchar](500) NOT NULL,
            [CtiSchemaName] [varchar](100) NOT NULL,
            [CtiPKList] [varchar](500) NOT NULL,
            [CtiExpectedRows] [int] NOT NULL,
            [CtiInsertCount] [int] NOT NULL
            )";

            SqlCommand cmd = new SqlCommand(query);

            SqlNonQuery(dbName, cmd);
        }


        public void PublishTableInfo(string dbName, TableConf t, long CTID, long expectedRows) {
            String sql;
            if (expectedRows > 0) {
                sql = String.Format(@"INSERT INTO tblCTTableInfo_{0} (CtiTableName, CtiSchemaName, CtiPKList, CtiExpectedRows, CtiInsertCount)
                               VALUES (@tableName, @schemaName, @pkList, @expectedRows,
                                ( SELECT COUNT(*) FROM {1} WHERE SYS_CHANGE_OPERATION IN ('I', 'U') )
                               )", CTID, t.ToCTName(CTID));
            } else {
                sql = String.Format(@"INSERT INTO tblCTTableInfo_{0} (CtiTableName, CtiSchemaName, CtiPKList, CtiExpectedRows, CtiInsertCount)
                               VALUES (@tableName, @schemaName, @pkList, @expectedRows, 0)", CTID, t.ToCTName(CTID));
            }
            SqlCommand cmd = new SqlCommand(sql);
            cmd.Parameters.Add("@tableName", SqlDbType.VarChar, 500).Value = t.Name;
            cmd.Parameters.Add("@schemaName", SqlDbType.VarChar, 500).Value = t.SchemaName;
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
        /// <param name="reader">DataReader object to stream input from</param>
        /// <param name="dbName">Database name</param>
        /// <param name="schema">Schema of the table to write to</param>
        /// <param name="table">Table name to write to</param>
        /// <param name="timeout">Timeout</param>
        public void BulkCopy(IDataReader reader, string dbName, string schema, string table, int timeout) {
            SqlBulkCopy bulkCopy = new SqlBulkCopy(buildConnString(dbName), SqlBulkCopyOptions.KeepIdentity);
            bulkCopy.BulkCopyTimeout = timeout;
            bulkCopy.DestinationTableName = schema + ".[" + table + "]";
            bulkCopy.NotifyAfter = 1000;
            var sw = Stopwatch.StartNew();
            bulkCopy.SqlRowsCopied += new SqlRowsCopiedEventHandler((s, e) => logger.Log("Copied " + e.RowsCopied + " rows so far, in " + sw.Elapsed, LogLevel.Debug));
            bulkCopy.WriteToServer(reader);
        }

        public RowCounts ApplyTableChanges(TableConf table, TableConf archiveTable, string dbName, Int64 CTID, string CTDBName, bool isConsolidated) {
            var tableSql = new List<SqlCommand>();
            tableSql.Add(BuildMergeQuery(table, dbName, CTID, CTDBName, isConsolidated));
            if (archiveTable != null) {
                tableSql.Add(BuildMergeQuery(archiveTable, dbName, CTID, CTDBName, isConsolidated));
            }
            var s = TransactionQuery(tableSql, dbName, Config.QueryTimeout);
            int inserted = s[0].Rows[0].Field<int>("insertcount");
            int deleted = s[0].Rows[0].Field<int>("deletecount");
            logger.Log("table " + table.Name + ": insert: " + inserted + " | delete: " + deleted, LogLevel.Info);
            var rowCounts = new RowCounts(inserted, deleted);
            if (archiveTable != null) {
                inserted = s[1].Rows[0].Field<int>("insertcount");
                deleted = s[1].Rows[0].Field<int>("deletecount");
                logger.Log("table " + archiveTable.Name + ": insert: " + inserted + " | delete: " + deleted, LogLevel.Info);
                rowCounts = new RowCounts(rowCounts.Inserted + inserted, rowCounts.Deleted + deleted);
            }
            return rowCounts;
        }

        private SqlCommand BuildMergeQuery(TableConf table, string dbName, Int64 CTID, string CTDBName, bool isConsolidated) {
            string CtTableName;
            if (isConsolidated && Config.Slave == Config.RelayServer && CTDBName == Config.RelayDB) {
                //special case - consolidated batch where Relay == Slave
                CtTableName = "[" + table.SchemaName + "].[tblCT" + table.Name + "_" + Config.Slave + "]";
            } else {
                CtTableName = table.ToFullCTName(CTID);
            }
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
                          table.Name,
                          CtTableName,
                          table.PkList,
                          table.MergeUpdateList.Length > 2 ? table.MergeUpdateList : table.PkList.Replace("AND", ","),
                          table.SimpleColumnList,
                          table.MasterColumnList.Replace("P.", "CT."),
                          table.SchemaName,
                          CTDBName
                          );
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
        /// Scripts out a table as CREATE TABLE, respecting column lists defined for that table
        /// </summary>
        /// <param name="dbName">Database name</param>
        /// <param name="table">Table name</param>
        /// <param name="schema">Table's schema</param>\
        /// <param name="originalTableName">Original table to pull TableConf for</param>
        /// <param name="flavor">SQL type of the table the script will be run on</param>
        /// <returns>The CREATE TABLE script as a string</returns>
        public string ScriptTable(string dbName, string table, string schema, string originalTableName, SqlFlavor flavor = SqlFlavor.MSSQL) {
            //get actual field list on the source table
            var includeColumns = new List<string>() { "SYS_CHANGE_VERSION", "SYS_CHANGE_OPERATION" };
            List<TColumn> columns = GetFieldList(dbName, table, schema, originalTableName, includeColumns);
            switch (flavor) 
            {
                case SqlFlavor.MSSQL:
                    return string.Format(
                        @"CREATE TABLE [{0}].[{1}] (
                            {2}
                        );", schema, table, string.Join(",", columns.Select(c => c.ToExpression())));
                case SqlFlavor.MySQL:
                    foreach (TColumn column in columns)
                    {
                        column.dataType = new DataType(DataType.MapDataType(SqlFlavor.MSSQL, SqlFlavor.MySQL, column.dataType.BaseType), column.dataType.CharacterMaximumLength, column.dataType.NumericPrecision, column.dataType.NumericScale);
                        if (column.dataType.UsesMaxLength() && column.dataType.CharacterMaximumLength == -1)
                        {
                            column.dataType = new DataType("longtext");
                        }
                    }
                    var ctid = table.Split('_').Last();
                    TableConf fakeTableConf = new TableConf();
                    fakeTableConf.Name = originalTableName;
                    string pks = String.Join(",",GetPrimaryKeysFromInfoTable(fakeTableConf, Convert.ToInt64(ctid), dbName));
                    return string.Format("CREATE TABLE {0}.{1}({2}, PRIMARY KEY ({3}));", dbName, table, string.Join(",", columns.Select(c => c.ToExpression(SqlFlavor.MySQL))), pks);
                default:
                    throw new NotImplementedException("No scripting rules defined for " + flavor.ToString());
            }
        }

        public void Consolidate(string ctTableName, string consolidatedTableName, string dbName, string schemaName) {
            var columns = GetIntersectColumnList(dbName, ctTableName, schemaName, consolidatedTableName, schemaName);
            var query = string.Format(
                "INSERT INTO [{0}] ({1}) SELECT {1} FROM [{2}]",
                consolidatedTableName, string.Join(",", columns), ctTableName);
            var cmd = new SqlCommand(query);
            SqlNonQuery(dbName, cmd);
        }

        public void RemoveDuplicatePrimaryKeyChangeRows(TableConf table, string consolidatedTableName, string dbName) {
            var pks = table.columns.Where(c => c.isPk);
            var zipped = pks.Zip(pks, (a, b) => "a." + a + " = b." + b);
            string whereCondition = string.Join(" AND ", zipped);

            //it's possible we would get two rows with the same SYS_CHANGE_VERSION in two separate CTIDs, so we add this
            //identity column to guarantee they won't stick around
            string alter = string.Format("ALTER TABLE [{0}] ADD [Tesla_Unique_ID] BIGINT IDENTITY(1,1) NOT NULL", consolidatedTableName);
            SqlNonQuery(dbName, new SqlCommand(alter));

            //dedupe
            string delete = string.Format(
                            @"DELETE a FROM [{0}] a 
                              WHERE EXISTS (
                                SELECT 1 FROM [{0}] b WHERE {1} AND (a.sys_change_version < b.sys_change_version
                                OR (a.sys_change_version = b.sys_change_version AND a.[Tesla_Unique_ID] < b.[Tesla_Unique_ID]))                                
                              ) ",
                              consolidatedTableName, whereCondition);
            SqlNonQuery(dbName, new SqlCommand(delete));

            //now remove the column so it doesn't end up on slaves
            alter = string.Format("ALTER TABLE [{0}] DROP COLUMN [Tesla_Unique_ID]", consolidatedTableName);
            SqlNonQuery(dbName, new SqlCommand(alter));
        }


        public DataTable GetPendingCTSlaveVersions(string dbName, string slaveIdentifier, int bitwise) {
            string query = @"SELECT * FROM tblCTSlaveVersion
                            WHERE slaveIdentifier = @slaveidentifier AND CTID >
                            (
                            	SELECT MAX(ctid) FROM tblCTSlaveVersion WHERE slaveIdentifier = @slaveidentifier AND syncBitWise = @bitwise
                            ) ORDER BY CTID";
            SqlCommand cmd = new SqlCommand(query);
            cmd.Parameters.Add("@bitwise", SqlDbType.Int).Value = bitwise;
            cmd.Parameters.Add("@slaveidentifier", SqlDbType.VarChar, 500).Value = slaveIdentifier;
            logger.Log("Running query: " + cmd.CommandText + "... slaveidentifiers is " + slaveIdentifier, LogLevel.Debug);
            return SqlQuery(dbName, cmd);
        }

        public void CopyIntoHistoryTable(ChangeTable t, string slaveCTDB, bool isConsolidated) {
            string sql;
            string sourceTable;
            List<TColumn> fields;
            if (isConsolidated && Config.Slave == Config.RelayServer && slaveCTDB == Config.RelayDB) {
                sourceTable = "[" + t.schemaName + "].[" + t.consolidatedName + "]";
                fields = GetFieldList(slaveCTDB, t.consolidatedName, t.schemaName);
            } else {
                sourceTable = t.ctName;
                fields = GetFieldList(slaveCTDB, t.ctName, t.schemaName);
            }

            string insertColumns = "CTHistID, " + string.Join(",", fields.Select(col => col.name));
            string selectColumns = "CAST(" + t.CTID + " AS BIGINT) AS CTHistID, " + string.Join(",", fields.Select(col => col.name));

            if (CheckTableExists(slaveCTDB, t.historyName, t.schemaName)) {
                logger.Log("table " + t.historyName + " already exists; selecting into it", LogLevel.Trace);
                sql = string.Format("INSERT INTO {0} ({1}) SELECT {2} FROM {3}", t.historyName, insertColumns, selectColumns, sourceTable);
                logger.Log(sql, LogLevel.Debug);
            } else {
                logger.Log("table " + t.historyName + " does not exist, inserting into it", LogLevel.Trace);
                sql = string.Format("SELECT {0} INTO {1} FROM {2}", selectColumns, t.historyName, sourceTable);
                logger.Log(sql, LogLevel.Debug);
            }
            var cmd = new SqlCommand(sql);
            SqlNonQuery(slaveCTDB, cmd);
        }


        public ChangeTrackingBatch GetCTBatch(string dbName, Int64 CTID) {
            SqlCommand cmd;
            cmd = new SqlCommand("SELECT TOP 1 CTID, syncStartVersion, syncStopVersion, syncBitWise FROM dbo.tblCTVersion WHERE ctid = @ctid");
            cmd.Parameters.Add("@ctid", SqlDbType.BigInt).Value = CTID;
            DataTable result = SqlQuery(dbName, cmd);
            return result.Rows.Count > 0 ? new ChangeTrackingBatch(result.Rows[0]) : null;
        }

        public void RevertCTBatch(string dbName, Int64 CTID) {
            SqlCommand cmd;
            cmd = new SqlCommand("UPDATE dbo.tblCTVersion SET SyncBitWise = 0 WHERE ctid = @ctid");
            cmd.Parameters.Add("@ctid", SqlDbType.BigInt).Value = CTID;
            SqlNonQuery(dbName, cmd);
        }


        public void MergeCTTable(TableConf table, string destDB, string sourceDB, long CTID) {
            var mergeList = table.columns.Any(c => !c.isPk) ? table.MergeUpdateList : string.Join(",", table.columns.Select(c => String.Format("P.{0} = CT.{0}", c.name)));
            var columnList = string.Join(",", table.columns.Select(c => c.name));
            var insertList = string.Join(",", table.columns.Select(c => "CT." + c.name));
            //the logic here is that if the same primary key changed on multiple shards, we take the first row we find for that PK.
            //however, we prefer inserts/updates over deletes since we don't want to lose data if a record moves from one shard to another.
            string sql =
                string.Format(@"MERGE dbo.{0} WITH(ROWLOCK) AS P
	               USING (SELECT * FROM {1}.dbo.{0}) AS CT
	               ON ({2})
	               WHEN MATCHED AND P.SYS_CHANGE_OPERATION = 'D' AND CT.SYS_CHANGE_OPERATION IN ('I', 'U')
	                 THEN UPDATE SET {3}
	               WHEN NOT MATCHED
	                 THEN INSERT ({4}) VALUES ({5});",
                   table.ToCTName(CTID), sourceDB, table.PkList, mergeList, columnList, insertList);
            SqlCommand cmd = new SqlCommand(sql);
            SqlNonQuery(destDB, cmd);
        }

        public IEnumerable<string> GetPrimaryKeysFromInfoTable(TableConf table, long CTID, string database) {
            string sql = string.Format(@"SELECT CtipkList FROM tblCTTableInfo_{0} WHERE CtiTableName = @tableName", CTID);
            SqlCommand cmd = new SqlCommand(sql);
            cmd.Parameters.Add("@tableName", SqlDbType.VarChar, 5000).Value = table.Name;
            var res = SqlQueryToScalar<string>(database, cmd);
            return res.Split(new char[] { ',' });
        }

        public Dictionary<TableConf, IList<string>> GetAllPrimaryKeys(string dbName, IEnumerable<TableConf> tables, ChangeTrackingBatch batch) {
            if (tables.Count() == 0) {
                return new Dictionary<TableConf, IList<string>>();
            }
            var placeHolders = tables.Select((t, i) => "@table" + i);
            string sql = string.Format("SELECT CtiTableName, CtipkList FROM tblCTTableInfo_{0} WHERE CtiTableName IN ( {1} )",
                           batch.CTID, string.Join(",", placeHolders));
            var cmd = new SqlCommand(sql);
            foreach (var pht in placeHolders.Zip(tables, (ph, t) => Tuple.Create(ph, t.Name))) {
                cmd.Parameters.Add(pht.Item1, SqlDbType.VarChar, 500).Value = pht.Item2;
            }
            var res = SqlQuery(dbName, cmd);
            var tablePks = new Dictionary<TableConf, IList<string>>();
            foreach (DataRow row in res.Rows) {
                var tableName = row.Field<string>("CtiTableName");
                var table = tables.FirstOrDefault(t => t.Name.Equals(tableName, StringComparison.OrdinalIgnoreCase));
                var pks = row.Field<string>("CtipkList").Split(new char[] { ',' });
                tablePks[table] = pks;
            }
            return tablePks;
        }

        public Dictionary<TableConf, IEnumerable<string>> GetAllPrimaryKeysMaster(string database, IEnumerable<TableConf> tableConfs) {
            if (tableConfs.Count() == 0) {
                return new Dictionary<TableConf, IEnumerable<string>>();
            }
            var placeHolders = tableConfs.Select((t, i) => "@table" + i);
            string sql = String.Format(
                @"  SELECT cu.CONSTRAINT_NAME, cu.COLUMN_NAME, cu.TABLE_NAME
                    FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE cu 
                    WHERE EXISTS ( 
	                  SELECT tc.* FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc 
	                  WHERE tc.TABLE_NAME IN ({0})
	                  AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY' 
	                  AND tc.CONSTRAINT_NAME = cu.CONSTRAINT_NAME 
                    )", string.Join(",", placeHolders));
            var cmd = new SqlCommand(sql);
            foreach (var pht in placeHolders.Zip(tableConfs, (ph, t) => Tuple.Create(ph, t.Name))) {
                cmd.Parameters.Add(pht.Item1, SqlDbType.VarChar, 500).Value = pht.Item2;
            }
            var res = SqlQuery(database, cmd);
            var tablePks = new Dictionary<TableConf, IList<string>>();
            foreach (DataRow row in res.Rows) {
                var tableName = row.Field<string>("TABLE_NAME");
                var table = tableConfs.FirstOrDefault(t => t.Name.Equals(tableName, StringComparison.OrdinalIgnoreCase));
                var pk = row.Field<string>("COLUMN_NAME");
                if (!tablePks.ContainsKey(table)) {
                    tablePks[table] = new List<string>();
                }
                tablePks[table].Add(pk);
            }
            return tablePks.ToDictionary(t => t.Key, t => t.Value.AsEnumerable());
        }

        public void RecreateView(string dbName, string viewName, string viewSelect) {
            var cmd = new SqlCommand(string.Format(
                @"IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES 
                    WHERE TABLE_NAME = '{0}' AND TABLE_TYPE = 'VIEW')
	            DROP VIEW {0};", viewName));
            SqlNonQuery(dbName, cmd);
            cmd = new SqlCommand(string.Format("CREATE VIEW {0} AS {1}", viewName, viewSelect));
            SqlNonQuery(dbName, cmd);
        }

        public int GetExpectedRowCounts(string ctDbName, long CTID) {
            string sql = string.Format("SELECT ISNULL(SUM(CtiInsertCount), 0) FROM tblCTTableInfo_{0};", CTID);
            var cmd = new SqlCommand(sql);
            return SqlQueryToScalar<int>(ctDbName, cmd);
        }

        public IEnumerable<long> GetOldCTIDsMaster(string dbName, DateTime chopDate) {
            string sql = "SELECT ctid FROM [dbo].[tblCTVersion] WHERE syncStartTime < @chopDate";
            var cmd = new SqlCommand(sql);
            cmd.Parameters.Add("@chopDate", SqlDbType.DateTime).Value = chopDate;
            var res = SqlQuery(dbName, cmd);
            var CTIDs = new List<long>();
            foreach (DataRow row in res.Rows) {
                CTIDs.Add(row.Field<long>("ctid"));
            }
            return CTIDs;
        }
        public IEnumerable<long> GetOldCTIDsRelay(string dbName, DateTime chopDate) {
            string sql = @"SELECT ctid, MAX(syncstoptime) AS maxstop
                           FROM [dbo].[tblCTSlaveVersion]
                           GROUP BY CTID
                           HAVING MAX(syncstoptime) < @chopDate";
            var cmd = new SqlCommand(sql);
            cmd.Parameters.Add("@chopDate", SqlDbType.DateTime).Value = chopDate;
            var res = SqlQuery(dbName, cmd);
            var CTIDs = new List<long>();
            foreach (DataRow row in res.Rows) {
                CTIDs.Add(row.Field<long>("ctid"));
            }
            return CTIDs;
        }

        public IEnumerable<long> GetOldCTIDsSlave(string dbName, DateTime chopDate, string slaveIdentifier) {
            string sql = @"SELECT ctid 
                           FROM [dbo].[tblCTSlaveVersion]
                           WHERE slaveIdentifier = @slaveIdentifier
                           AND syncstoptime < @chopDate";
            var cmd = new SqlCommand(sql);
            cmd.Parameters.Add("@slaveIdentifier", SqlDbType.VarChar, 500).Value = slaveIdentifier;
            cmd.Parameters.Add("@chopDate", SqlDbType.DateTime).Value = chopDate;
            var res = SqlQuery(dbName, cmd);
            var CTIDs = new List<long>();
            foreach (DataRow row in res.Rows) {
                CTIDs.Add(row.Field<long>("ctid"));
            }
            return CTIDs;
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
            cmd.Parameters.Add("@tableName", SqlDbType.VarChar, 500).Value = table.Name;
            var res = SqlQuery(sourceCTDB, cmd);
            return res.Rows.Count > 0;
        }

        public long? GetInitializeStartVersion(string sourceCTDB, TableConf table) {
            string sql = @"SELECT nextSynchVersion FROM tblCTInitialize WHERE tableName = @tableName";
            var cmd = new SqlCommand(sql);
            cmd.Parameters.Add("@tableName", SqlDbType.VarChar, 500).Value = table.Name;
            var res = SqlQuery(sourceCTDB, cmd);
            if (res.Rows.Count == 0) {
                return null;
            } else {
                return res.Rows[0].Field<long>("nextSynchVersion");
            }
        }

        public void CleanUpInitializeTable(string dbName, DateTime syncStartTime) {
            string sql = @"DELETE FROM tblCTInitialize
                           WHERE inProgress = 0
                           AND iniFinishTime < @syncStartTime";
            var cmd = new SqlCommand(sql);
            cmd.Parameters.Add("@syncStartTime", SqlDbType.DateTime).Value = syncStartTime;
            SqlNonQuery(dbName, cmd);
        }

        public DataTable GetTablesWithChanges(string dbName, IList<ChangeTrackingBatch> batches) {
            string query = "";
            foreach (var batch in batches) {
                if (query.Length > 0) {
                    query += "\r\nUNION ALL\r\n";
                }
                query += string.Format("SELECT CAST({0} AS BIGINT) AS CTID, CtiTableName, CtiSchemaName FROM dbo.tblCTTableInfo_{0} WITH(NOLOCK) WHERE CtiExpectedRows > 0"
                    , batch.CTID);
            }
            var cmd = new SqlCommand(query);
            return SqlQuery(dbName, cmd);
        }

        public void MergeInfoTable(string shardDB, string consolidatedDB, long CTID) {
            //merge rows into the consolidated info table. this is used for sharded agents.
            //the shardcoordinator will only publish the rows for tables that actually had changes
            //but then we pull in the others so that slaves get the PK list, which ends up getting used
            //for consolidated batches on the slaves.
            string sql = string.Format(@"INSERT INTO [dbo].[tblCTtableInfo_{1}] (CtiTableName, CtiSchemaName, CtiPKList, CtiExpectedRows, CtiInsertCount)
            SELECT CtiTableName, CtiSchemaName, CtiPKList, CtiExpectedRows, CtiInsertCount
            FROM [{0}].[dbo].[tblCTtableInfo_{1}] i
            WHERE NOT EXISTS
	            (SELECT 1 FROM [dbo].[tblCTtableInfo_{1}]
	            WHERE CtiTableName = i.CtiTableName AND CtiSchemaName = i.CtiSchemaName)",
               shardDB, CTID);
            var cmd = new SqlCommand(sql);
            SqlNonQuery(consolidatedDB, cmd);
        }
        
        public IEnumerable<long> GetOldStopSyncMaster(string dbName, DateTime chopDate)
        {
            string sql = "SELECT syncStopVersion AS ctid FROM [dbo].[tblCTVersion] WHERE syncStartTime < @chopDate";
            var cmd = new SqlCommand(sql);
            cmd.Parameters.Add("@chopDate", SqlDbType.DateTime).Value = chopDate;
            var res = SqlQuery(dbName, cmd);
            var CTIDs = new List<long>();
            foreach (DataRow row in res.Rows)
            {
                CTIDs.Add(row.Field<long>("ctid"));
            }
            return CTIDs;
        }

    }
}
