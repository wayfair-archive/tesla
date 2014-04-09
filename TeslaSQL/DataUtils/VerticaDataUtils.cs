using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using System.Text;
using System.Data.SqlClient;
using System.Data;
using System.Text.RegularExpressions;
using Vertica.Data.VerticaClient;

namespace TeslaSQL.DataUtils {
    public class VerticaDataUtils : IDataUtils {

        public Logger logger;
        public TServer server;

        public VerticaDataUtils(Logger logger, TServer server) {
            this.logger = logger;
            this.server = server;
        }

        /// <summary>
        /// Runs a sql query and returns results as requested type
        /// </summary>
        /// <param name="cmd">VerticaCommand to run</param>
        /// <param name="timeout">Query timeout</param>
        /// <returns>DataTable object representing the result</returns>
        internal DataTable SqlQuery(VerticaCommand cmd, int? timeout = null) {
            int commandTimeout = timeout ?? Config.QueryTimeout;
            string connStr = buildConnString();

            using (VerticaConnection conn = new VerticaConnection(connStr)) {
                conn.Open();

                cmd.Connection = conn;
                cmd.CommandTimeout = commandTimeout;

                LogCommand(cmd);
                DataSet ds = new DataSet();
                VerticaDataAdapter da = new VerticaDataAdapter(cmd);
                //this is where the query is run
                da.Fill(ds);

                //return the result, which is the first DataTable in the DataSet
                return ds.Tables[0];
            }
        }

        /// <summary>
        /// Runs a query that does not return results (i.e. a write operation)
        /// </summary>
        /// <param name="cmd">VerticaCommand to run</param>
        /// <param name="timeout">Timeout (higher than selects since some writes can be large)</param>
        /// <returns>The number of rows affected</returns>
        internal int SqlNonQuery(VerticaCommand cmd, int? timeout = null) {
            int commandTimeout = timeout ?? Config.QueryTimeout;
            //build connection string based on server/db info passed in
            string connStr = buildConnString();
            int numrows;
            //using block to avoid resource leaks
            using (VerticaConnection conn = new VerticaConnection(connStr)) {
                //open database connection
                conn.Open();
                cmd.Connection = conn;
                cmd.CommandTimeout = commandTimeout;
                LogCommand(cmd);
                numrows = cmd.ExecuteNonQuery();
            }
            return numrows;
        }

        /// <summary>
        /// Runs a query that does not return results (i.e. a write operation), with transaction
        /// </summary>
        /// <param name="cmd">VerticaCommand to run</param>
        /// <param name="timeout">Timeout (higher than selects since some writes can be large)</param>
        /// <returns>The number of rows affected</returns>
        internal int SqlNonQueryWithTransaction(VerticaCommand cmd, int? timeout = null) {
            int commandTimeout = timeout ?? Config.QueryTimeout;
            //build connection string based on server/db info passed in
            string connStr = buildConnString();
            int numrows;
            //using block to avoid resource leaks
            using (VerticaConnection conn = new VerticaConnection(connStr)) {
                //open database connection
                conn.Open();
                VerticaTransaction txn = conn.BeginTransaction();
                cmd.Connection = conn;
                cmd.CommandTimeout = commandTimeout;
                LogCommand(cmd);
                numrows = cmd.ExecuteNonQuery();
                txn.Commit();
            }
            return numrows;
        }

        /// <summary>
        /// Runs a sql query and returns first column and row from results as specified type
        /// </summary>
        /// <param name="cmd">VerticaCommand to run</param>
        /// <param name="timeout">Query timeout</param>
        /// <returns>The value in the first column and row, as the specified type</returns>
        private T SqlQueryToScalar<T>(VerticaCommand cmd, int? timeout = null)
        {
            DataTable result = SqlQuery(cmd, timeout);
            // return result in first column and first row as specified type
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
        /// Parse a SQL query, substituting parameters for their values.
        /// </summary>
        /// <param name="cmd">VerticaCommand to parse</param>
        /// <returns>The parsed query</returns>
        private string ParseCommand(VerticaCommand cmd) {
            string query = cmd.CommandText;
            foreach (VerticaParameter p in cmd.Parameters) {
                query = query.Replace(p.ParameterName, "'" + p.Value.ToString() + "'");
            }
            return query;
        }

        /// <summary>
        /// Log a command
        /// </summary>
        /// <param name="cmd">VerticaCommand to log</param>
        private void LogCommand(VerticaCommand cmd) {
            logger.Log("Executing query: " + ParseCommand(cmd), LogLevel.Debug);
        }

        /// <summary>
        /// Builds a connection string for the passed in server identifier using global config values
        /// NOTE: Vertica does not have multiple databases per host so we 
        /// have to use schemas as a logical separation for databases.
        /// Database name is configured in the config file
        /// </summary>
        /// <returns>An ADO.NET connection string</returns>
        private string buildConnString() {
            string host = "";
            string user = "";
            string password = "";
            string label = "";
            string isolationLevel = "";
            string backupServerNode = "";
            string verticaDatabase = "";
            int port;
            int connectionTimeout;
            bool connectionLoadBalance;

            switch (server) {
                case TServer.SLAVE:
                    host = Config.Slave;
                    user = Config.SlaveUser;
                    password = (new cTripleDes().Decrypt(Config.SlavePassword));
                    label = Config.VerticaLabel;
                    isolationLevel = Config.VerticaIsolationLevel;
                    backupServerNode = Config.VerticaBackupServerNode;
                    verticaDatabase = Config.VerticaDatabase;
                    port = Config.VerticaPort;
                    connectionTimeout = Config.VerticaConnectionTimeout;
                    connectionLoadBalance = Config.VerticaConnectionLoadBalance;
                    break;
                default:
                    throw new NotImplementedException("Vertica is only supported as a slave!");
            }
            VerticaConnectionStringBuilder builder = new VerticaConnectionStringBuilder();
            builder.Host = host;
            builder.Database = verticaDatabase;
            builder.User = user;
            builder.Password = password;
            builder.Label = label;
            builder.BackupServerNode = backupServerNode;
            builder.Port = port;
            builder.ConnectionTimeout = connectionTimeout;
            builder.ConnectionLoadBalance = connectionLoadBalance;
            try {
                builder.IsolationLevel = (IsolationLevel)Enum.Parse(typeof(IsolationLevel), isolationLevel);
            } catch (Exception) {
                throw new Exception("Problem parsing Vertica connection isolation level");
            }

            return builder.ToString();
        }

        // NOTE: this may need work when Vertica is used as master agent
        // currently this is not used by any workflow
        public List<TColumn> GetFieldList(string dbName, string table, string schema) {
            var cols = new List<TColumn>();
            using (var con = new VerticaConnection(buildConnString())) {
                con.Open();
                var t = con.GetSchema("Columns", new string[] { dbName, schema, table, null });
                foreach (DataRow row in t.Rows) {
                    cols.Add(new TColumn(row.Field<string>("COLUMN_NAME"), false, null, true));
                }
            }
            return cols;
        }

        /// <summary>
        /// Writes data from the given data file to the destination Vertica database
        /// </summary>
        /// <param name="fileName">Name of data file to copy from</param>
        /// <param name="dbName">Database name</param>
        /// <param name="table">Table name to write to</param>
        /// <param name="timeout">Timeout</param>
        public void BulkCopy(string fileName, string dbName, string table, int timeout) {
            // reference: http://goo.gl/8R6UXJ
            string copyStatement = string.Format(
                "COPY {0}.{1} FROM '{2}' DELIMITER '|' NULL '' ENCLOSED BY '' RECORD TERMINATOR E'\r\n' ABORT ON ERROR DIRECT STREAM NAME 'Tesla' NO COMMIT",
                dbName,
                table,
                fileName);
            VerticaCommand cmd = new VerticaCommand(copyStatement);
            SqlNonQueryWithTransaction(cmd);
        }

        public bool DropTableIfExists(string dbName, string table, string schema) {
            // here we are still calling the public method defined by the interface
            if (CheckTableExists(dbName, table, schema)) {
                // for Vertica, the source database name becomes the schema name
                string drop = string.Format("DROP TABLE {0}.{1};", dbName, table);
                var cmd = new VerticaCommand(drop);
                return SqlNonQuery(cmd) > 0;
            }
            return false;
        }

        protected bool CheckTableExists(string schema, string table) {
            VerticaCommand cmd = new VerticaCommand();
            // NOTE: Vertica is data case-sensitive, and command case-insensitive
            // which means:
            // for data, "SELECT ... WHERE col = 'A'" is not the same as "SELECT ... WHERE col = 'a'",
            // for command, "CREATE TABLE t ..." will not work if a table [T] already exists
            cmd.CommandText = "SELECT table_schema, table_name FROM v_catalog.tables " +
                "WHERE UPPER(table_schema) = UPPER(@TABLE_SCHEMA) " +
                "AND UPPER(table_name) = UPPER(@TABLE_NAME)";
            cmd.Parameters.Add(new VerticaParameter("TABLE_SCHEMA", VerticaType.VarChar, schema));
            cmd.Parameters.Add(new VerticaParameter("TABLE_NAME", VerticaType.VarChar, table));
            var res = SqlQuery(cmd);
            return res.Rows.Count > 0;
        }

        /// <summary>
        /// Public facing function to check if [dbName].[schema].[table] exists in Vertica DB
        /// For Vertica, we are always connecting to the same database (Config.VerticaDatabase)
        /// but different schemas.  Which means the source's database name becomes the destination's
        /// (Vertica's) schema name
        /// </summary>
        /// <param name="dbName">Database name from the caller</param>
        /// <param name="table"></param>
        /// <param name="schema"></param>
        /// <returns></returns>
        public bool CheckTableExists(string dbName, string table, string schema = "") {
            // ignore [schema] and call the overloaded protected method using [dbName] as [schema]
            return CheckTableExists(dbName, table);
        }

        public RowCounts ApplyTableChanges(TableConf table, TableConf archiveTable, string dbName, long CTID, string CTDBName, bool isConsolidated) {
            var cmds = new List<InsertDelete>();
            cmds.Add(BuildApplyCommand(table, dbName, CTDBName, CTID));
            if (archiveTable != null) {
                cmds.Add(BuildApplyCommand(archiveTable, dbName, CTDBName, CTID));
            }
            var connStr = buildConnString();
            var rowCounts = new RowCounts(0, 0);
            using (var conn = new VerticaConnection(connStr)) {
                conn.Open();
                VerticaTransaction trans = conn.BeginTransaction();
                foreach (var id in cmds) {
                    id.delete.Connection = conn;
                    id.delete.Transaction = trans;
                    id.delete.CommandTimeout = Config.QueryTimeout;
                    logger.Log(id.delete.CommandText, LogLevel.Trace);
                    int deleted = id.delete.ExecuteNonQuery();
                    logger.Log(new { Table = table.Name, message = "Rows deleted: " + deleted }, LogLevel.Info);
                    id.insert.Connection = conn;
                    id.insert.Transaction = trans;
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
            public readonly VerticaCommand insert;
            public readonly VerticaCommand delete;
            public InsertDelete(VerticaCommand insert, VerticaCommand delete) {
                this.insert = insert;
                this.delete = delete;
            }
        }

        /// <summary>
        /// Build the apply command
        /// </summary>
        /// <param name="table">Table to apply changes to</param>
        /// <param name="schema">Vertica schema to apply changes to</param>
        /// <param name="CTDBName">CT database name, which is actually Vertica CT schema name</param>
        /// <param name="CTID">Change tracking ID</param>
        /// <returns>InsertDelete object representing the apply command</returns>
        private InsertDelete BuildApplyCommand(TableConf table, string schema, string CTDBName, long CTID) {
            // NOTE: Vertica does not like the first alias P in the following command:
            //      DELETE FROM a.b P WHERE EXISTS (SELECT 1 FROM c.d CT WHERE P.id = CT.id)
            // instead, the first alias has to be removed, thus:
            //      DELETE FROM a.b WHERE EXISTS (SELECT 1 FROM c.d CT WHERE a.b.id = CT.id)
            // and in the case of multi-column primary key:
            //      DELETE FROM a.b WHERE EXISTS (SELECT 1 FROM c.d CT WHERE a.b.id1 = CT.id1 AND a.b.id2 = CT.id2)
            string verticaTableName = string.Format("{0}.{1}", schema, table.Name);
            string delete = string.Format(
                @"DELETE FROM {0} WHERE EXISTS (SELECT 1 FROM {1}.{2} CT WHERE {3});",
                verticaTableName,
                CTDBName,
                table.ToCTName(CTID),
                table.getNoAliasPkList(verticaTableName));

            // since Vertica does not have the reserved words issue
            // we are using table.SimpleColumnList
            string insert = string.Format(
                @"INSERT INTO {0} ({1}) 
                SELECT {1} FROM {2}.{3} CT
                WHERE NOT EXISTS (SELECT 1 FROM {0} P WHERE {4}) AND CT.sys_change_operation IN ( 'I', 'U' );",
                verticaTableName,
                table.SimpleColumnList,
                CTDBName,
                table.ToCTName(CTID),
                table.PkList);
            var deleteCmd = new VerticaCommand(delete);
            var insertCmd = new VerticaCommand(insert);
            return new InsertDelete(insertCmd, deleteCmd);
        }

        public void CopyIntoHistoryTable(ChangeTable t, string dbName, bool isConsolidated) {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Check if we shall refresh views for the given table, column, action(, dataType) combination
        /// </summary>
        /// <param name="t">Table configuration</param>
        /// <param name="dbName">Database name</param>
        /// <param name="columnName">Column name</param>
        /// <param name="action">The demanded action</param>
        /// <param name="columnShallExist">Whether the column shall exist</param>
        /// <param name="dataType">Demanded datatype of the column</param>
        /// <returns>Whether we shall refresh views for the given table and column</returns>
        protected bool ShallRefreshViews(TableConf t, string dbName, string columnName, string action, bool columnShallExist, string dataType = null) {
            if (t.Name.EndsWith("Archive")) {
                // if the given table is an archive table, check its non-archive (base) table
                string nonArchiveTableName = t.Name.Substring(0, t.Name.LastIndexOf("Archive"));
                if (CheckColumnExists(dbName, t.SchemaName, nonArchiveTableName, columnName)) {
                    if (columnShallExist) {
                        // if the column exists in the non-archive (base) table, and it shall exist
                        if (action == "ModifyColumn") {
                            if (ColumnDatatypeMatches(dbName, t.SchemaName, nonArchiveTableName, columnName, dataType)) {
                                // if we are modifying the column
                                // we shall refresh views if the data type of the target column matches the demanded data type
                                return true;
                            }
                            // will return false if the data type does not match
                        } else {
                            // if we are not modifying the column
                            // we shall refresh views if the column exists and shall exist in the non-archive (base) table
                            return true;
                        }
                    }
                    // will return false if the column exists, but shall not exist
                } else {
                    // if the column does NOT exist, and it shall NOT exist
                    if (!columnShallExist) {
                        return true;
                    }
                    // will return false if the column does not exist, but shall exist
                }
            } else {
                // if the given table is NOT an archive table, check its archive table
                string archiveTableName = t.Name + "Archive";
                if (CheckColumnExists(dbName, t.SchemaName, archiveTableName, columnName)) {
                    if (columnShallExist) {
                        // if the column exists in the archive table, and it shall exist
                        if (action == "ModifyColumn") {
                            if (ColumnDatatypeMatches(dbName, t.SchemaName, archiveTableName, columnName, dataType)) {
                                // if we are modifying the column
                                // we shall refresh views if the data type of the target column matches the demanded data type
                                return true;
                            }
                            // will return false if the data type does not match
                        } else {
                            // if we are not modifying the column
                            // we shall refresh views if the column exists and shall exist in the archive table
                            return true;
                        }
                    }
                    // will return false if the column exists, but shall not exist
                } else {
                    // if the column does NOT exist, and it shall NOT exist
                    if (!columnShallExist) {
                        return true;
                    }
                    // will return false if the column does not exist, but shall exist
                }
            }
            return false;
        }

        public void RenameColumn(TableConf t, string dbName, string columnName, string newColumnName, string historyDB) {
            // rename the column if it exists
            if (CheckColumnExists(dbName, t.SchemaName, t.Name, columnName)) {
                string sql = string.Format(
                    @"ALTER TABLE {0}.{1} RENAME COLUMN {2} TO {3};",
                    dbName,
                    t.Name,
                    columnName,
                    newColumnName);
                var cmd = new VerticaCommand(sql);
                SqlNonQuery(cmd);
                if (ShallRefreshViews(t, dbName, newColumnName, action: "RenameColumn", columnShallExist: true)) {
                    RefreshViews(dbName, t.Name);
                }
            }
        }

        public void ModifyColumn(TableConf t, string dbName, string columnName, DataType dataType, string historyDB) {
            // modify the column if it exists
            if (CheckColumnExists(dbName, t.SchemaName, t.Name, columnName)) {
                string destDataType = MapColumnTypeName(Config.RelayType, dataType, t.getColumnModifier(columnName));
                if (!ColumnDatatypeMatches(dbName, t.SchemaName, t.Name, columnName, destDataType)) {
                    // do not modify if the destination column already has the right data type
                    string sql = string.Format(
                        @"ALTER TABLE {0}.{1} ALTER COLUMN {2} SET DATA TYPE {3};",
                        dbName,
                        t.Name,
                        columnName,
                        destDataType);
                    var cmd = new VerticaCommand(sql);
                    SqlNonQuery(cmd);
                    if (ShallRefreshViews(t, dbName, columnName, action: "ModifyColumn", columnShallExist: true, dataType: destDataType)) {
                        RefreshViews(dbName, t.Name);
                    }
                }
            }
        }

        public void AddColumn(TableConf t, string dbName, string columnName, DataType dataType, string historyDB) {
            // NOTE: Reserved word should not be a problem for Vertica
            // in case we found it is some point in the future, enable mapping
            // columnName = MapReservedWord(columnName);
            if (!CheckColumnExists(dbName, t.SchemaName, t.Name, columnName)) {
                string destDataType = MapColumnTypeName(Config.RelayType, dataType, t.getColumnModifier(columnName));
                string sql = string.Format(
                    @"ALTER TABLE {0}.{1} ADD {2} {3};",
                    dbName,
                    t.Name,
                    columnName,
                    destDataType);
                var cmd = new VerticaCommand(sql);
                SqlNonQuery(cmd);
                if (ShallRefreshViews(t, dbName, columnName, action: "AddColumn", columnShallExist: true)) {
                    RefreshViews(dbName, t.Name);
                }
            }
        }

        public void DropColumn(TableConf t, string dbName, string columnName, string historyDB) {
            // NOTE: Reserved word should not be a problem for Vertica
            // in case we found it is some point in the future, enable mapping
            // columnName = MapReservedWord(columnName);
            if (CheckColumnExists(dbName, t.SchemaName, t.Name, columnName)) {
                string sql = string.Format(
                    @"ALTER TABLE {0}.{1} DROP COLUMN {2} RESTRICT;",
                    dbName,
                    t.Name,
                    columnName);
                var cmd = new VerticaCommand(sql);
                SqlNonQuery(cmd);
                if (ShallRefreshViews(t, dbName, columnName, action: "DropColumn", columnShallExist: false)) {
                    RefreshViews(dbName, t.Name);
                }
            }
        }

        // The refresh command configured will execute if Config.RefreshViews has a configuration for the dbName tableName pair
        private void RefreshViews(string dbName, string tableName) {
            var refresh = Config.RefreshViews.Where(r => r.Db.ToLower() == dbName.ToLower() && r.TableName.ToLower() == tableName.ToLower()).FirstOrDefault();
            if (refresh == null) {
                logger.Log("No refresh view config is available for [" + dbName + "].[" + tableName + "]. Abort refreshing views.", LogLevel.Debug);
                return;
            }
            string sql = refresh.Command;
            var cmd = new VerticaCommand(sql);
            try {
                SqlNonQuery(cmd);
            } catch (Exception) {
                throw new Exception("Please check any pending schema changes to be applied on Vertica before refreshing the view::" + dbName + ".." + refresh.ViewName);
            }
        }

        /// <summary>
        /// Check if the datatype of the column [column] in [dbName].[table] in Vertica DB
        /// matches the given data type
        /// We treat dbName as the schema in Vertica
        /// </summary>
        /// <param name="dbName">Database name</param>
        /// <param name="schema">Schema name</param>
        /// <param name="table">Table name</param>
        /// <param name="column">Column name</param>
        /// <param name="dataType">Data type to match</param>
        /// <returns>Boolean representing whether the column exists</returns>
        private bool ColumnDatatypeMatches(string dbName, string schema, string table, string column, string dataType) {
            // NOTE: for our scenario (MSSQL relay, Vertica slave)
            // MSSQL database name becomes Vertica schema name
            // and MSSQL schema name is ignored
            // NOTE: not sure why this same method is public in NetezzaDataUtils class
            // where the method is not used anywhere outside the class. We are keeping
            // this method private here, as is the case in MSSQLDataUtils and MySQLDataUtils
            string sql = string.Format(
                @"SELECT data_type FROM v_catalog.columns 
                WHERE UPPER(table_schema) = UPPER('{0}')
                    AND UPPER(table_name) = UPPER('{1}')
                    AND UPPER(column_name) = UPPER('{2}')",
                dbName,
                table,
                column);
            var cmd = new VerticaCommand(sql);
            try {
                string dataTypeString = SqlQueryToScalar<string>(cmd);
                return dataTypeString.Equals(dataType, StringComparison.OrdinalIgnoreCase);
            } catch (Exception e) {
                logger.Log("Exception while matching data type for column [" + dbName + "].[" + table + "].[" + column + "]. " + e.Message, LogLevel.Debug);
                return false;
            }
        }

        /// <summary>
        /// Check if a column [column] exists in [dbName].[table] in Vertica DB
        /// We treat dbName as the schema in Vertica
        /// </summary>
        /// <param name="dbName">Database name</param>
        /// <param name="schema">Schema name</param>
        /// <param name="table">Table name</param>
        /// <param name="column">Column name</param>
        /// <returns>Boolean representing whether the column exists</returns>
        private bool CheckColumnExists(string dbName, string schema, string table, string column) {
            // NOTE: for our scenario (MSSQL relay, Vertica slave)
            // MSSQL database name becomes Vertica schema name
            // and MSSQL schema name is ignored
            // NOTE: not sure why this same method is public in NetezzaDataUtils class
            // where the method is not used anywhere outside the class. We are keeping
            // this method private here, as is the case in MSSQLDataUtils and MySQLDataUtils
            string sql = string.Format(
                @"SELECT 1 FROM v_catalog.columns 
                WHERE UPPER(table_schema) = UPPER('{0}')
                    AND UPPER(table_name) = UPPER('{1}')
                    AND UPPER(column_name) = UPPER('{2}')",
                dbName,
                table,
                column);
            var cmd = new VerticaCommand(sql);
            var res = SqlQuery(cmd);
            return res.Rows.Count > 0;
        }

        // NOTE: we don't think we need this for Vertica
        // keeping as placeholder in case some thing emerges
        public static string MapReservedWord(string col) {
            return col;
        }

        public IEnumerable<TTable> GetTables(string dbName) {
            var tables = new List<TTable>();
            using (var con = new VerticaConnection(buildConnString())) {
                con.Open();
                var t = con.GetSchema("Tables", new string[] { null, null, null, "TABLE" });
                foreach (DataRow row in t.Rows) {
                    string tableName = row.Field<string>("TABLE_NAME");
                    // NOTE: interestingly, "TABLE_SCHEM" shall be used instead of "TABLE_SCHEMA"
                    // in case you think this is a typo
                    string schema = row.Field<string>("TABLE_SCHEM");
                    tables.Add(new TTable(tableName, schema));
                }
            }
            return tables;
        }

        /// <summary>
        /// Map source column type to Vertica column type, to the form of: data_type_name[(octet_length)]
        /// NOTE: other DataUtils may consider implementing this method to map data types correctly
        /// </summary>
        /// <param name="sourceFlavor">SQL type of the data source</param>
        /// <param name="sourceDataType">Data type in the source</param>
        /// <param name="modifier">Modifier configured for the column</param>
        /// <returns>string representing Vertica column type in the form of: data_type_name[(octet_length)]</returns>
        public string MapColumnTypeName(SqlFlavor sourceFlavor, DataType sourceDataType, ColumnModifier modifier) {
            string typeName = "";
            string modDataType = DataType.MapDataType(sourceFlavor, SqlFlavor.Vertica, sourceDataType.BaseType);

            switch (sourceFlavor) {
                case SqlFlavor.MSSQL:
                    // these MSSQL types will carry over the length specs (CHARACTER_MAXIMUM_LENGTH) from MSSQL to Vertica
                    // NOTE: for these types, we have to make sure, in the data mapping file, the mapped-to Vertica data type is
                    // specified without the (LENGTH) suffix. That is:
                    // correct: char => char
                    // wrong: char => char(65000)
                    var typesUsingLength = new string[6] { "binary", "char", "nchar", "nvarchar", "varbinary", "varchar" };

                    // these MSSQL types will carry over the scale and precision specs (NUMERIC_PRECISION, NUMERIC_SCALE) from MSSQL to Vertica
                    var typesUsingScale = new string[4] { "decimal", "money", "numeric", "smallmoney" };

                    if (modifier != null) {
                        // if modifier is specified, and matches regex, apply modifier to get type name
                        if (Regex.IsMatch(modDataType, @".*\(\d+\)$")) {
                            // if (LENGTH) is specified in the mapped data type
                            typeName = Regex.Replace(modDataType, @"\d+", modifier.length.ToString());
                        } else {
                            // there is no (LENGTH) in the mapped data type
                            typeName = modDataType + "(" + modifier.length.ToString() + ")";
                        }
                    } else {
                        // if no modifier is specified, or regex does not match
                        string suffix = "";

                        if (typesUsingLength.Contains(sourceDataType.BaseType) && sourceDataType.CharacterMaximumLength != null) {
                            // if this type uses length, and its CHARACTER_MAXIMUM_LENGTH is set in MSSQL

                            // if CHARACTER_MAXIMUM_LENGTH is -1 (max) [(n)varchar(max) types stored with a maxlen of -1 in MSSQL]
                            // or if CHARACTER_MAXIMUM_LENGTH is greater than Vertica string length,
                            // then change that to Vertica string length
                            // otherwise keep the CHARACTER_MAXIMUM_LENGTH value from MSSQL
                            suffix = "(" + ((sourceDataType.CharacterMaximumLength == -1 || sourceDataType.CharacterMaximumLength > Config.VerticaStringLength)
                                ? Convert.ToString(Config.VerticaStringLength) : Convert.ToString(sourceDataType.CharacterMaximumLength)) + ")";
                        } else if (typesUsingScale.Contains(sourceDataType.BaseType) && sourceDataType.NumericPrecision != null && sourceDataType.NumericScale != null) {
                            // if this type uses scale and precision
                            // and both information are available from MSSQL
                            suffix = "(" + sourceDataType.NumericPrecision + ", " + sourceDataType.NumericScale + ")";
                        }

                        typeName = modDataType + suffix;
                    }
                    break;
                default:
                    throw new NotImplementedException("Vertica column type name mapping is not implementation for this source: " + sourceFlavor.ToString());
            }
            return typeName;
        }

        #region unimplemented

        public IEnumerable<long> GetOldCTIDsSlave(string dbName, DateTime chopDate, string slaveIdentifier) {
            throw new NotImplementedException();
        }

        public DataRow GetLastCTBatch(string dbName, AgentType agentType, string slaveIdentifier = "") {
            throw new NotImplementedException("Vertica is only supported as a slave!");
        }

        public IEnumerable<string> GetIntersectColumnList(string dbName, string table1, string schema1, string table2, string schema2) {
            throw new NotImplementedException("Not sure if we need this yet!");
        }

        public DataTable GetPendingCTVersions(string dbName, Int64 CTID, int syncBitWise) {
            throw new NotImplementedException("Vertica is only supported as a slave!");
        }

        public DataTable GetPendingCTSlaveVersions(string dbName, string slaveIdentifier, int bitwise) {
            throw new NotImplementedException("Vertica is only supported as a slave!");
        }

        public DateTime GetLastStartTime(string dbName, Int64 CTID, int syncBitWise, AgentType type, string slaveIdentifier = null) {
            throw new NotImplementedException("Vertica is only supported as a slave!");
        }

        public Int64 GetCurrentCTVersion(string dbName) {
            throw new NotImplementedException("Vertica is only supported as a slave!");
        }

        public Int64 GetMinValidVersion(string dbName, string table, string schema) {
            throw new NotImplementedException("Vertica is only supported as a slave!");
        }

        public ChangeTrackingBatch CreateCTVersion(string dbName, Int64 syncStartVersion, Int64 syncStopVersion) {
            throw new NotImplementedException("Vertica is only supported as a slave!");
        }

        public void CreateSlaveCTVersion(string dbName, ChangeTrackingBatch ctb, string slaveIdentifier) {
            throw new NotImplementedException("Vertica is only supported as a slave!");
        }

        public void CreateSchemaChangeTable(string dbName, Int64 CTID) {
            throw new NotImplementedException("Vertica is only supported as a slave!");
        }

        public DataTable GetDDLEvents(string dbName, DateTime afterDate) {
            throw new NotImplementedException("Vertica is only supported as a slave!");
        }

        public void WriteSchemaChange(string dbName, Int64 CTID, SchemaChange schemaChange) {
            throw new NotImplementedException("Vertica is only supported as a slave!");
        }

        public DataRow GetDataType(string dbName, string table, string schema, string column) {
            throw new NotImplementedException("Vertica is only supported as a slave!");
        }

        public void UpdateSyncStopVersion(string dbName, Int64 syncStopVersion, Int64 CTID) {
            throw new NotImplementedException("Vertica is only supported as a slave!");
        }

        public void WriteBitWise(string dbName, Int64 CTID, int value, AgentType agentType) {
            throw new NotImplementedException("Vertica is only supported as a slave!");
        }

        public int ReadBitWise(string dbName, Int64 CTID, AgentType agentType) {
            throw new NotImplementedException("Vertica is only supported as a slave!");
        }

        public void MarkBatchComplete(string dbName, Int64 CTID, DateTime syncStopTime, string slaveIdentifier) {
            throw new NotImplementedException("Vertica is only supported as a slave!");
        }

        public void MarkBatchesComplete(string dbName, IEnumerable<long> CTIDs, DateTime syncStopTime, string slaveIdentifier) {
            throw new NotImplementedException("Vertica is only supported as a slave!");
        }

        public DataTable GetSchemaChanges(string dbName, Int64 CTID) {
            throw new NotImplementedException("Vertica is only supported as a slave!");
        }

        public Int64 GetTableRowCount(string dbName, string table, string schema) {
            throw new NotImplementedException("Vertica is only supported as a slave!");
        }

        public bool IsChangeTrackingEnabled(string dbName, string table, string schema) {
            throw new NotImplementedException("Vertica is only supported as a slave!");
        }

        public void LogError(string message, string headers) {
            throw new NotImplementedException("Vertica is only supported as a slave!");
        }

        public IEnumerable<TError> GetUnsentErrors() {
            throw new NotImplementedException("Vertica is only supported as a slave!");
        }

        public void MarkErrorsSent(IEnumerable<int> celIds) {
            throw new NotImplementedException("Vertica is only supported as a slave!");
        }

        public void CreateTableInfoTable(string p, long p_2) {
            throw new NotImplementedException("Vertica is only supported as a slave!");
        }

        public void PublishTableInfo(string dbName, TableConf t, long CTID, long expectedRows) {
            throw new NotImplementedException("Vertica is only supported as a slave!");
        }

        public void CreateConsolidatedTable(string tableName, long CTID, string schemaName, string dbName) {
            throw new NotImplementedException();
        }

        public void Consolidate(string tableName, long CTID, string dbName, string schemaName) {
            throw new NotImplementedException();
        }

        public void RemoveDuplicatePrimaryKeyChangeRows(TableConf table, string consolidatedTableName, string dbName) {
            throw new NotImplementedException("Vertica is only supported as a slave!");
        }

        public void CreateConsolidatedTable(string originalName, string schemaName, string dbName, string consolidatedName) {
            throw new NotImplementedException("Vertica is only supported as a slave!");
        }

        public void Consolidate(string ctTableName, string consolidatedTableName, string dbName, string schemaName) {
            throw new NotImplementedException("Vertica is only supported as a slave!");
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
            throw new NotImplementedException("Vertica is only supported as a slave!");
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

        #endregion
    }
}

