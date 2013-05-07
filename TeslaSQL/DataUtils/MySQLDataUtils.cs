using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using System.Text;
using System.Data.SqlClient;
using System.Data;
using TeslaSQL.Agents;
using System.Diagnostics;
using MySql.Data;
using MySql.Data.MySqlClient;

namespace TeslaSQL.DataUtils {
    
    public class MySQLDataUtils : IDataUtils {

        public Logger logger;
        public TServer server;

        public MySQLDataUtils(Logger logger, TServer server)
        {
            this.logger = logger;
            this.server = server;
        }

        public DataRow GetLastCTBatch(string dbName, AgentType agentType, string slaveIdentifier = "") {
            MySqlCommand cmd;
            //for slave we have to pass the slave identifier in and use tblCTSlaveVersion
            if (agentType.Equals(AgentType.Slave))
            {
                cmd = new MySqlCommand("SELECT CTID, syncStartVersion, syncStopVersion, syncBitWise, syncStartTime" +
                    " FROM tblCTSlaveVersion WITH(NOLOCK) WHERE slaveIdentifier = @slave ORDER BY CTID DESC LIMIT 0,1");
                cmd.Parameters.Add("@slave", MySqlDbType.VarChar, 100).Value = slaveIdentifier;
            }
            else
            {
                cmd = new MySqlCommand("SELECT CTID, syncStartVersion, syncStopVersion, syncBitWise, syncStartTime FROM tblCTVersion ORDER BY CTID DESC LIMIT 0,1");
            }

            DataTable result = MySqlQuery(dbName, cmd);
            return result.Rows.Count > 0 ? result.Rows[0] : null;
        }

        public DataTable GetPendingCTVersions(string dbName, Int64 CTID, int syncBitWise)
        {
            string query = ("SELECT CTID, syncStartVersion, syncStopVersion, syncStartTime, syncBitWise" +
                                " FROM tblCTVersion WITH(NOLOCK) WHERE CTID > @ctid AND syncBitWise & @syncbitwise > 0" +
                                " ORDER BY CTID ASC");
            MySqlCommand cmd = new MySqlCommand(query);
            cmd.Parameters.Add("@ctid", MySqlDbType.DateTime).Value = CTID;
            cmd.Parameters.Add("@syncbitwise", MySqlDbType.Int32).Value = syncBitWise;

            //get query results as a datatable since there can be multiple rows
            return MySqlQuery(dbName, cmd);
        }

        public DataTable GetPendingCTSlaveVersions(string dbName, string slaveIdentifier, int bitwise)
        {
            throw new NotImplementedException();
        }

        public DateTime GetLastStartTime(string dbName, Int64 CTID, int syncBitWise, AgentType type, string slaveIdentifier = null)
        {
            MySqlCommand cmd;
            if (slaveIdentifier != null)
            {
                cmd = new MySqlCommand(
                "select MAX(syncStartTime) as maxStart FROM dbo.tblCTSlaveVersion WITH(NOLOCK)"
                + " WHERE syncBitWise & @syncbitwise > 0 AND CTID < @CTID and slaveIdentifier = @slaveidentifier");
                cmd.Parameters.Add("@syncbitwise", MySqlDbType.Int32).Value = syncBitWise;
                cmd.Parameters.Add("@CTID", MySqlDbType.Int64).Value = CTID;
                cmd.Parameters.Add("@slaveidentifier", MySqlDbType.VarChar, 500).Value = slaveIdentifier;
            }
            else
            {
                cmd = new MySqlCommand(
                "select MAX(syncStartTime) as maxStart FROM dbo.tblCTVersion WITH(NOLOCK)"
                + " WHERE syncBitWise & @syncbitwise > 0 AND CTID < @CTID");
                cmd.Parameters.Add("@syncbitwise", MySqlDbType.Int32).Value = syncBitWise;
                cmd.Parameters.Add("@CTID", MySqlDbType.Int64).Value = CTID;
            }

            DateTime? lastStartTime = MySqlQueryToScalar<DateTime?>(dbName, cmd);
            if (lastStartTime == null)
            {
                return DateTime.Now.AddDays(-1);
            }
            return (DateTime)lastStartTime;
        }

        public Int64 GetCurrentCTVersion(string dbName)
        {
            throw new NotImplementedException();
        }

        public Int64 GetMinValidVersion(string dbName, string table, string schema)
        {
            throw new NotImplementedException();
        }

        public ChangeTrackingBatch CreateCTVersion(string dbName, Int64 syncStartVersion, Int64 syncStopVersion)
        {
            //create new row in tblCTVersion, output the CTID
            string query = "INSERT INTO tblCTVersion (syncStartVersion, syncStopVersion, syncStartTime, syncBitWise)";
            query += " OUTPUT inserted.CTID, inserted.syncStartTime";
            query += " VALUES (@startVersion, @stopVersion, CURDATE(), 0)";

            MySqlCommand cmd = new MySqlCommand(query);

            //Ostensibly at some point we'll take out the convert to Int32 when MySql starts using 64 bit timestamps
            cmd.Parameters.Add("@startVersion", MySqlDbType.Timestamp).Value = Convert.ToInt32(syncStartVersion);
            cmd.Parameters.Add("@stopVersion", MySqlDbType.Timestamp).Value = Convert.ToInt32(syncStopVersion);
            var res = MySqlQuery(dbName, cmd);
            return new ChangeTrackingBatch(
                res.Rows[0].Field<Int64>("CTID"),
                syncStartVersion,
                syncStopVersion,
                0,
                res.Rows[0].Field<DateTime>("syncStartTime")
                );
        }

        public int SelectIntoCTTable(string sourceCTDB, TableConf table, string sourceDB, ChangeTrackingBatch batch, int timeout, Int64? startVersionOverride = null)
        {
            throw new NotImplementedException();

            /*
            * There is no way to have column lists or table names be parametrized/dynamic in sqlcommands other than building the string
            * manually like this. However, the table name and column list fields are trustworthy because they have already been compared to
            * actual database objects at this point. The database names are also validated to be legal database identifiers.
            * Only the start and stop versions are actually parametrizable.
            */
            string query = "SELECT " + table.ModifiedMasterColumnList + ", CT.SYS_CHANGE_VERSION, CT.SYS_CHANGE_OPERATION ";
            query += " INTO " + table.SchemaName + "." + table.ToCTName(batch.CTID);
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

            MySqlCommand cmd = new MySqlCommand(query);

            cmd.Parameters.Add("@startversion", MySqlDbType.Int64).Value = (startVersionOverride.HasValue ? startVersionOverride.Value : batch.SyncStartVersion);
            cmd.Parameters.Add("@stopversion", MySqlDbType.Int64).Value = batch.SyncStopVersion;

            return MySqlNonQuery(sourceCTDB, cmd, Config.QueryTimeout);
        }

        public void CreateSlaveCTVersion(string dbName, ChangeTrackingBatch ctb, string slaveIdentifier)
        {
            throw new NotImplementedException();
        }

        public void CreateSchemaChangeTable(string dbName, Int64 CTID)
        {
            //drop the table on the relay server if it exists
            DropTableIfExists(dbName, "tblCTSchemaChange_" + Convert.ToString(CTID));

            //can't parametrize the CTID since it's part of a table name, but it is an Int64 so it's not an injection risk
            string query = "CREATE TABLE tblCTSchemaChange_" + CTID + " (";
            query += @"
            CscID int NOT NULL AUTO_INCREMENT PRIMARY KEY,
	        CscDdeID int NOT NULL,
	        CscTableName varchar(500) NOT NULL,
            CscEventType varchar(50) NOT NULL,
            CscSchema varchar(100) NOT NULL,
            CscColumnName varchar(500) NOT NULL,
            CscNewColumnName varchar(500) NULL,
            CscBaseDataType varchar(100) NULL,
            CscCharacterMaximumLength int NULL,
            CscNumericPrecision int NULL,
            CscNumericScale int NULL
            )";

            MySqlCommand cmd = new MySqlCommand(query);

            MySqlNonQuery(dbName, cmd);
        }

        public DataTable GetDDLEvents(string dbName, DateTime afterDate)
        {
            if (!CheckTableExists(dbName, "tblDDLEvent"))
            {
                throw new Exception("tblDDLEvent does not exist on the source database, unable to check for schema changes. Please create the table and the trigger that populates it!");
            }

            string query = "SELECT DdeID, DdeEventData FROM dbo.tblDDLEvent WHERE DdeTime > @afterdate";

            MySqlCommand cmd = new MySqlCommand(query);
            cmd.Parameters.Add("@afterdate", MySqlDbType.DateTime).Value = afterDate;

            return MySqlQuery(dbName, cmd);
        }

        public void WriteSchemaChange(string dbName, Int64 CTID, SchemaChange schemaChange)
        {
            string query = "INSERT INTO tblCTSchemaChange_" + Convert.ToString(CTID) +
                            " (CscDdeID, CscTableName, CscEventType, CscSchema, CscColumnName, CscNewColumnName, " +
                            " CscBaseDataType, CscCharacterMaximumLength, CscNumericPrecision, CscNumericScale) " +
                            " VALUES (@ddeid, @tablename, @eventtype, @schema, @columnname, @newcolumnname, " +
                            " @basedatatype, @charactermaximumlength, @numericprecision, @numericscale)";

            var cmd = new MySqlCommand(query);
            cmd.Parameters.Add("@ddeid", MySqlDbType.Int32).Value = schemaChange.DdeID;
            cmd.Parameters.Add("@tablename", MySqlDbType.VarChar, 500).Value = schemaChange.TableName;
            cmd.Parameters.Add("@eventtype", MySqlDbType.VarChar, 50).Value = schemaChange.EventType;
            cmd.Parameters.Add("@schema", MySqlDbType.VarChar, 100).Value = schemaChange.SchemaName;
            cmd.Parameters.Add("@columnname", MySqlDbType.VarChar, 500).Value = schemaChange.ColumnName;
            cmd.Parameters.Add("@newcolumnname", MySqlDbType.VarChar, 500).Value = schemaChange.NewColumnName;
            if (schemaChange.DataType != null)
            {
                cmd.Parameters.Add("@basedatatype", MySqlDbType.VarChar, 100).Value = schemaChange.DataType.BaseType;
                cmd.Parameters.Add("@charactermaximumlength", MySqlDbType.Int32).Value = schemaChange.DataType.CharacterMaximumLength;
                cmd.Parameters.Add("@numericprecision", MySqlDbType.Int32).Value = schemaChange.DataType.NumericPrecision;
                cmd.Parameters.Add("@numericscale", MySqlDbType.Int32).Value = schemaChange.DataType.NumericScale;
            }
            else
            {
                if (schemaChange.EventType == SchemaChangeType.Add)
                {
                    throw new Exception("Cannot add a schema change without a valid datatype");
                }
                cmd.Parameters.Add("@basedatatype", MySqlDbType.VarChar, 100).Value = DBNull.Value;
                cmd.Parameters.Add("@charactermaximumlength", MySqlDbType.Int32).Value = DBNull.Value;
                cmd.Parameters.Add("@numericprecision", MySqlDbType.Int32).Value = DBNull.Value;
                cmd.Parameters.Add("@numericscale", MySqlDbType.Int32).Value = DBNull.Value;
            }
            foreach (IDataParameter p in cmd.Parameters)
            {
                if (p.Value == null)
                    p.Value = DBNull.Value;
            }
            MySqlNonQuery(dbName, cmd);
        }

        public DataRow GetDataType(string dbName, string table, string schema, string column)
        {
            throw new NotImplementedException();
            var cmd = new MySqlCommand("SELECT DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE " +
                                    "FROM INFORMATION_SCHEMA.COLUMNS WITH(NOLOCK) WHERE AND TABLE_CATALOG = @db " +
                                    "AND TABLE_NAME = @table AND COLUMN_NAME = @column");
            cmd.Parameters.Add("@db", MySqlDbType.VarChar, 500).Value = dbName;
            cmd.Parameters.Add("@table", MySqlDbType.VarChar, 500).Value = table;
            cmd.Parameters.Add("@column", MySqlDbType.VarChar, 500).Value = column;

            DataTable result = MySqlQuery(dbName, cmd);

            if (result == null || result.Rows.Count == 0)
            {
                throw new DoesNotExistException("Column " + column + " does not exist on table " + table + "!");
            }

            return result.Rows[0];
        }

        public void UpdateSyncStopVersion(string dbName, Int64 syncStopVersion, Int64 CTID)
        {
            string query = "UPDATE tblCTVersion set syncStopVersion = @stopversion WHERE CTID = @ctid";
            var cmd = new MySqlCommand(query);

            cmd.Parameters.Add("@stopversion", MySqlDbType.Timestamp).Value = syncStopVersion;
            cmd.Parameters.Add("@ctid", MySqlDbType.Int64).Value = CTID;

            MySqlNonQuery(dbName, cmd);
        }

        public bool CheckTableExists(string dbName, string table, string schema = "")
        {
            var cmd = new MySqlCommand(
                    @"SELECT COUNT(*) as TableExists FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_NAME = @tablename AND TABLE_TYPE = 'BASE TABLE'");
            cmd.Parameters.Add("@tablename", MySqlDbType.VarChar, 500).Value = table;
            var result = MySqlQuery(dbName, cmd);
            return result.Rows.Count > 0 && result.Rows[0].Field<int>("TableExists") == 1;
        }

        public IEnumerable<string> GetIntersectColumnList(string dbName, string tableName1, string schema1, string tableName2, string schema2)
        {
            List<TColumn> fields1 = GetFieldList(dbName, tableName1, schema1);
            List<TColumn> fields2 = GetFieldList(dbName, tableName2, schema2);
            var columns1 = new List<string>();
            var columns2 = new List<string>();
            //create this so that casing changes to columns don't cause problems, just use the lowercase column name
            foreach (var c in fields1)
            {
                columns1.Add(c.name.ToLower());
            }
            foreach (var c in fields2)
            {
                columns2.Add(c.name.ToLower());
            }
            return columns1.Intersect(columns2);
        }

        public bool DropTableIfExists(string dbName, string table, string schema = "")
        {
            if (CheckTableExists(dbName, table))
            {
                MySqlCommand cmd = new MySqlCommand(string.Format(@"DROP TABLE IF EXISTS {0}", table));
                MySqlNonQuery(dbName, cmd);
                return true;
            }
            
            return false;
        }

        public List<TColumn> GetFieldList(string dbName, string table, string schema="")
        {
            throw new NotImplementedException();

            String sql = @"SELECT c.COLUMN_NAME, DATA_TYPE,
                CHARACTER_MAXIMUM_LENGTH, NUMERIC_SCALE, NUMERIC_PRECISION, IS_NULLABLE,
                CASE WHEN EXISTS (
	                SELECT 1 FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE cu
	                WHERE cu.COLUMN_NAME = c.COLUMN_NAME AND cu.TABLE_NAME = c.TABLE_NAME
	                AND EXISTS (
		                SELECT * FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
		                WHERE tc.TABLE_NAME = c.TABLE_NAME
                        AND tc.CONSTRAINT_NAME = 'PRIMARY'
                        LIMIT 0,1
                        )
                    ) THEN CAST(1 AS BIT) ELSE CAST(0 AS BIT) END AS InPrimaryKey
                FROM INFORMATION_SCHEMA.COLUMNS c
                WHERE c.TABLE_NAME = @table";
            var cmd = new MySqlCommand(sql);
            cmd.Parameters.Add("@table", MySqlDbType.VarChar, 500).Value = table;
            DataTable res = MySqlQuery(dbName, cmd);
            var columns = new List<TColumn>();
            if (res.Rows.Count == 0)
            {
                logger.Log("Unable to get field list for " + dbName + "." + schema + "." + table + " because it does not exist", LogLevel.Debug);
                return columns;
            }
            foreach (DataRow row in res.Rows)
            {
                columns.Add(new TColumn(
                    row.Field<string>("COLUMN_NAME"),
                    row.Field<bool>("InPrimaryKey"),
                    DataType.ParseDataType(row),
                    //for some reason IS_NULLABLE is a varchar(3) rather than a bool or bit
                    row.Field<string>("IS_NULLABLE") == "YES" ? true : false));
            }
            return columns;
        }

        public void WriteBitWise(string dbName, Int64 CTID, int value, AgentType agentType)
        {
            string query;
            MySqlCommand cmd;
            if (agentType.Equals(AgentType.Slave))
            {
                query = "UPDATE tblCTSlaveVersion SET SyncBitWise = SyncBitWise + @syncbitwise";
                query += " WHERE slaveIdentifier = @slaveidentifier AND CTID = @ctid AND SyncBitWise & @syncbitwise = 0";
                cmd = new MySqlCommand(query);
                cmd.Parameters.Add("@syncbitwise", MySqlDbType.Int32).Value = value;
                cmd.Parameters.Add("@slaveidentifier", MySqlDbType.VarChar, 100).Value = Config.Slave;
                cmd.Parameters.Add("@ctid", MySqlDbType.Timestamp).Value = CTID;
            }
            else
            {
                query = "UPDATE tblCTVersion SET SyncBitWise = SyncBitWise + @syncbitwise";
                query += " WHERE CTID = @ctid AND SyncBitWise & @syncbitwise = 0";
                cmd = new MySqlCommand(query);
                cmd.Parameters.Add("@syncbitwise", MySqlDbType.Int32).Value = value;
                cmd.Parameters.Add("@ctid", MySqlDbType.Timestamp).Value = CTID;
            }
            MySqlNonQuery(dbName, cmd);
        }

        public int ReadBitWise(string dbName, Int64 CTID, AgentType agentType)
        {
            throw new NotImplementedException();
            string query;
            MySqlCommand cmd;
            if (agentType.Equals(AgentType.Slave))
            {
                query = "SELECT syncBitWise from tblCTSlaveVersion WITH(NOLOCK)";
                query += " WHERE slaveIdentifier = @slaveidentifier AND CTID = @ctid";
                cmd = new MySqlCommand(query);
                cmd.Parameters.Add("@slaveidentifier", MySqlDbType.VarChar, 100).Value = Config.Slave;
                cmd.Parameters.Add("@ctid", MySqlDbType.Timestamp).Value = CTID;
            }
            else
            {
                query = "SELECT syncBitWise from tblCTVersion WITH(NOLOCK)";
                query += " WHERE CTID = @ctid";
                cmd = new MySqlCommand(query);
                cmd.Parameters.Add("@ctid", MySqlDbType.Timestamp).Value = CTID;
            }
            return MySqlQueryToScalar<Int32>(dbName, cmd);
        }

        public void MarkBatchComplete(string dbName, Int64 CTID, DateTime syncStopTime, string slaveIdentifier)
        {
            MySqlCommand cmd;
            Enum.GetValues(typeof(SyncBitWise)).Cast<int>().Sum();
            string query = "UPDATE tblCTSlaveVersion SET syncBitWise = @syncbitwise, syncStopTime = @syncstoptime";
            query += " WHERE slaveIdentifier = @slaveidentifier AND CTID = @ctid";
            cmd = new MySqlCommand(query);
            cmd.Parameters.Add("@syncbitwise", MySqlDbType.Int32).Value = Enum.GetValues(typeof(SyncBitWise)).Cast<int>().Sum();
            cmd.Parameters.Add("@syncstoptime", MySqlDbType.DateTime).Value = syncStopTime;
            cmd.Parameters.Add("@slaveidentifier", MySqlDbType.VarChar, 100).Value = slaveIdentifier;
            cmd.Parameters.Add("@ctid", MySqlDbType.Timestamp).Value = CTID;
            MySqlNonQuery(dbName, cmd);
        }

        public DataTable GetSchemaChanges(string dbName, Int64 CTID)
        {
            var cmd = new MySqlCommand("SELECT CscID, CscDdeID, CscTableName, CscEventType, CscSchema, CscColumnName" +
                             ", CscNewColumnName, CscBaseDataType, CscCharacterMaximumLength, CscNumericPrecision, CscNumericScale" +
                             " FROM tblCTSchemaChange_" + Convert.ToString(CTID));
            return MySqlQuery(dbName, cmd);
        }

        public Int64 GetTableRowCount(string dbName, string table, string schema = "")
        {
            var cmd = new MySqlCommand(string.Format("SELECT COUNT(*) FROM {0}", table));
            return MySqlQueryToScalar<Int32>(dbName, cmd);
        }

        public bool IsChangeTrackingEnabled(string dbName, string table, string schema)
        {
            throw new NotImplementedException();
            var cmd = new MySqlCommand(string.Format(@"
                SELECT 1 as IsEnabled FROM sys.change_tracking_tables WHERE OBJECT_ID = OBJECT_ID('{0}.{1}')",
                schema, table));
            var result = MySqlQuery(dbName, cmd);
            return result.Rows.Count > 0 && result.Rows[0].Field<int>("IsEnabled") == 1;
        }

        public void LogError(string message, string headers)
        {
            var cmd = new MySqlCommand("INSERT INTO tblCTError (CelError, CelHeaders) VALUES ( @error, @headers )");
            cmd.Parameters.Add("@error", MySqlDbType.VarChar, -1).Value = message;
            cmd.Parameters.Add("@headers", MySqlDbType.VarChar, -1).Value = headers;
            MySqlNonQuery(Config.ErrorLogDB, cmd);
        }

        public IEnumerable<TError> GetUnsentErrors()
        {
            var cmd = new MySqlCommand("SELECT CelError, CelId, CelHeaders, CelLogDate FROM tblCTError WHERE CelSent = 0");
            DataTable res = MySqlQuery(Config.ErrorLogDB, cmd);
            var errors = new List<TError>();
            foreach (DataRow row in res.Rows)
            {
                errors.Add(new TError(row.Field<string>("CelHeaders"), row.Field<string>("CelError"), row.Field<DateTime>("CelLogDate"), row.Field<int>("CelId")));
            }
            return errors;
        }

        public void MarkErrorsSent(IEnumerable<int> celIds)
        {
            var cmd = new MySqlCommand("UPDATE tblCTError SET CelSent = 1 WHERE CelId IN (" + string.Join(",", celIds) + ")");
            MySqlNonQuery(Config.ErrorLogDB, cmd);
        }

        public void RenameColumn(TableConf t, string dbName, string columnName, string newColumnName, string historyDB)
        {
            //This is indefinitely not implemented because of the way we're implementing change tracking in MySQL
            throw new NotImplementedException();
        }

        public void ModifyColumn(TableConf t, string dbName, string columnName, string dataType, string historyDB)
        {
            MySqlCommand cmd;
            //Modify the column if it exists
            if (CheckColumnExists(dbName, t.SchemaName, t.Name, columnName))
            {
                cmd = new MySqlCommand("ALTER TABLE " + t.FullName + " MODIFY COLUMN " + columnName + " " + dataType);
                logger.Log("Altering table column with command: " + cmd.CommandText, LogLevel.Debug);
                MySqlNonQuery(dbName, cmd);
            }
            //modify on history table if that exists too
            if (t.RecordHistoryTable && CheckTableExists(historyDB, t.HistoryName, t.SchemaName) && CheckColumnExists(historyDB, t.SchemaName, t.HistoryName, columnName))
            {
                cmd = new MySqlCommand("ALTER TABLE " + t.SchemaName + "." + t.HistoryName + " MODIFY COLUMN " + columnName + " " + dataType);
                logger.Log("Altering history table column with command: " + cmd.CommandText, LogLevel.Debug);
                MySqlNonQuery(historyDB, cmd);
            }
        }

        public void AddColumn(TableConf t, string dbName, string columnName, string dataType, string historyDB)
        {
            MySqlCommand cmd;
            //add column if it doesn't exist
            if (!CheckColumnExists(dbName, t.SchemaName, t.Name, columnName))
            {
                cmd = new MySqlCommand("ALTER TABLE " + t.FullName + " ADD " + columnName + " " + dataType);
                logger.Log("Altering table with command: " + cmd.CommandText, LogLevel.Debug);
                MySqlNonQuery(dbName, cmd);
            }
            //add column to history table if the table exists and the column doesn't
            if (t.RecordHistoryTable && CheckTableExists(historyDB, t.HistoryName) && !CheckColumnExists(historyDB, t.SchemaName, t.HistoryName, columnName))
            {
                cmd = new MySqlCommand("ALTER TABLE " + t.HistoryName + " ADD " + columnName + " " + dataType);
                logger.Log("Altering history table column with command: " + cmd.CommandText, LogLevel.Debug);
                MySqlNonQuery(historyDB, cmd);
            }
        }

        public void DropColumn(TableConf t, string dbName, string columnName, string historyDB)
        {
            MySqlCommand cmd;
            //drop column if it exists
            if (CheckColumnExists(dbName, t.SchemaName, t.Name, columnName))
            {
                cmd = new MySqlCommand("ALTER TABLE " + t.FullName + " DROP COLUMN " + columnName);
                logger.Log("Altering table with command: " + cmd.CommandText, LogLevel.Debug);
                MySqlNonQuery(dbName, cmd);
            }
            //if history table exists and column exists, drop it there too
            if (t.RecordHistoryTable && CheckTableExists(historyDB, t.HistoryName) && CheckColumnExists(historyDB, t.SchemaName, t.HistoryName, columnName))
            {
                cmd = new MySqlCommand("ALTER TABLE " + t.HistoryName + " DROP COLUMN " + columnName);
                logger.Log("Altering history table column with command: " + cmd.CommandText, LogLevel.Debug);
                MySqlNonQuery(historyDB, cmd);
            }
        }

        public void CreateTableInfoTable(string dbName, Int64 CTID)
        {
            throw new NotImplementedException();
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

        public void PublishTableInfo(string dbName, TableConf table, long CTID, long expectedRows)
        {
            throw new NotImplementedException();
        }

        public RowCounts ApplyTableChanges(TableConf table, TableConf archiveTable, string dbName, Int64 CTID, string CTDBName, bool isConsolidated)
        {
            throw new NotImplementedException();
        }

        public void Consolidate(string ctTableName, string consolidatedTableName, string dbName, string schemaName)
        {
            throw new NotImplementedException();
        }

        public void RemoveDuplicatePrimaryKeyChangeRows(TableConf table, string consolidatedTableName, string dbName){
            throw new NotImplementedException();
        }

        public void CopyIntoHistoryTable(ChangeTable t, string slaveCTDB, bool isConsolidated)
        {
            throw new NotImplementedException();
        }

        public ChangeTrackingBatch GetCTBatch(string dbName, Int64 ctid)
        {
            throw new NotImplementedException();
        }

        public void RevertCTBatch(string dbName, Int64 ctid)
        {
            throw new NotImplementedException();
        }

        public void MergeCTTable(TableConf table, string destDB, string sourceDB, Int64 CTID)
        {
            throw new NotImplementedException();
        }

        public void CreateShardCTVersion(string dbName, Int64 CTID, Int64 startVersion)
        {
            String query = "INSERT INTO tblCTVersion (ctid, syncStartVersion, syncStartTime, syncBitWise)";
            query += " VALUES (@ctid,@syncStartVersion, CURDATE(), 0)";

            MySqlCommand cmd = new MySqlCommand(query);

            //Ostensibly we'll get rid of the convert to Int32 when MySql starts using 64 bit timestamps
            cmd.Parameters.Add("@ctid", MySqlDbType.Timestamp).Value = Convert.ToInt32(CTID);
            cmd.Parameters.Add("@syncStartVersion", MySqlDbType.Timestamp).Value = Convert.ToInt32(startVersion);

            MySqlNonQuery(dbName, cmd);
        }

        public IEnumerable<string> GetPrimaryKeysFromInfoTable(TableConf table, long CTID, string database)
        {
            throw new NotImplementedException();
        }

        public int GetExpectedRowCounts(string ctDbName, long ctid)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<TTable> GetTables(string dbName)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<long> GetOldCTIDsMaster(string dbName, DateTime chopDate)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<long> GetOldCTIDsRelay(string dbName, DateTime chopDate)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<long> GetOldCTIDsSlave(string dbName, DateTime chopDate, string slaveIdentifier)
        {
            throw new NotImplementedException();
        }

        public void DeleteOldCTVersions(string dbName, DateTime chopDate)
        {
            throw new NotImplementedException();
        }

        public void DeleteOldCTSlaveVersions(string dbName, DateTime chopDate)
        {
            throw new NotImplementedException();
        }

        public bool IsBeingInitialized(string sourceCTDB, TableConf table)
        {
            throw new NotImplementedException();
        }

        public long? GetInitializeStartVersion(string sourceCTDB, TableConf table)
        {
            throw new NotImplementedException();
        }

        public void CleanUpInitializeTable(string dbName, DateTime syncStartTime)
        {
            throw new NotImplementedException();
        }

        public DataTable GetTablesWithChanges(string dbName, IList<ChangeTrackingBatch> batches)
        {
            throw new NotImplementedException();
        }

        public void MarkBatchesComplete(string dbName, IEnumerable<long> ctids, DateTime syncStopTime, string slaveIdentifier)
        {
            throw new NotImplementedException();
        }

        public Dictionary<TableConf, IList<TColumn>> GetAllFields(string dbName, Dictionary<TableConf, string> tableConfCTTableName)
        {
            throw new NotImplementedException();
        }

        public Dictionary<TableConf, IList<string>> GetAllPrimaryKeys(string dbName, IEnumerable<TableConf> tables, ChangeTrackingBatch batch)
        {
            throw new NotImplementedException();
        }

        public Dictionary<TableConf, IEnumerable<string>> GetAllPrimaryKeysMaster(string database, IEnumerable<TableConf> tableConfss)
        {
            throw new NotImplementedException();
        }

        public void MergeInfoTable(string shardDB, string consolidatedDB, long CTID)
        {
            throw new NotImplementedException();
        }

        private DataTable MySqlQuery(string dbName, MySqlCommand cmd, int? timeout = null)
        {
            int commandTimeout = timeout ?? Config.QueryTimeout;
            foreach (IDataParameter p in cmd.Parameters)
            {
                if (p.Value == null)
                    p.Value = DBNull.Value;
            }
            //build connection string based on server/db info passed in
            string connStr = buildConnString(dbName);

            //using block to avoid resource leaks
            using (MySqlConnection conn = new MySqlConnection(connStr))
            {
                //open database connection
                conn.Open();
                cmd.Connection = conn;
                cmd.CommandTimeout = commandTimeout;
                LogCommand(cmd);
                DataSet ds = new DataSet();
                MySqlDataAdapter da = new MySqlDataAdapter(cmd);
                //this is where the query is run
                da.Fill(ds);

                //return the result, which is the first DataTable in the DataSet
                return ds.Tables[0];
            }
        }

        private void LogCommand(MySqlCommand cmd)
        {
            logger.Log("Executing query: " + ParseCommand(cmd), LogLevel.Debug);
        }

        private string ParseCommand(MySqlCommand cmd)
        {
            string query = cmd.CommandText;

            foreach (MySqlParameter p in cmd.Parameters)
            {
                query = query.Replace(p.ParameterName, "'" + p.Value.ToString() + "'");
            }

            return query;
        }

        private string buildConnString(string database)
        {
            string sqlhost = "";
            string sqluser = "";
            string sqlpass = "";

            switch (server)
            {
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

            return "server=" + sqlhost + "; database=" + database + ";user=" + sqluser + ";password=" + sqlpass;
        }

        private T MySqlQueryToScalar<T>(string dbName, MySqlCommand cmd, int? timeout = null)
        {
            DataTable result = MySqlQuery(dbName, cmd, timeout);
            //return result in first column and first row as specified type
            T toRet;
            try
            {
                toRet = (T)result.Rows[0][0];
            }
            catch (InvalidCastException)
            {
                throw new Exception("Unable to cast value " + result.Rows[0][0].ToString() + " to type " + typeof(T) +
                    " when running query: " + ParseCommand(cmd));
            }
            return toRet;
        }
        
        private bool CheckColumnExists(string dbName, string schema, string table, string column)
        {
            var cmd = new MySqlCommand(@"
            SELECT 1 as ColumnExists FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_NAME = @table AND COLUMN_NAME = @column");
            cmd.Parameters.Add("@table", MySqlDbType.VarChar, 500).Value = table;
            cmd.Parameters.Add("@column", MySqlDbType.VarChar, 500).Value = column;
            var result = MySqlQuery(dbName, cmd);
            return result.Rows.Count > 0 && result.Rows[0].Field<int>("ColumnExists") == 1;
        }

        internal int MySqlNonQuery(string dbName, MySqlCommand cmd, int? timeout = null)
        {
            int commandTimeout = timeout ?? Config.QueryTimeout;
            foreach (IDataParameter p in cmd.Parameters)
            {
                if (p.Value == null)
                    p.Value = DBNull.Value;
            }
            //build connection string based on server/db info passed in
            string connStr = buildConnString(dbName);
            int numrows;
            //using block to avoid resource leaks
            using (MySqlConnection conn = new MySqlConnection(connStr))
            {
                conn.Open();
                cmd.Connection = conn;
                cmd.CommandTimeout = commandTimeout;
                LogCommand(cmd);
                numrows = cmd.ExecuteNonQuery();
            }
            return numrows;
        }

        internal int SqlNonQuery(string dbName, SqlCommand cmd, int? timeout = null)
        {
            int commandTimeout = timeout ?? Config.QueryTimeout;
            foreach (IDataParameter p in cmd.Parameters)
            {
                if (p.Value == null)
                    p.Value = DBNull.Value;
            }
            //build connection string based on server/db info passed in
            string connStr = buildConnString(dbName);
            int numrows;
            //using block to avoid resource leaks
            using (SqlConnection conn = new SqlConnection(connStr))
            {
                conn.Open();
                cmd.Connection = conn;
                cmd.CommandTimeout = commandTimeout;
                LogCommand(cmd);
                numrows = cmd.ExecuteNonQuery();
            }
            return numrows;
        }
    }
}
