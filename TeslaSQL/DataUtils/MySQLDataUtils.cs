using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using MySql.Data.MySqlClient;

namespace TeslaSQL.DataUtils {
    
    public class MySQLDataUtils : IDataUtils {

        public Logger logger;
        public TServer server;
        private Int64 CTID;
        private const string CTIDtoTimestampTable = "tblCTIDTimestamp";
        private const string CTTimestampColumnName = "CTTimestamp";

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
                    " FROM tblCTSlaveVersion WITH(NOLOCK) WHERE slaveIdentifier = @slave ORDER BY cttimestamp DESC LIMIT 0,1");
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
            cmd.Parameters.Add("@ctid", MySqlDbType.Timestamp).Value = new DateTime(CTID).ToUniversalTime();
            cmd.Parameters.Add("@syncbitwise", MySqlDbType.Int32).Value = syncBitWise;

            //get query results as a datatable since there can be multiple rows
            return MySqlQuery(dbName, cmd);
        }

        public DataTable GetPendingCTSlaveVersions(string dbName, string slaveIdentifier, int bitwise)
        {
            //not yet (slave)
            throw new NotImplementedException();
        }

        public DateTime GetLastStartTime(string dbName, Int64 CTID, int syncBitWise, AgentType type, string slaveIdentifier = null)
        {
            MySqlCommand cmd;
            if (slaveIdentifier != null)
            {
                cmd = new MySqlCommand(
                "select MAX(syncStartTime) as maxStart FROM tblCTSlaveVersion"
                + " WHERE syncBitWise & @syncbitwise > 0 AND cttimestamp < @CTID and slaveIdentifier = @slaveidentifier");
                cmd.Parameters.Add("@syncbitwise", MySqlDbType.Int32).Value = syncBitWise;
                cmd.Parameters.Add("@CTID", MySqlDbType.Timestamp).Value = new DateTime(CTID).ToUniversalTime();
                cmd.Parameters.Add("@slaveidentifier", MySqlDbType.VarChar, 500).Value = slaveIdentifier;
            }
            else
            {
                cmd = new MySqlCommand(
                "select MAX(syncStartTime) as maxStart FROM tblCTVersion"
                + " WHERE syncBitWise & @syncbitwise > 0 AND cttimestamp < @CTID");
                cmd.Parameters.Add("@syncbitwise", MySqlDbType.Int32).Value = syncBitWise;
                cmd.Parameters.Add("@CTID", MySqlDbType.Timestamp).Value = new DateTime(CTID).ToUniversalTime();
            }

            DateTime? lastStartTime = MySqlQueryToScalar<DateTime?>(dbName, cmd);
            if (lastStartTime == null)
            {
                return DateTime.Now.AddDays(-1);
            }
            return (DateTime)lastStartTime;
        }

        private void CreateCTIDTimestampTable(string dbName)
        {
            //creates the CTID to Timestamp table on the Master mysql database and sets the current ID to the current time
            StringBuilder query = new StringBuilder();
            query.Append("CREATE TABLE ");
            query.Append(CTIDtoTimestampTable);
            query.AppendLine("(");
            query.AppendLine("CTID bigint NOT NULL AUTO_INCREMENT PRIMARY KEY,");
            query.Append(CTTimestampColumnName);
            query.AppendLine(" timestamp NOT NULL);");
            MySqlNonQuery(dbName, new MySqlCommand(query.ToString()));
            query.Clear();
            query.Append("INSERT INTO ");
            query.AppendLine(CTIDtoTimestampTable);
            query.Append("SET ");
            query.Append(CTTimestampColumnName);
            query.AppendLine(" = NOW();");
            MySqlNonQuery(dbName, new MySqlCommand(query.ToString()));
        }

        public Int64 GetCurrentCTVersion(string dbName)
        {
            if (!CheckTableExists(dbName, CTIDtoTimestampTable))
            {
                CreateCTIDTimestampTable(dbName);
                return 1;
            }
            StringBuilder query = new StringBuilder();
            DateTime mysqlTimestamp, maxTimestamp = DateTime.MinValue;

            //get the max timestamp from all of the tables that Tesla is watching
            foreach (TableConf table in Config.Tables)
            {
                query.Append("SELECT MAX(");
                query.Append(CTTimestampColumnName);
                query.Append(") FROM ");
                query.Append(table.Name);
                query.AppendLine(";");
                mysqlTimestamp = DateTime.Parse(MySqlQueryToScalar<String>(dbName, new MySqlCommand(query.ToString())));
                maxTimestamp = maxTimestamp > mysqlTimestamp ? maxTimestamp : mysqlTimestamp;
                query.Clear();
            }
           
            //write that timestamp into our version<->timestamp table as the latest "version" number, then select that
            query.Append("INSERT INTO ");
            query.AppendLine(CTIDtoTimestampTable);
            query.Append("SET ");
            query.Append(CTTimestampColumnName);
            query.Append(" = ");
            query.Append(maxTimestamp.ToString("yyyy-MM-dd HH:mm:ss"));
            query.AppendLine(";");
            MySqlNonQuery(dbName, new MySqlCommand(query.ToString()));
            query.Clear();
            query.Append("SELECT MAX(CTID) FROM ");
            query.Append(CTIDtoTimestampTable);
            query.AppendLine(";");
            MySqlCommand cmd = new MySqlCommand(query.ToString());
            this.CTID = MySqlQueryToScalar<Int64>(dbName, cmd);
            return this.CTID;
        }

        public Int64 GetMinValidVersion(string dbName, string table, string schema)
        {
            throw new NotImplementedException();
        }

        public ChangeTrackingBatch CreateCTVersion(string dbName, Int64 syncStartVersion, Int64 syncStopVersion)
        {
            StringBuilder query = new StringBuilder();
            query.AppendLine("INSERT INTO tblCTVersion (syncStartVersion, syncStopVersion, syncStartTime, syncBitWise)");
            query.AppendLine("VALUES (@startVersion, @stopVersion, CURDATE(), 0)");

            MySqlCommand cmd = new MySqlCommand(query.ToString());

            cmd.Parameters.Add("@startVersion", MySqlDbType.Timestamp).Value = new DateTime(syncStartVersion).ToUniversalTime();
            cmd.Parameters.Add("@stopVersion", MySqlDbType.Timestamp).Value = new DateTime(syncStopVersion).ToUniversalTime();
            MySqlNonQuery(dbName, cmd);

            query.Clear();
            query.AppendLine("SELECT cttimestamp, syncStartTime");
            query.AppendLine("FROM tblCTVersion");
            query.AppendLine("WHERE PK = last_insert_id();");

            cmd = new MySqlCommand(query.ToString());

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

            String tableToInsert = table.ToCTName(batch.CTID);
            StringBuilder query = new StringBuilder();

            query.Append("SELECT @@SESSION.BINLOG_FORMAT;");
            String binlogFormat = MySqlQueryToScalar<String>(sourceDB, new MySqlCommand(query.ToString()));
            Boolean wasAlreadyRow = String.Compare(binlogFormat, "row", true) == 0;
            query.Clear();
            if (!wasAlreadyRow)
            {
                query.Append("SET @@SESSION.BINLOG_FORMAT = 'ROW';");
                MySqlNonQuery(sourceDB, new MySqlCommand(query.ToString()));
                query.Clear();
            }
            query.Append("SET TRANSACTION ISOLATION LEVEL READ COMMITTED;");
            MySqlNonQuery(sourceDB, new MySqlCommand(query.ToString()));
            query.Clear();

            query.AppendLine("BEGIN;");
            query.Append("INSERT INTO ");
            query.AppendLine(tableToInsert);
            query.Append("SELECT ");
            query.Append(table.ModifiedMasterColumnList);
            query.AppendLine(" ctType, ctTimeStamp");
            query.Append("FROM ct_");
            query.Append(table.Name);
            query.Append(" AS P LEFT OUTER JOIN ");
            query.Append(table.Name);
            query.Append(" AS CT ON ");
            query.AppendLine(table.PkList);
            query.Append("WHERE ctTimeStamp >= '");
            query.Append(batch.CTID);
            query.AppendLine("';");
            query.AppendLine("COMMIT;");

            int result = MySqlNonQuery(sourceDB, new MySqlCommand(query.ToString()));

            query.Clear();

            if (!wasAlreadyRow)
            {
                query.Append("SET @@SESSION.BINLOG_FORMAT = '");
                query.Append(binlogFormat);
                query.Append("';");
                MySqlNonQuery(sourceDB, new MySqlCommand(query.ToString()));
                query.Clear();
            }

            return result;
        }

        public void CreateSlaveCTVersion(string dbName, ChangeTrackingBatch ctb, string slaveIdentifier)
        {
            //not yet (slave)
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

        private String FakeDDLEvent(string dbName, string tableName, string command)
        {
            StringBuilder eventInstance = new StringBuilder();
            eventInstance.Append("<EVENT_INSTANCE>");
            eventInstance.Append("<EventType>ALTER_TABLE</EventType>");
            eventInstance.Append("<PostTime>");
            eventInstance.Append(DateTime.UtcNow.ToString());
            eventInstance.Append("</PostTime>");
            eventInstance.Append("<SPID>00000000</SPID>");
            eventInstance.Append("<ServerName>MySQL Cluster</ServerName>");
            eventInstance.Append("<LoginName>unknown</LoginName>");
            eventInstance.Append("<UserName>unknown</UserName>");
            eventInstance.Append("<DatabaseName>");
            eventInstance.Append(dbName);
            eventInstance.Append("</DatabaseName>");
            eventInstance.Append("<SchemaName>N/A</SchemaName>");
            eventInstance.Append("<ObjectName>");
            eventInstance.Append(tableName);
            eventInstance.Append("</ObjectName>");
            eventInstance.Append("<ObjectType>TABLE</ObjectType>");
            eventInstance.Append("<TSQLCommand>");
            eventInstance.Append("<SetOptions />");
            eventInstance.Append("<CommandText>");
            eventInstance.Append(command);
            eventInstance.Append("</CommandText></TSQLCommand></EVENT_INSTANCE>");
            return eventInstance.ToString();
        }

        public DataTable GetDDLEvents(string dbName, DateTime afterDate)
        {
            //get the information schema snapshot, compare it to old one, generate datatable with the following columns
            //ddeid (some unique int)
            //ddeeventdata
            //<EVENT_INSTANCE>
            //<EventType>ALTER_TABLE</EventType>
            //<PostTime>datetime</PostTime>
            //<SPID>random int<SPID>
            //<ServerName>server name</ServerName>
            //<LoginName>unknown</LoginName>
            //<UserName>unknown</UserName>
            //<DatabaseName>database name</DatabaseName>
            //<SchemaName>N/A</SchemaName>
            //<ObjectName>table name</ObjectName>
            //<ObjectType>TABLE</ObjectType>
            //<TSQLCommand>
            //<SetOptions />
            //<CommandText>command</CommandText>
            //</TSQLCommand>
            //</EVENT_INSTANCE>

            String currentSchemaTableName, compareSchemaTableName = "";
            //start initialization of events table to mimic MSSQL output
            DataTable events = new DataTable();
            DataColumn ddlid = new DataColumn("DdeId");
            events.Columns.Add(ddlid);
            DataColumn eventdata = new DataColumn("DdeEventData");
            events.Columns.Add(eventdata);
            //done

            using (MySqlConnection connection = new MySqlConnection(buildConnString(dbName)))
            {
                //for more info please visit http://msdn.microsoft.com/en-us/library/ms254934(v=vs.80).aspx
                //section "Specifying the Restriction Values"
                //in short: rescrictions[TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE]
                //in mysql, catalog is just included to meet the sql spec and isn't used, schema is the db

                StringBuilder query = new StringBuilder();
                DataTable currentSchemaTable, compareSchemaTable, result = new DataTable();
                //all of the columns that we care about
                String[] columnNames = new String[] { "COLUMN_NAME", "ORDINAL_POSITION", "IS_NULLABLE", "COLUMN_TYPE", "CHARACTER_MAX", "NUMERIC_PRECISION", "NUMERIC_SCALE", "COLUMN_KEY", "EXTRA" };
                connection.Open();
                foreach (TableConf table in Config.Tables)
                {
                    currentSchemaTableName = table.Name + "_schema_" + this.CTID.ToString();
                    compareSchemaTableName = table.Name + "_schema_" + (this.CTID - 1).ToString();
                    //make a snapshot of the current schema to work off of
                    query.Clear();
                    query.Append("CREATE TABLE ");
                    query.Append(currentSchemaTableName);
                    query.Append(" LIKE ");
                    query.Append(table.Name);
                    query.AppendLine(";");
                    MySqlNonQuery(dbName, new MySqlCommand(query.ToString()));
                    //if this is the first time running, just return an empty event set
                    if (!CheckTableExists(dbName, compareSchemaTableName))
                    {
                        return events;
                    }
                    currentSchemaTable = GetColumnInformationFromInformationSchema(dbName, currentSchemaTableName, columnNames);
                    compareSchemaTable = GetColumnInformationFromInformationSchema(dbName, compareSchemaTableName, columnNames);
                    List<string> currentSchemaColumnNames, compareSchemaColumnNames = new List<string>();
                    foreach (DataRow row in currentSchemaTable.Rows)
                    {
                        currentSchemaColumnNames.Add(row["COLUMN_NAME"].ToString());
                    }
                    foreach (DataRow row in compareSchemaTable.Rows)
                    {
                        compareSchemaColumnNames.Add(row["COLUMN_NAME"].ToString());
                    }
                    //check for dropped columns
                    foreach (string columnName in compareSchemaColumnNames)
                    {
                        if (!currentSchemaColumnNames.Contains(columnName))
                        {
                            DataRow toAdd = new DataRow();
                            toAdd["DdeId"] = 2;
                            query.Clear();
                            query.Append("ALTER TABLE ");
                            query.Append(table.Name);
                            query.Append("DROP COLUMN ");
                            query.Append(columnName);
                            query.AppendLine(";");
                            toAdd["DdeEventData"] = FakeDDLEvent(dbName, table.Name, query.ToString());
                            events.Rows.Add(toAdd);
                        }
                    }
                    //looking for data type changes or ordinal position changes
                    for (int index = 0; index < currentSchemaTable.Rows.Count; index++)
                    {
                        foreach (String columnName in new String[] {"COLUMN_NAME", "COLUMN_TYPE", "IS_NULLABLE", "COLUMN_KEY"})
                        {
                            if (currentSchemaTable.Rows[index][columnName] != compareSchemaTable.Rows[index][columnName])
                            {
                                DataRow toAdd = new DataRow();
                                toAdd["DdeId"] = index + 1;
                                query.Clear();
                                query.Append("ALTER TABLE ");
                                query.Append(table.Name);
                                switch (columnName)
                                {
                                    case "COLUMN_TYPE":
                                    case "IS_NULLABLE":
                                    case "COLUMN_KEY":
                                        query.Append("MODIFY ");
                                        query.Append(currentSchemaTable.Rows[index]["COLUMN_NAME"]);
                                        query.Append(" ");
                                        query.Append(currentSchemaTable.Rows[index]["COLUMN_TYPE"]);
                                        query.Append(" ");
                                        query.Append(currentSchemaTable.Rows[index]["COLUMN_TYPE"] == "YES" ? "NULL" : "NOT NULL");
                                        query.Append(" ");
                                        if (currentSchemaTable.Rows[index]["COLUMN_KEY"] == "PRI")
                                        {
                                            query.Append("PRIMARY KEY");
                                        }
                                        if (currentSchemaTable.Rows[index]["EXTRA"] == "auto_increment")
                                        {
                                            query.Append("AUTO_INCREMENT");
                                        }
                                        break;
                                    case "COLUMN_NAME":
                                        break;
                                    default:
                                }
                                query.AppendLine(";");
                                toAdd["DdeEventData"] = FakeDDLEvent(dbName, table.Name, query.ToString());


                            }
                        }
                    }

                }
            }

            return events;
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

            cmd.Parameters.Add("@stopversion", MySqlDbType.DateTime).Value = syncStopVersion;
            cmd.Parameters.Add("@ctid", MySqlDbType.Int64).Value = CTID;

            MySqlNonQuery(dbName, cmd);
        }

        public bool CheckTableExists(string dbName, string table, string schema = "")
        {
            var cmd = new MySqlCommand(
                    @"SELECT 1 as TableExists FROM INFORMATION_SCHEMA.TABLES
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
            String sql = @"SELECT  c.COLUMN_NAME,
                                    DATA_TYPE,
                                    CHARACTER_MAXIMUM_LENGTH,
                                    NUMERIC_SCALE,
                                    NUMERIC_PRECISION,
                                    IS_NULLABLE,
                                    IF(s.INDEX_NAME IS NOT NULL, 1, 0) InPrimaryKey
                            FROM    INFORMATION_SCHEMA.COLUMNS c
                            LEFT JOIN    INFORMATION_SCHEMA.STATISTICS s
                            ON      c.COLUMN_NAME = s.COLUMN_NAME AND
                                    c.TABLE_NAME = s.TABLE_NAME AND
                                    c.TABLE_SCHEMA = s.TABLE_SCHEMA AND
                                    s.INDEX_NAME = 'PRIMARY'
                            WHERE   c.TABLE_NAME = @table";
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
                cmd.Parameters.Add("@ctid", MySqlDbType.Timestamp).Value = new DateTime(CTID).ToUniversalTime();
            }
            else
            {
                query = "UPDATE tblCTVersion SET SyncBitWise = SyncBitWise + @syncbitwise";
                query += " WHERE CTID = @ctid AND SyncBitWise & @syncbitwise = 0";
                cmd = new MySqlCommand(query);
                cmd.Parameters.Add("@syncbitwise", MySqlDbType.Int32).Value = value;
                cmd.Parameters.Add("@ctid", MySqlDbType.Timestamp).Value = new DateTime(CTID).ToUniversalTime();
            }
            MySqlNonQuery(dbName, cmd);
        }

        public int ReadBitWise(string dbName, Int64 CTID, AgentType agentType)
        {
            //does this get called?
            throw new NotImplementedException();
            string query;
            MySqlCommand cmd;
            if (agentType.Equals(AgentType.Slave))
            {
                query = "SELECT syncBitWise from tblCTSlaveVersion WITH(NOLOCK)";
                query += " WHERE slaveIdentifier = @slaveidentifier AND CTID = @ctid";
                cmd = new MySqlCommand(query);
                cmd.Parameters.Add("@slaveidentifier", MySqlDbType.VarChar, 100).Value = Config.Slave;
                cmd.Parameters.Add("@ctid", MySqlDbType.Timestamp).Value = new DateTime(CTID).ToUniversalTime();
            }
            else
            {
                query = "SELECT syncBitWise from tblCTVersion WITH(NOLOCK)";
                query += " WHERE CTID = @ctid";
                cmd = new MySqlCommand(query);
                cmd.Parameters.Add("@ctid", MySqlDbType.Timestamp).Value = new DateTime(CTID).ToUniversalTime();
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
            cmd.Parameters.Add("@syncstoptime", MySqlDbType.Timestamp).Value = syncStopTime.ToUniversalTime();
            cmd.Parameters.Add("@slaveidentifier", MySqlDbType.VarChar, 100).Value = slaveIdentifier;
            cmd.Parameters.Add("@ctid", MySqlDbType.Timestamp).Value = new DateTime(CTID).ToUniversalTime();
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
            return CheckTableExists(dbName, "ct_" + table);
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
            //We're not writing to a MySQL relay at this time
            throw new NotImplementedException();
        }

        public void PublishTableInfo(string dbName, TableConf table, long CTID, long expectedRows)
        {
            //We're not writing to a MySQL relay at this time
            throw new NotImplementedException();
        }

        public RowCounts ApplyTableChanges(TableConf table, TableConf archiveTable, string dbName, Int64 CTID, string CTDBName, bool isConsolidated)
        {
            //We're not writing to MySQL slaves at this time
            throw new NotImplementedException();
        }

        public void Consolidate(string ctTableName, string consolidatedTableName, string dbName, string schemaName)
        {
            //not yet
            throw new NotImplementedException();
        }

        public void RemoveDuplicatePrimaryKeyChangeRows(TableConf table, string consolidatedTableName, string dbName){
            //not yet
            throw new NotImplementedException();
        }

        public void CopyIntoHistoryTable(ChangeTable t, string slaveCTDB, bool isConsolidated)
        {
            //not yet
            throw new NotImplementedException();
        }

        public ChangeTrackingBatch GetCTBatch(string dbName, Int64 ctid)
        {
            //not yet
            throw new NotImplementedException();
        }

        public void RevertCTBatch(string dbName, Int64 ctid)
        {
            //not yet
            throw new NotImplementedException();
        }

        public void MergeCTTable(TableConf table, string destDB, string sourceDB, Int64 CTID)
        {
            //not yet
            throw new NotImplementedException();
        }

        public void CreateShardCTVersion(string dbName, Int64 CTID, Int64 startVersion)
        {
            String query = "INSERT INTO tblCTVersion (ctid, syncStartVersion, syncStartTime, syncBitWise)";
            query += " VALUES (@ctid,@syncStartVersion, CURDATE(), 0)";

            MySqlCommand cmd = new MySqlCommand(query);

            cmd.Parameters.Add("@ctid", MySqlDbType.Timestamp).Value = new DateTime(CTID).ToUniversalTime();
            cmd.Parameters.Add("@syncStartVersion", MySqlDbType.Timestamp).Value = new DateTime(startVersion).ToUniversalTime();

            MySqlNonQuery(dbName, cmd);
        }

        public IEnumerable<string> GetPrimaryKeysFromInfoTable(TableConf table, long CTID, string database)
        {
            //not yet
            throw new NotImplementedException();
        }

        public int GetExpectedRowCounts(string ctDbName, long ctid)
        {
            //not yet
            throw new NotImplementedException();
        }

        public IEnumerable<TTable> GetTables(string dbName)
        {
            string sql = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'";
            var cmd = new MySqlCommand(sql);
            var res = MySqlQuery(dbName, cmd);
            var tables = new List<TTable>();
            foreach (DataRow row in res.Rows)
            {
                tables.Add(new TTable(row.Field<string>("TABLE_NAME"), ""));
            }
            return tables;
        }

        public IEnumerable<long> GetOldCTIDsMaster(string dbName, DateTime chopDate)
        {
            string sql = "SELECT ctid FROM tblCTVersion WHERE syncStartTime < @chopDate";
            var cmd = new MySqlCommand(sql);
            cmd.Parameters.Add("@chopDate", MySqlDbType.Timestamp).Value = chopDate.ToUniversalTime();
            DataTable res = MySqlQuery(dbName, cmd);
            var CTIDs = new List<long>();
            foreach (DataRow row in res.Rows)
            {
                CTIDs.Add(row.Field<long>("ctid"));
            }
            return CTIDs;
        }

        public IEnumerable<long> GetOldCTIDsRelay(string dbName, DateTime chopDate)
        {
            //not yet
            throw new NotImplementedException();
        }

        public IEnumerable<long> GetOldCTIDsSlave(string dbName, DateTime chopDate, string slaveIdentifier)
        {
            //not yet
            throw new NotImplementedException();
        }

        public void DeleteOldCTVersions(string dbName, DateTime chopDate)
        {
            //not yet
            throw new NotImplementedException();
        }

        public void DeleteOldCTSlaveVersions(string dbName, DateTime chopDate)
        {
            //not yet
            throw new NotImplementedException();
        }

        public bool IsBeingInitialized(string sourceCTDB, TableConf table)
        {
            string sql = string.Format(@"SELECT 1 FROM tblCTInitialize WHERE tableName = @tableName AND inProgress = 1",
                           sourceCTDB);
            var cmd = new MySqlCommand(sql);
            cmd.Parameters.Add("@tableName", MySqlDbType.VarChar, 500).Value = table.Name;
            var res = MySqlQuery(sourceCTDB, cmd);
            return res.Rows.Count > 0;
        }

        public Int64? GetInitializeStartVersion(string sourceCTDB, TableConf table)
        {
            string sql = @"SELECT nextSynchVersion FROM tblCTInitialize WHERE tableName = @tableName";
            var cmd = new MySqlCommand(sql);
            cmd.Parameters.Add("@tableName", MySqlDbType.VarChar, 500).Value = table.Name;
            DataTable res = MySqlQuery(sourceCTDB, cmd);
            if (res.Rows.Count == 0)
            {
                return null;
            }
            else
            {
                return res.Rows[0].Field<long>("nextSynchVersion");
            }
        }

        public void CleanUpInitializeTable(string dbName, DateTime syncStartTime)
        {
            string sql = @"DELETE FROM tblCTInitialize
                           WHERE inProgress = 0
                           AND iniFinishTime < @syncStartTime";
            var cmd = new MySqlCommand(sql);
            cmd.Parameters.Add("@syncStartTime", MySqlDbType.Timestamp).Value = syncStartTime.ToUniversalTime();
            MySqlNonQuery(dbName, cmd);
        }

        public DataTable GetTablesWithChanges(string dbName, IList<ChangeTrackingBatch> batches)
        {
            //not yet
            throw new NotImplementedException();
        }

        public void MarkBatchesComplete(string dbName, IEnumerable<long> ctids, DateTime syncStopTime, string slaveIdentifier)
        {
            //not yet (slave)
            throw new NotImplementedException();
        }

        public Dictionary<TableConf, IList<TColumn>> GetAllFields(string dbName, Dictionary<TableConf, string> t)
        {
            if (t.Count == 0)
            {
                return new Dictionary<TableConf, IList<TColumn>>();
            }
            var placeHolders = t.Select((_, i) => "@table" + i);
            string sql = string.Format(@"SELECT c.COLUMN_NAME,
                                                DATA_TYPE,
                                                CHARACTER_MAXIMUM_LENGTH,
                                                NUMERIC_SCALE,
                                                NUMERIC_PRECISION,
                                                IS_NULLABLE,
                                                IF(s.INDEX_NAME IS NOT NULL, 1, 0) InPrimaryKey
                                        FROM    INFORMATION_SCHEMA.COLUMNS c
                                        LEFT JOIN    INFORMATION_SCHEMA.STATISTICS s
                                        ON      c.COLUMN_NAME = s.COLUMN_NAME AND
                                                c.TABLE_NAME = s.TABLE_NAME AND
                                                c.TABLE_SCHEMA = s.TABLE_SCHEMA AND
                                                s.INDEX_NAME = 'PRIMARY'
                                        WHERE   c.TABLE_NAME IN ( {0} );",
                                       string.Join(",", placeHolders));
            var cmd = new MySqlCommand(sql);
            foreach (var ph in placeHolders.Zip(t.Values, (ph, tn) => Tuple.Create(ph, tn)))
            {
                cmd.Parameters.Add(ph.Item1, MySqlDbType.VarChar, 500).Value = ph.Item2;
            }
            var res = MySqlQuery(dbName, cmd);
            var fields = new Dictionary<TableConf, IList<TColumn>>();
            foreach (DataRow row in res.Rows)
            {
                var tableName = row.Field<string>("TABLE_NAME");
                var tc = t.Keys.FirstOrDefault(table => t[table].Equals(tableName, StringComparison.OrdinalIgnoreCase));
                if (tc == null) { continue; }
                if (!fields.ContainsKey(tc))
                {
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

        public Dictionary<TableConf, IList<string>> GetAllPrimaryKeys(string dbName, IEnumerable<TableConf> tables, ChangeTrackingBatch batch)
        {
            //not yet (slave)
            throw new NotImplementedException();
        }

        public Dictionary<TableConf, IEnumerable<string>> GetAllPrimaryKeysMaster(string database, IEnumerable<TableConf> tableConfss)
        {
            //this is never called?
            throw new NotImplementedException();
        }

        public void MergeInfoTable(string shardDB, string consolidatedDB, long CTID)
        {
            //not yet (shard)
            throw new NotImplementedException();
        }

        public List<TColumn> GetFieldList(string dbName, string table, string schema, string originalTableName, IList<string> includeColumns)
        {
            //get actual field list on the source table
            var columns = GetFieldList(dbName, table);

            if (columns.Count == 0)
            {
                //table doesn't exist
                throw new DoesNotExistException();
            }

            //get the table config object
            var t = Config.TableByName(originalTableName ?? table);

            //this will be null when copying a table that isn't in the config,
            //such as tblCTTableInfo or tblCTSchemaChange
            if (t == null)
            {
                return columns;
            }

            //only include columns in the column list if it is configured, plus the list of includeColumns
            return columns.Where(c => t.ColumnList == null
                || includeColumns.Contains(c.name, StringComparer.OrdinalIgnoreCase)
                || t.ColumnList.Contains(c.name, StringComparer.OrdinalIgnoreCase)
                ).ToList();
        }

        public MySqlDataReader ExecuteReader(string dbName, MySqlCommand cmd, int timeout = 1200)
        {
            var sourceConn = new MySqlConnection(buildConnString(dbName));
            sourceConn.Open();
            cmd.Connection = sourceConn;
            cmd.CommandTimeout = timeout;
            MySqlDataReader reader = cmd.ExecuteReader();
            return reader;
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

        private IList<DataTable> TransactionQuery(IList<MySqlCommand> commands, string dbName, int timeout)
        {
            var connStr = buildConnString(dbName);
            var tables = new List<DataTable>();
            using (var conn = new MySqlConnection(connStr))
            {
                conn.Open();
                var trans = conn.BeginTransaction();
                foreach (var cmd in commands)
                {
                    logger.Log(cmd.CommandText, LogLevel.Trace);
                    cmd.Transaction = trans;
                    cmd.CommandTimeout = timeout;
                    cmd.Connection = conn;
                    var ds = new DataSet();
                    var da = new MySqlDataAdapter(cmd);
                    da.Fill(ds);
                    tables.Add(ds.Tables[0]);
                }
                trans.Commit();
            }
            return tables;
        }

        private DataTable GetColumnInformationFromInformationSchema(string dbName, string tableName, string[] columns = null)
        {
            StringBuilder query = new StringBuilder("SELECT ");

            if (columns == null || columns.Count() < 1)
            {
                query.Append("*");
            }
            else
            {
                foreach(string col in columns)
                {
                    query.Append(" ");
                    query.Append(col);
                }
            }

            query.Append(" FROM information_schema.columns WHERE TABLE_SCHEMA = ");
            query.Append(dbName);
            query.Append(" AND TABLE_NAME = ");
            query.Append(tableName);
            query.AppendLine(" ORDER BY ORDINAL_POSITION;");

            return MySqlQuery(dbName, new MySqlCommand(query.ToString()));
        }
    }
}
