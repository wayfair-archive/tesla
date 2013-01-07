﻿using System;
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
        public void ApplyTableChanges(TableConf table, TableConf archiveTable, string dbName, long ctid, string CTDBName) {
            var cmds = new List<InsertDelete>();
            cmds.Add(BuildApplyCommand(table, dbName, CTDBName, ctid));
            if (archiveTable != null) {
                cmds.Add(BuildApplyCommand(archiveTable, dbName, CTDBName, ctid));
            }
            var connStr = buildConnString(dbName);
            using (var conn = new OleDbConnection(connStr)) {
                conn.Open();
                var trans = conn.BeginTransaction();
                foreach (var id in cmds) {
                    id.delete.Transaction = trans;
                    id.delete.Connection = conn;
                    logger.Log(id.delete.CommandText, LogLevel.Trace);
                    int numRows = id.delete.ExecuteNonQuery();
                    logger.Log("Rows deleted: " + numRows, LogLevel.Info);
                    id.insert.Transaction = trans;
                    id.insert.Connection = conn;
                    logger.Log(id.insert.CommandText, LogLevel.Trace);
                    numRows = id.insert.ExecuteNonQuery();
                    logger.Log("Rows deleted: " + numRows, LogLevel.Info);
                }
                trans.Commit();
            }
        }

        class InsertDelete {
            public readonly OleDbCommand insert;
            public readonly OleDbCommand delete;
            public InsertDelete(OleDbCommand insert, OleDbCommand delete) {
                this.insert = insert;
                this.delete = delete;
            }
        }
        private InsertDelete BuildApplyCommand(TableConf table, string dbName, string CTDBName, long ctid) {
            string delete = string.Format(@"DELETE FROM {0} P
                                          WHERE EXISTS (SELECT 1 FROM {1}..{2} CT WHERE {3});",
                                          table.Name, CTDBName, table.ToCTName(ctid), table.pkList);

            string insert = string.Format(@"INSERT INTO {0} ({1}) 
                              SELECT {1} FROM {2}..{3} CT WHERE NOT EXISTS (SELECT 1 FROM {0} P WHERE {4}) AND CT.sys_change_operation IN ( 'I', 'U' );",
                                          table.Name, table.simpleColumnList, CTDBName, table.ToCTName(ctid), table.pkList);
            var deleteCmd = new OleDbCommand(delete);
            var insertCmd = new OleDbCommand(insert);
            return new InsertDelete(insertCmd, deleteCmd);
        }

        public void CopyIntoHistoryTable(ChangeTable t, string dbName) {
            string sql;
            if (CheckTableExists(dbName, t.historyName, t.schemaName)) {
                logger.Log("table " + t.historyName + " already exists; selecting into it", LogLevel.Trace);
                sql = string.Format("INSERT INTO {0} SELECT {1} AS CTHistID, * FROM {2}", t.historyName, t.ctid, t.ctName);
                logger.Log(sql, LogLevel.Debug);
            } else {
                logger.Log("table " + t.historyName + " does not exist, inserting into it", LogLevel.Trace);
                sql = string.Format("CREATE TABLE {0} AS SELECT {1} AS CTHistID, * FROM {2}", t.historyName,t.ctid, t.ctName);
                logger.Log(sql, LogLevel.Debug);
            }
            var cmd = new OleDbCommand(sql);
            SqlNonQuery(dbName, cmd);
        }
        public void RenameColumn(TableConf t, string dbName, string schema, string table, string columnName, string newColumnName) {
            logger.Log("Please check pending schema changes to be applied on Netezza for " + dbName + "." + schema + "." + table + " on " + config.slave, LogLevel.Error);
        }
        public void ModifyColumn(TableConf t, string dbName, string schema, string table, string columnName, string dataType) {
            logger.Log("Please check pending schema changes to be applied on Netezza for " + dbName + "." + schema + "." + table + " on " + config.slave, LogLevel.Error);
        }
        public void AddColumn(TableConf t, string dbName, string schema, string table, string columnName, string dataType) {
            if (CheckColumnExists(dbName, schema, table, columnName)) {
                return;
            }
            dataType = DataType.MapDataType(config.relayType.Value, SqlFlavor.Netezza, dataType);
            string sql = string.Format("ALTER TABLE {0} ADD {1} {2}; GROOM TABLE {0} VERSIONS;", table, columnName, dataType);
            var cmd = new OleDbCommand(sql);
            SqlNonQuery(dbName, cmd);
            RefreshViews(dbName, table);
        }

        private void RefreshViews(string dbName, string tableName) {
            var refresh = config.refreshViews.Where(r => r.db.ToLower() == dbName.ToLower() && r.tableName.ToLower() == tableName.ToLower()).FirstOrDefault();
            if (refresh == null) {
                return;
            }
            string sql = refresh.command;
            var cmd = new OleDbCommand(sql);
            try {
                SqlNonQuery(dbName, cmd);
            } catch (Exception e) {
                throw new Exception("Please check any pending schema changes to be applied on Netezza before refreshing the view::" + dbName + ".." + refresh.viewName);
            }

        }

        public void DropColumn(TableConf t, string dbName, string schema, string table, string columnName) {
            if (!CheckColumnExists(dbName, schema, table, columnName)) {
                return;
            }
            string sql = string.Format("ALTER TABLE {0} DROP COLUMN {1} RESTRICT; GROOM TABLE {0} VERSIONS;", table, columnName);
            var cmd = new OleDbCommand(sql);
            SqlNonQuery(dbName, cmd);
            RefreshViews(dbName, table);
        }

        public bool CheckColumnExists(string dbName, string schema, string table, string column) {
            string sql = string.Format("SELECT 1 FROM _v_relation_column_def WHERE LOWER(name) = LOWER('{0}') AND type='TABLE' AND LOWER(attname) = LOWER('{1}')",
                                       table, column);
            var cmd = new OleDbCommand(sql);
            var res = SqlQuery(dbName, cmd);
            return res.Rows.Count > 0;
        }


        #region unimplemented



        public DataRow GetLastCTBatch(string dbName, AgentType agentType, string slaveIdentifier = "") {
            throw new NotImplementedException("Netezza is only supported as a slave!");
        }


        public IEnumerable<string> GetIntersectColumnList(string dbName, string table1, string schema1, string table2, string schema2) {
            throw new NotImplementedException("Not sure if we need this yet!");
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

