#region Using Statements
using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Text;
using System.Linq;
using System.Data;
using System.Diagnostics;
using Xunit;
using TeslaSQL.DataUtils;
using System.Data.SqlClient;
#endregion


namespace TeslaSQL.Agents {
    //each agent (master, slave, etc.) should inherit this
    public abstract class Agent {

        protected IDataUtils sourceDataUtils;
        protected IDataUtils destDataUtils;

        protected Logger logger;

        protected Agent() {
            //this constructor is only used by for running unit tests
            this.logger = new Logger(null, null, null, "");
        }

        protected Agent(IDataUtils sourceDataUtils, IDataUtils destDataUtils, Logger logger) {
            this.sourceDataUtils = sourceDataUtils;
            this.destDataUtils = destDataUtils;
            this.logger = logger;
        }

        public abstract void Run();

        public abstract void ValidateConfig();


        /// <summary>
        /// Set field list values for each table in the config
        /// </summary>
        /// <param name="Database">Database name to run on</param>
        /// <param name="tableConfArray">Array of tableconf objects to loop through and set field lists on</param>
        public virtual void SetFieldLists(string database, IEnumerable<TableConf> tableConfArray, IDataUtils dataUtils) {
            foreach (TableConf t in tableConfArray) {
                try {
                    List<TColumn> columns = dataUtils.GetFieldList(database, t.Name, t.SchemaName);
                    SetFieldList(t, columns);
                } catch (Exception e) {
                    HandleException(e, t, "Error setting field lists for table " + t.SchemaName + "." + t.Name + ": " + e.Message + " - Stack Trace:" + e.StackTrace);
                }
            }
        }

        /// <summary>
        /// Set several field lists on a TableConf object using its config and an smo table object.
        /// </summary>
        /// <param name="t">A table configuration object</param>
        /// <param name="fields">Dictionary of field names with a bool for whether they are part of the primary key</param>
        public void SetFieldList(TableConf t, IEnumerable<TColumn> fields) {
            Stopwatch st = new Stopwatch();
            st.Start();
            t.columns.Clear();
            t.columns = fields.Where(c => t.ColumnList == null || t.ColumnList.Contains(c.name, StringComparer.OrdinalIgnoreCase)).ToList();
            st.Stop();
            logger.Log(new { message = "SetFieldList Elapsed time : " + st.ElapsedMilliseconds, Table = t.FullName }, LogLevel.Trace);
        }

        protected void HandleException(Exception e, TableConf table, string message = "") {
            message = "Table: " + table.FullName + "; StopOnError: " + table.StopOnError + (message.Length > 0 ? "\r\n" + message : "");
            logger.Log(e, message);
            if (table.StopOnError) {
                throw e;
            }
        }

        /// <summary>
        /// Gets ChangesCaptured object based on row counts in CT tables
        /// </summary>
        /// <param name="tables">Array of table config objects</param>
        /// <param name="sourceCTDB">CT database name</param>
        /// <param name="CTID">CT batch id</param>
        public Dictionary<string, Int64> GetRowCounts(IEnumerable<TableConf> tables, string sourceCTDB, Int64 CTID) {
            Dictionary<string, Int64> rowCounts = new Dictionary<string, Int64>();

            foreach (TableConf t in tables) {
                logger.Log(new { message = "Getting rowcount", Table = t.SchemaName + "." + t.ToCTName(CTID) }, LogLevel.Trace);
                try {
                    rowCounts.Add(t.FullName, sourceDataUtils.GetTableRowCount(sourceCTDB, t.ToCTName(CTID), t.SchemaName));
                    logger.Log("Successfully retrieved rowcount of " + rowCounts[t.FullName], LogLevel.Trace);
                } catch (DoesNotExistException) {
                    logger.Log("CT table does not exist, using rowcount of 0", LogLevel.Trace);
                    rowCounts.Add(t.FullName, 0);
                }
            }
            return rowCounts;
        }

        protected void PublishTableInfo(IEnumerable<TableConf> tableConf, string relayDB, IDictionary<string, long> changesCaptured, Int64 CTID) {
            logger.Log(new { message = "Creating TableInfo table", CTID = CTID }, LogLevel.Info);
            destDataUtils.CreateTableInfoTable(relayDB, CTID);
            foreach (var t in tableConf) {
                logger.Log(new { message = "Publishing info", Table = t.Name }, LogLevel.Trace);
                try {
                    destDataUtils.PublishTableInfo(relayDB, t, CTID, changesCaptured[t.FullName]);
                } catch (Exception e) {
                    HandleException(e, t, "Error publishing TableInfo for table " + t.SchemaName + "." + t.Name + ": " + e.Message + " - Stack Trace:" + e.StackTrace);
                }
            }
        }
    }
}