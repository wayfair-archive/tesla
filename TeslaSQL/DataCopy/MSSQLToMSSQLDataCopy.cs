using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using System.Text;
using System.Data.SqlClient;
using TeslaSQL.DataUtils;
namespace TeslaSQL.DataCopy {
    public class MSSQLToMSSQLDataCopy : IDataCopy {

        private MSSQLDataUtils sourceDataUtils;
        private MSSQLDataUtils destDataUtils;
        private Logger logger;

        public MSSQLToMSSQLDataCopy(MSSQLDataUtils sourceDataUtils, MSSQLDataUtils destDataUtils, Logger logger) {
            this.sourceDataUtils = sourceDataUtils;
            this.destDataUtils = destDataUtils;
            this.logger = logger;
        }

        /// <summary>
        /// Runs a query on the source server and copies the resulting data to the destination
        /// </summary>
        /// <param name="sourceDB">Source database name</param>
        /// <param name="cmd">Query to get the data from</param>
        /// <param name="destinationTable">Table to write to on the destination (must already exist)</param>
        /// <param name="queryTimeout">How long the query on the source can run for</param>
        /// <param name="bulkCopyTimeout">How long writing to the destination can take</param>
        private void CopyDataFromQuery(string sourceDB, string destDB, SqlCommand cmd, string destinationTable, string destinationSchema = "dbo", int queryTimeout = 36000, int bulkCopyTimeout = 36000) {
            using (SqlDataReader reader = sourceDataUtils.ExecuteReader(sourceDB, cmd, 1200)) {
                destDataUtils.BulkCopy(reader, destDB, destinationSchema, destinationTable, bulkCopyTimeout);
            }
        }

        public void CopyTable(string sourceDB, string sourceTableName, string schema, string destDB, int timeout, string destTableName = null, string originalTableName = null) {
            //by default the dest table will have the same name as the source table
            destTableName = destTableName ?? sourceTableName;

            //drop table at destination and create from source schema
            CopyTableDefinition(sourceDB, sourceTableName, schema, destDB, destTableName, originalTableName);

            //if we are provided an original table name, get the configured fieldlists for it
            //if not (i.e. in the case of tblCTTableInfo in shardcoordinator) we can just use *
            string columnList;
            if (originalTableName == null) {
                columnList = "*";
            } else {
                var includeColumns = new List<string>() { "SYS_CHANGE_VERSION", "SYS_CHANGE_OPERATION" };
                var columns = sourceDataUtils.GetFieldList(sourceDB, sourceTableName, schema, originalTableName, includeColumns);
                columnList = string.Join(",", columns.Select(c => c.name));
            }

            //can't parametrize tablename or schema name but they have already been validated against the server so it's safe
            SqlCommand cmd = new SqlCommand(string.Format("SELECT {0} FROM [{1}].[{2}]", columnList, schema, sourceTableName));
            CopyDataFromQuery(sourceDB, destDB, cmd, destTableName, schema, timeout, timeout);
        }


        /// <summary>
        /// Copies the schema of a table from one server to another, dropping it first if it exists at the destination.
        /// </summary>
        /// <param name="sourceDB">Source database name</param>
        /// <param name="sourceTableName">Table name</param>
        /// <param name="schema">Table's schema</param>
        /// <param name="destDB">Destination database name</param>
        public void CopyTableDefinition(string sourceDB, string sourceTableName, string schema, string destDB, string destTableName, string originalTableName = null) {
            //script out the table at the source
            string createScript = sourceDataUtils.ScriptTable(sourceDB, sourceTableName, schema, originalTableName);
            createScript = createScript.Replace(sourceTableName, destTableName);
            SqlCommand cmd = new SqlCommand(createScript);

            //drop it if it exists at the destination
            destDataUtils.DropTableIfExists(destDB, destTableName, schema);

            //create it at the destination
            destDataUtils.SqlNonQuery(destDB, cmd);
        }
    }
}
