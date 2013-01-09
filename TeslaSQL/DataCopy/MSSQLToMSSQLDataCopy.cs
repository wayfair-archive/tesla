using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using System.Text;
using System.Data.SqlClient;
using Microsoft.SqlServer.Management.Smo;
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

        public void CopyTable(string sourceDB, string sourceTableName, string schema, string destDB, int timeout, string destTableName = null, Int64? CTID = null) {
            //by default the dest table will have the same name as the source table
            destTableName = (destTableName == null) ? sourceTableName : destTableName;

            //drop table at destination and create from source schema
            CopyTableDefinition(sourceDB, sourceTableName, schema, destDB, destTableName);

            //can't parametrize tablename or schema name but they have already been validated against the server so it's safe
            SqlCommand cmd = new SqlCommand("SELECT * FROM [" + schema + "].[" + sourceTableName + "]");
            CopyDataFromQuery(sourceDB, destDB, cmd, destTableName, schema, timeout, timeout);
        }


        /// <summary>
        /// Copies the schema of a table from one server to another, dropping it first if it exists at the destination.
        /// </summary>
        /// <param name="sourceDB">Source database name</param>
        /// <param name="sourceTableName">Table name</param>
        /// <param name="schema">Table's schema</param>
        /// <param name="destDB">Destination database name</param>
        public void CopyTableDefinition(string sourceDB, string sourceTableName, string schema, string destDB, string destTableName, Int64? CTID = null) {
            //script out the table at the source
            string createScript = sourceDataUtils.ScriptTable(sourceDB, sourceTableName, schema);
            createScript = createScript.Replace(sourceTableName, destTableName);
            SqlCommand cmd = new SqlCommand(createScript);

            //drop it if it exists at the destination
            bool didExist = destDataUtils.DropTableIfExists(destDB, destTableName, schema);

            //create it at the destination
            int result = destDataUtils.SqlNonQuery(destDB, cmd);
        }
    }
}
