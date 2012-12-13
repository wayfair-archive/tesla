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

        private MSSQLDataUtils sourceDataUtils { get; set; }
        private MSSQLDataUtils destDataUtils { get; set; }

        public MSSQLToMSSQLDataCopy(MSSQLDataUtils sourceDataUtils, MSSQLDataUtils destDataUtils) {
            this.sourceDataUtils = sourceDataUtils;
            this.destDataUtils = destDataUtils;
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

        public void CopyTable(string sourceDB, string table, string schema, string destDB, int timeout, string destTableName = null) {
            //by default the dest table will have the same name as the source table
            destTableName = (destTableName == null) ? table : destTableName;

            //drop table at destination and create from source schema
            CopyTableDefinition(sourceDB, table, schema, destDB);

            //can't parametrize tablename or schema name but they have already been validated against the server so it's safe
            SqlCommand cmd = new SqlCommand("SELECT * FROM [" + schema + "].[" + table + "]");
            CopyDataFromQuery(sourceDB, destDB, cmd, table, schema, timeout, timeout);
        }


        /// <summary>
        /// Copies the schema of a table from one server to another, dropping it first if it exists at the destination.
        /// </summary>
        /// <param name="sourceDB">Source database name</param>
        /// <param name="table">Table name</param>
        /// <param name="schema">Table's schema</param>
        /// <param name="destDB">Destination database name</param>
        private void CopyTableDefinition(string sourceDB, string table, string schema, string destDB) {
            //script out the table at the source
            string createScript = ScriptTable(sourceDB, table, schema);
            SqlCommand cmd = new SqlCommand(createScript);

            //drop it if it exists at the destination
            bool didExist = destDataUtils.DropTableIfExists(destDB, table, schema);

            //create it at the destination
            int result = destDataUtils.SqlNonQuery(destDB, cmd);
        }


        /// <summary>
        /// Scripts out a table as CREATE TABLE
        /// </summary>
        /// <param name="server">Server identifier to connect to</param>
        /// <param name="dbName">Database name</param>
        /// <param name="table">Table name</param>
        /// <param name="schema">Table's schema</param>
        /// <returns>The CREATE TABLE script as a string</returns>
        private string ScriptTable(string dbName, string table, string schema) {
            //initialize scriptoptions variable
            ScriptingOptions scriptOptions = new ScriptingOptions();
            scriptOptions.ScriptBatchTerminator = true;
            scriptOptions.NoCollation = true;

            //get smo table object
            Table t_smo = sourceDataUtils.GetSmoTable(dbName, table, schema);

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
    }
}
