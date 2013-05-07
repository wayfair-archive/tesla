using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using System.Text;
using MySql.Data.MySqlClient;
using System.Data.SqlClient;
using TeslaSQL.DataUtils;

namespace TeslaSQL.DataCopy
{
    public class MySQLToMSSQLDataCopy : IDataCopy
    {
        private MySQLDataUtils sourceDataUtils;
        private MSSQLDataUtils destDataUtils;
        private Logger logger;

        public MySQLToMSSQLDataCopy(MySQLDataUtils sourceDataUtils, MSSQLDataUtils destDataUtils, Logger logger)
        {
            this.sourceDataUtils = sourceDataUtils;
            this.destDataUtils = destDataUtils;
            this.logger = logger;
        }

        private void CopyDataFromQuery(string sourceDB, string destDB, MySqlCommand cmd, string destinationTable, string destinationSchema = "dbo", int queryTimeout = 36000, int bulkCopyTimeout = 36000)
        {
            using (MySqlDataReader reader = sourceDataUtils.ExecuteReader(sourceDB, cmd, 1200))
            {
                destDataUtils.BulkCopy(reader, destDB, destinationSchema, destinationTable, bulkCopyTimeout);
            }
        }

        void CopyTable(string sourceDB, string sourceTableName, string schema, string destDB, int timeout, string destTableName = null, string originalTableName = null)
        {
            throw new NotImplementedException();
            //by default the dest table will have the same name as the source table
            destTableName = destTableName ?? sourceTableName;

            //drop table at destination and create from source schema
            CopyTableDefinition(sourceDB, sourceTableName, schema, destDB, destTableName, originalTableName);

            //if we are provided an original table name, get the configured fieldlists for it
            //if not (i.e. in the case of tblCTTableInfo in shardcoordinator) we can just use *
            string columnList;
            if (originalTableName == null)
            {
                columnList = "*";
            }
            else
            {
                var includeColumns = new List<string>() { "SYS_CHANGE_VERSION", "SYS_CHANGE_OPERATION" };
                var columns = sourceDataUtils.GetFieldList(sourceDB, sourceTableName, schema, originalTableName, includeColumns);
                columnList = string.Join(",", columns.Select(c => c.name));
            }

            //can't parametrize tablename or schema name but they have already been validated against the server so it's safe
            SqlCommand cmd = new SqlCommand(string.Format("SELECT {0} FROM [{1}].[{2}]", columnList, schema, sourceTableName));
            CopyDataFromQuery(sourceDB, destDB, cmd, destTableName, schema, timeout, timeout);
        }

        /// <summary>
        /// Copies the table form sourceDB.sourceTableName over to destDB.destTableName. Deletes the existing destination table first if it exists
        /// </summary>
        void CopyTableDefinition(string sourceDB, string sourceTableName, string schema, string destDB, string destTableName, string originalTableName = null)
        {
            throw new NotImplementedException();
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
