using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using System.Text;
using MySql.Data.MySqlClient;
using System.Data;
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
            using (IDataReader reader = sourceDataUtils.ExecuteReader(sourceDB, cmd, 1200))
            {
                destDataUtils.BulkCopy(reader, destDB, destinationSchema, destinationTable, bulkCopyTimeout);
            }
        }

        public void CopyTable(string sourceDB, string sourceTableName, string schema, string destDB, int timeout, string destTableName = null, string originalTableName = null)
        {
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
                var columns = sourceDataUtils.GetFieldList(sourceDB, sourceTableName, schema);
                columnList = string.Join(",", columns.Select(c => c.name));
            }

            //can't parametrize tablename or schema name but they have already been validated against the server so it's safe
            var cmd = new MySqlCommand(string.Format("SELECT {0} FROM {1}", columnList, sourceTableName));
            CopyDataFromQuery(sourceDB, destDB, cmd, destTableName, schema, timeout, timeout);
        }

        /// <summary>
        /// Copies the table from sourceDB.sourceTableName over to destDB.destTableName. Deletes the existing destination table first if it exists
        /// </summary>
        public void CopyTableDefinition(string sourceDB, string sourceTableName, string schema, string destDB, string destTableName, string originalTableName = null)
        {
            List<TColumn> columns = sourceDataUtils.GetFieldList(sourceDB, sourceTableName);
            List<String> pkColumnNames = new List<String>();

            StringBuilder script = new StringBuilder();
            String type;
            script.Append("CREATE TABLE [");
            script.Append(schema);
            script.Append("].[");
            script.Append(destTableName);
            script.AppendLine("](");

            foreach (TColumn column in columns)
            {
                script.Append('[');
                script.Append(column.name);
                script.Append("] [");
                type = DataType.MapDataType(SqlFlavor.MySQL, SqlFlavor.MSSQL, column.dataType.BaseType);
                script.Append(type);
                script.Append(']');

                if (column.isPk)
                {
                    pkColumnNames.Add(column.name);
                }

                switch (type)
                {
                    case "varchar":
                    case "nvarchar":
                    case "char":
                    case "nchar":
                    case "text":
                    case "ntext":
                        script.Append('(');
                        script.Append(column.dataType.CharacterMaximumLength != null ? column.dataType.CharacterMaximumLength.ToString() : "MAX");
                        script.Append(')');
                        break;
                    case "decimal":
                    case "numeric":
                    case "real":
                    case "float":
                        script.Append('(');
                        script.Append(column.dataType.NumericPrecision ?? 18);
                        if (column.dataType.NumericScale != null)
                        {
                            script.Append(',');
                            script.Append(column.dataType.NumericScale);
                        }
                        script.Append(')');
                        break;
                    default:
                        break;
                }

                if (column.isNullable)
                {
                    script.Append(" NULL");
                }
                else
                {
                    script.Append(" NOT NULL");
                }

                if (column != columns.Last())
                {
                    script.AppendLine(",");
                }
                else
                {
                    script.AppendLine();
                }
            }

            if (pkColumnNames.Count > 0)
            {
                script.Append(", CONSTRAINT pk_");
                script.Append(destTableName);
                script.Append(" PRIMARY KEY (");
                if (pkColumnNames.Count == 1)
                {
                    script.Append(pkColumnNames.FirstOrDefault());
                }
                else
                {
                    script.Append(pkColumnNames.Aggregate((i, j) => i + ", " + j));
                }
                script.Append(')');
            }
            //drop it if it exists at the destination
            destDataUtils.DropTableIfExists(destDB, destTableName, schema);

            //create it at the destination
            destDataUtils.SqlNonQuery(destDB, new SqlCommand(script.ToString()));
        }

    }
}
