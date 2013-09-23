using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using System.Text;
using System.Data.SqlClient;
using TeslaSQL.DataUtils;
using MySql.Data.MySqlClient;
using System.Diagnostics;
using System.Text.RegularExpressions;
using System.IO;

namespace TeslaSQL.DataCopy {
    public class MSSQLToMySQLDataCopy : IDataCopy {

        private MSSQLDataUtils sourceDataUtils;
        private MySQLDataUtils destDataUtils;
        private Logger logger;

        public MSSQLToMySQLDataCopy(MSSQLDataUtils sourceDataUtils, MySQLDataUtils destDataUtils, Logger logger) {
            this.sourceDataUtils = sourceDataUtils;
            this.destDataUtils = destDataUtils;
            this.logger = logger;
        }

        private static void CreateDirectoryIfNotExists(string directory)
        {
            DirectoryInfo dir = new DirectoryInfo(directory);
            if (!dir.Exists)
            {
                dir.Create();
            }
        }

        public void CopyTable(string sourceDB, string sourceTableName, string schema, string destDB, int timeout, string destTableName = null, string originalTableName = null) {
            //by default the dest table will have the same name as the source table
            destTableName = (destTableName == null) ? sourceTableName : destTableName;
            originalTableName = originalTableName ?? sourceTableName;

            //drop table at destination and create from source schema
            CopyTableDefinition(sourceDB, sourceTableName, schema, destDB, destTableName, originalTableName);

            var cols = GetColumns(sourceDB, sourceTableName, schema, originalTableName);
            var bcpSelect = string.Format("SELECT {0} FROM {1}..{2};",
                                          string.Join(",", cols.Select(col => col.ColExpression())),
                                          sourceDB, sourceTableName);
            if (bcpSelect.Length > 3800)
            {
                //BCP commands fail if their text length is over 4000 characters, and we need some padding
                //drop view CTVWtablename if exists
                //create view CTVWtablename AS $bcpSelect
                string viewName = "CTVW" + sourceTableName;
                sourceDataUtils.RecreateView(sourceDB, viewName, bcpSelect);
                bcpSelect = string.Format("SELECT * FROM {0}..{1}", sourceDB, viewName);
            }
            string directory = Config.BcpPath.TrimEnd('\\') + @"\" + sourceDB.ToLower();
            CreateDirectoryIfNotExists(directory);
            string password = new cTripleDes().Decrypt(Config.RelayPassword);
            var bcpArgs = string.Format(@"""{0}"" queryout {1}\{2}.txt -c -S{3} -U {4} -P {5} -t""|"" -r""&_+-!/=/=""",
                                            bcpSelect,
                                            directory,
                                            destTableName,
                                            Config.RelayServer,
                                            Config.RelayUser,
                                            password
                                            );
            logger.Log("BCP command: bcp " + bcpArgs.Replace(password, "********"), LogLevel.Trace);
            var outputBuilder = new StringBuilder();
            var errorBuilder = new StringBuilder();
            var bcp = new Process();
            bcp.StartInfo.FileName = "bcp";
            bcp.StartInfo.Arguments = bcpArgs;
            bcp.StartInfo.UseShellExecute = false;
            bcp.StartInfo.RedirectStandardError = true;
            bcp.StartInfo.RedirectStandardOutput = true;
            bcp.OutputDataReceived += delegate(object sender, DataReceivedEventArgs e)
            {
                lock (outputBuilder)
                {
                    outputBuilder.AppendLine(e.Data);
                }
            };
            bcp.ErrorDataReceived += delegate(object sender, DataReceivedEventArgs e)
            {
                lock (errorBuilder)
                {
                    errorBuilder.AppendLine(e.Data);
                }
            };
            bcp.Start();
            bcp.BeginOutputReadLine();
            bcp.BeginErrorReadLine();
            bool status = bcp.WaitForExit(Config.DataCopyTimeout * 1000);
            if (!status)
            {
                bcp.Kill();
                throw new Exception("BCP timed out for table " + sourceTableName);
            }
            if (bcp.ExitCode != 0)
            {
                string err = outputBuilder + "\r\n" + errorBuilder;
                logger.Log(err, LogLevel.Critical);
                throw new Exception("BCP error: " + err);
            }
            logger.Log("BCP successful for " + sourceTableName, LogLevel.Trace);
            string filename = sourceDB.ToLower() + "/" + destTableName + ".txt";
            destDataUtils.BulkCopy(filename, destDB, destTableName, 60 * 10, cols);
            
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
            string createScript = sourceDataUtils.ScriptTable(sourceDB, sourceTableName, schema, originalTableName, SqlFlavor.MySQL);
            createScript = createScript.Replace(sourceTableName, destTableName);
            MySqlCommand cmd = new MySqlCommand(createScript);
         
            //drop it if it exists at the destination
            destDataUtils.DropTableIfExists(destDB, destTableName, schema);

            //create it at the destination
            destDataUtils.MySqlNonQuery(destDB, cmd);
        }


        public struct Col
        {
            public string name;
            public string typeName;
            public DataType dataType;

            public Col(string name, string typeName, DataType dataType)
            {
                this.name = name;
                this.typeName = typeName;
                this.dataType = dataType;
            }
            /// <summary>
            /// Return an expression for use in BCP command
            /// </summary>
            public string ColExpression()
            {
                return name;
            }
            public override string ToString()
            {
                return name + " " + typeName;
            }
        }

        private List<Col> GetColumns(string sourceDB, string sourceTableName, string schema, string originalTableName)
        {
            //get actual field list on the source table
            var includeColumns = new List<string>() { "SYS_CHANGE_VERSION", "SYS_CHANGE_OPERATION" };
            var columns = sourceDataUtils.GetFieldList(sourceDB, sourceTableName, schema, originalTableName, includeColumns);
            //get the table config object
            var table = Config.TableByName(originalTableName);

            var cols = new List<Col>();
            foreach (TColumn col in columns)
            {
                string typeName = col.dataType.BaseType;

                ColumnModifier mod = null;
                //see if there are any column modifiers which override our length defaults
                ColumnModifier[] modifiers = table.ColumnModifiers;
                if (modifiers != null)
                {
                    IEnumerable<ColumnModifier> mods = modifiers.Where(c => ((c.columnName == col.name) && (c.type == "ShortenField")));
                    mod = mods.FirstOrDefault();
                }

                string modDataType = DataType.MapDataType(SqlFlavor.MSSQL, SqlFlavor.MySQL, typeName);
                if (typeName != modDataType)
                {
                    if (mod != null && Regex.IsMatch(modDataType, @".*\(\d+\)$"))
                    {
                        modDataType = Regex.Replace(modDataType, @"\d+", mod.length.ToString());
                    }
                    cols.Add(new Col(col.name, modDataType, col.dataType));
                    continue;
                }

                if (col.dataType.UsesMaxLength())
                {
                    if (mod != null)
                    {
                        typeName += "(" + mod.length + ")";
                    }
                }
                else if (col.dataType.UsesPrecisionScale())
                {
                    typeName += "(" + col.dataType.NumericPrecision + "," + col.dataType.NumericScale + ")";
                }
                cols.Add(new Col(col.name, typeName, col.dataType));
            }
            return cols;
        }
    }
}
