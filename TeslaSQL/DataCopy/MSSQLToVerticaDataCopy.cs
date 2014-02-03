using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using TeslaSQL.DataUtils;
using System.Data.SqlClient;
using System.Data;
// using System.Data.OleDb;
using Vertica.Data.VerticaClient;
using System.Diagnostics;
using System.Text.RegularExpressions;
using System.IO;

namespace TeslaSQL.DataCopy {
    public class MSSQLToVerticaDataCopy : IDataCopy {

        private MSSQLDataUtils sourceDataUtils;
        private VerticaDataUtils destDataUtils;
        private Logger logger;
        private string vServer;
        private string vUser;
        // private string vPrivateKeyPath;

        public MSSQLToVerticaDataCopy(MSSQLDataUtils sourceDataUtils, VerticaDataUtils destDataUtils, Logger logger, string vServer, string vUser) {
            this.sourceDataUtils = sourceDataUtils;
            this.destDataUtils = destDataUtils;
            this.logger = logger;
            this.vServer = vServer;
            this.vUser = vUser;
            // this.vPrivateKeyPath = vPrivateKeyPath;
        }

        private static void CreateDirectoryIfNotExists(string directory) {
            DirectoryInfo dir = new DirectoryInfo(directory);
            if (!dir.Exists) {
                dir.Create();
            }
        }

        /// <summary>
        /// Runs a query on the source server and copies the resulting data to the destination
        /// </summary>
        /// <param name="fileName">Name of data file to copy from</param>
        /// <param name="destinationTable">Table to write to on the destination (must already exist)</param>
        /// <param name="bulkCopyTimeout">How long writing to the destination can take</param>
        private void CopyDataFromQuery(string fileName, string destDB, string destinationTable, int bulkCopyTimeout = 36000)
        {
            destDataUtils.BulkCopy(fileName, destDB, destinationTable, bulkCopyTimeout);
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
            if (bcpSelect.Length > 3800) {
                //BCP commands fail if their text length is over 4000 characters, and we need some padding
                //drop view CTVWtablename if exists
                //create view CTVWtablename AS $bcpSelect
                string viewName = "CTVW" + sourceTableName;
                sourceDataUtils.RecreateView(sourceDB, viewName, bcpSelect);
                bcpSelect = string.Format("SELECT * FROM {0}..{1}", sourceDB, viewName);
            }
            string bcpDirectory = Config.BcpPath.TrimEnd('\\') + @"\" + sourceDB.ToLower();
            string bcpFileName = bcpDirectory + @"\" + destTableName + ".txt";
            CreateDirectoryIfNotExists(bcpDirectory);
            string password = new cTripleDes().Decrypt(Config.RelayPassword);
            // var bcpArgs = string.Format(@"""{0}"" queryout {1}\{2}.txt -c -S{3} -U {4} -P {5} -t""|"" -r\n",
            var bcpArgs = string.Format(@"""{0}"" queryout {1} -c -S{2} -U {3} -P {4} -t""|"" -r\n",
                                            bcpSelect,
                                            bcpFileName,
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
            bcp.OutputDataReceived += delegate(object sender, DataReceivedEventArgs e) {
                lock (outputBuilder) {
                    outputBuilder.AppendLine(e.Data);
                }
            };
            bcp.ErrorDataReceived += delegate(object sender, DataReceivedEventArgs e) {
                lock (errorBuilder) {
                    errorBuilder.AppendLine(e.Data);
                }
            };
            bcp.Start();
            bcp.BeginOutputReadLine();
            bcp.BeginErrorReadLine();
            bool status = bcp.WaitForExit(Config.DataCopyTimeout * 1000);
            if (!status) {
                bcp.Kill();
                throw new Exception("BCP timed out for table " + sourceTableName);
            }
            if (bcp.ExitCode != 0) {
                string err = outputBuilder + "\r\n" + errorBuilder;
                logger.Log(err, LogLevel.Critical);
                throw new Exception("BCP error: " + err);
            }
            logger.Log("BCP successful for " + destTableName, LogLevel.Trace);

            // TODO: use COPY on Vertica
            string verticaCopyDirectory = Config.VerticaCopyPath.TrimEnd('/') + "/" + sourceDB.ToLower();
            string verticaCopyFileName = verticaCopyDirectory + "/" + destTableName + ".txt";
            CopyDataFromQuery(verticaCopyFileName, destDB, destTableName, timeout);
            logger.Log("Vertica COPY successful for table " + destTableName, LogLevel.Trace);
        }

        struct Col {
            public string name;
            public string typeName;
            public DataType dataType;
            public bool isPk;

            public Col(string name, string typeName, DataType dataType, bool isPk) {
                this.name = name;
                this.typeName = typeName;
                this.dataType = dataType;
                this.isPk = isPk;
            }
            /// <summary>
            /// Return an expression for use in BCP command
            /// </summary>
            public string ColExpression() {
                if (dataType.IsStringType()) {
                    //This nasty expression should handle everything that could be in a string which would break nzload.
                    string toFormat = @"ISNULL(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(cast({0}";
                    toFormat += @" as varchar(max)),'\', '\\'),CHAR(13)+CHAR(10),' '),'\""', '\'+'\""'),";
                    toFormat += @"'|', ','),CHAR(10), ' '), 'NULL', '\NULL'), 'NULL') as {0}";
                    return string.Format(toFormat, name);
                }
                return name;
            }
            public override string ToString() {
                return name + " " + typeName;
            }
        }

        // TODO: WORK
        public void CopyTableDefinition(string sourceDB, string sourceTableName, string schema, string destDB, string destTableName, string originalTableName = null) {
            var cols = GetColumns(sourceDB, sourceTableName, schema, originalTableName ?? sourceTableName);

            // get the table config object
            var table = Config.TableByName(originalTableName);
            List<TableConf> tableConfs = new List<TableConf>() { table };
            Dictionary<TableConf, IList<TColumn>> allColumnsByTable = sourceDataUtils.GetAllFields(sourceDB, tableConfs.ToDictionary(t => t, t => t.Name));
            // When we create this string we should check if the final string ends up being a database..tablename (two dots). In Vertica we want one dot only
            // The [PK] syntax should work for both a simple PK (one column only) and a composite PK (more than one column)
            // KSAFE should be another XML parameter at the table level in the config files for the subscriber side only
            // TODO: replace [PK] with the PK definition
            string vCreate = string.Format(
                @"CREATE TABLE {0}.{1}
                            (
                                {2} NOT NULL,
                                PRIMARY KEY ({3})
                            )
                ORDER BY {3}
                SEGMENTED BY HASH({3})
                ALL NODES
                KSAFE {4};",
                destDB, // for Vertica, the "database" becomes the "schema"
                destTableName,
                string.Join(",", cols),
                string.Join(",", cols.Where(c => c.isPk).Select(c => c.name)),
                Config.VerticaKsafe);
            logger.Log(vCreate, LogLevel.Trace);
            destDataUtils.DropTableIfExists(destDB, destTableName, schema);
            var cmd = new VerticaCommand(vCreate);
            destDataUtils.SqlNonQuery(cmd);

        }

        // TODO: WORK
        private List<Col> GetColumns(string sourceDB, string sourceTableName, string schema, string originalTableName) {
            //get actual field list on the source table
            var includeColumns = new List<string>() { "SYS_CHANGE_VERSION", "SYS_CHANGE_OPERATION" };
            var columns = sourceDataUtils.GetFieldList(sourceDB, sourceTableName, schema, originalTableName, includeColumns);
            //get the table config object
            var table = Config.TableByName(originalTableName);

            // TODO: verify and verify
            /*
            List<TableConf> tableConfs = new List<TableConf>(){table};
            Dictionary<TableConf, IList<TColumn>> allColumnsByTable = sourceDataUtils.GetAllFields(sourceDB, tableConfs.ToDictionary(t => t, t => t.Name));
             * */

            var cols = new List<Col>();
            bool isPrimaryKey;
            foreach (TColumn col in columns) {
                string typeName = col.dataType.BaseType;
                isPrimaryKey = false;

                if (table.columns.Count > 0)
                {
                    // if table.columns is populated
                    try
                    {
                        isPrimaryKey = table.columns.Where(c => (c.name != "SYS_CHANGE_VERSION" && c.name != "SYS_CHANGE_OPERATION")).First(c => c.name == col.name).isPk;
                    }
                    catch (Exception e)
                    {
                        logger.Log("Cannot determine whether column [" + col.name + "] is PK", LogLevel.Warn);
                        isPrimaryKey = false;
                    }
                }

                /*
                try
                {
                    isPrimaryKey = allColumnsByTable[table].First(c => c.name == col.name).isPk;
                }
                catch (Exception e)
                {
                    throw new Exception("Cannot determine whether the column [" + col.name + "] is primary key");
                }
                 * */

                /*
                try
                {
                    isPrimaryKey = table.columns.First(c => c.name == col.name).isPk;
                }
                catch (Exception e)
                {
                    isPrimaryKey = false;
                }
                 * */

                ColumnModifier mod = null;
                //see if there are any column modifiers which override our length defaults
                ColumnModifier[] modifiers = table.ColumnModifiers;
                if (modifiers != null) {
                    IEnumerable<ColumnModifier> mods = modifiers.Where(c => ((c.columnName == col.name) && (c.type == "ShortenField")));
                    mod = mods.FirstOrDefault();
                }

                // try to map the source data type to the destination data type 
                string modDataType = DataType.MapDataType(SqlFlavor.MSSQL, SqlFlavor.Vertica, typeName);
                if (typeName != modDataType) {
                    if (mod != null && Regex.IsMatch(modDataType, @".*\(\d+\)$")) {
                        modDataType = Regex.Replace(modDataType, @"\d+", mod.length.ToString());
                    }
                    // cols.Add(new Col(VerticaDataUtils.MapReservedWord(col.name), modDataType, col.dataType, col.isPk));
                    cols.Add(new Col(VerticaDataUtils.MapReservedWord(col.name), modDataType, col.dataType, isPrimaryKey));
                    continue;
                }

                if (col.dataType.UsesMaxLength())
                {
                    if (mod != null)
                    {
                        typeName += "(" + mod.length + ")";
                    }
                    else if (Config.NetezzaStringLength > 0)
                    {
                        // TODO: Config.NetezzaStringLength?
                        typeName += "(" + ((col.dataType.CharacterMaximumLength > Config.NetezzaStringLength
                            || col.dataType.CharacterMaximumLength < 1) ? Config.NetezzaStringLength : col.dataType.CharacterMaximumLength) + ")";
                    }
                    else
                    {
                        // TODO: 16000
                        typeName += "(" + (col.dataType.CharacterMaximumLength > 0 ? col.dataType.CharacterMaximumLength : 16000) + ")";
                    }
                }
                else if (col.dataType.UsesPrecisionScale())
                {
                    typeName += "(" + col.dataType.NumericPrecision + "," + col.dataType.NumericScale + ")";
                }
                cols.Add(new Col(VerticaDataUtils.MapReservedWord(col.name), typeName, col.dataType, isPrimaryKey));
                // cols.Add(new Col(VerticaDataUtils.MapReservedWord(col.name), typeName, col.dataType, col.isPk));
            }
            return cols;
        }
    }
}
