using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using TeslaSQL.DataUtils;
using System.Data.SqlClient;
using Microsoft.SqlServer.Management.Smo;
using System.Data;
using System.Data.OleDb;
using System.Diagnostics;
using System.Text.RegularExpressions;
using System.IO;

namespace TeslaSQL.DataCopy {
    public class MSSQLToNetezzaDataCopy : IDataCopy {

        private MSSQLDataUtils sourceDataUtils;
        private NetezzaDataUtils destDataUtils;
        private Logger logger;
        private string nzServer;
        private string nzUser;
        private string nzPrivateKeyPath;

        public MSSQLToNetezzaDataCopy(MSSQLDataUtils sourceDataUtils, NetezzaDataUtils destDataUtils, Logger logger, string nzServer, string nzUser, string nzPrivateKeyPath) {
            this.sourceDataUtils = sourceDataUtils;
            this.destDataUtils = destDataUtils;
            this.logger = logger;
            this.nzServer = nzServer;
            this.nzUser = nzUser;
            this.nzPrivateKeyPath = nzPrivateKeyPath;
        }

        private void CreateDirectoryIfNotExists(string directory) {
            DirectoryInfo dir = new DirectoryInfo(directory);
            if (!dir.Exists) {
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
            if (bcpSelect.Length > 3800) {
                //BCP commands fail if their text length is over 4000 characters, and we need some padding
                //drop view CTVWtablename if exists
                //create view CTVWtablename AS $bcpSelect
                string viewName = "CTVW" + sourceTableName;
                sourceDataUtils.RecreateView(sourceDB, viewName, bcpSelect);
                bcpSelect = string.Format("SELECT * FROM {0}..{1}", sourceDB, viewName);
            }
            string directory = Config.bcpPath.TrimEnd('\\') + @"\" + sourceDB.ToLower();
            CreateDirectoryIfNotExists(directory);
            var bcpArgs = string.Format(@"""{0}"" queryout {1}\{2}.txt -T -c -S{3} -t""|"" -r\n",
                                            bcpSelect,
                                            directory,
                                            destTableName,
                                            Config.relayServer
                                            );
            logger.Log(bcpArgs, LogLevel.Trace);
            var bcp = new Process();
            bcp.StartInfo.FileName = "bcp";
            bcp.StartInfo.Arguments = bcpArgs;
            bcp.StartInfo.UseShellExecute = false;
            bcp.StartInfo.RedirectStandardError = true;
            bcp.Start();
            bcp.WaitForExit();
            if (bcp.ExitCode != 0) {
                string err = bcp.StandardError.ReadToEnd();
                logger.Log(err, LogLevel.Critical);
                throw new Exception("BCP error: " + err);
            }

            string plinkArgs = string.Format(@"-ssh -v -l {0} -i {1} {2} {3} {4} {5}",
                                              nzUser,
                                              nzPrivateKeyPath,
                                              nzServer,
                                              Config.nzLoadScriptPath,
                                              destDB.ToLower(),
                                              destTableName);

            var plink = new Process();
            plink.StartInfo.FileName = Config.plinkPath;
            plink.StartInfo.Arguments = plinkArgs;
            plink.StartInfo.UseShellExecute = false;
            plink.StartInfo.RedirectStandardError = true;
            plink.StartInfo.RedirectStandardOutput = true;
            plink.Start();
            plink.WaitForExit();

            if (plink.ExitCode != 0) {
                string err = plink.StandardError.ReadToEnd();
                logger.Log(err, LogLevel.Critical);
                throw new Exception("plink error: " + err);
            }
            string output = plink.StandardOutput.ReadToEnd();
            if (Regex.IsMatch(output, "Cannot open input file .* No such file or directory")
                || !output.Contains("completed successfully")) {
                throw new Exception("Netezza load failed: " + output);
            }
            if (output.Contains("Disconnected: User aborted at host key verification")) {
                throw new Exception("Error connecting to Netezza server: Please verify host key");
            }
        }

        struct Col {
            public string name;
            public string typeName;
            public Microsoft.SqlServer.Management.Smo.DataType dataType;

            public Col(string name, string typeName, Microsoft.SqlServer.Management.Smo.DataType dataType) {
                this.name = name;
                this.typeName = typeName;
                this.dataType = dataType;
            }
            /// <summary>
            /// Return an expression for use in BCP command
            /// </summary>
            public string ColExpression() {
                var stringTypes = new HashSet<SqlDataType> {
                SqlDataType.Char,
                SqlDataType.NChar,
                SqlDataType.NVarChar,
                SqlDataType.NVarCharMax,
                SqlDataType.VarChar,
                SqlDataType.VarCharMax,
                SqlDataType.NText,
                SqlDataType.Text
                };

                if (stringTypes.Contains(dataType.SqlDataType)) {
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

        public void CopyTableDefinition(string sourceDB, string sourceTableName, string schema, string destDB, string destTableName, string originalTableName = null) {
            var cols = GetColumns(sourceDB, sourceTableName, schema, originalTableName ?? sourceTableName);
            string nzCreate = string.Format(
                @"CREATE TABLE {0}
                            (
                                {1} NOT NULL
                            ) DISTRIBUTE ON RANDOM;",
                destTableName,
                string.Join(",", cols));
            logger.Log(nzCreate, LogLevel.Trace);
            destDataUtils.DropTableIfExists(destDB, destTableName, schema);
            var cmd = new OleDbCommand(nzCreate);
            destDataUtils.SqlNonQuery(destDB, cmd);

        }

        private List<Col> GetColumns(string sourceDB, string sourceTableName, string schema, string originalTableName) {
            var table = sourceDataUtils.GetSmoTable(sourceDB, sourceTableName, schema);

            var shortenedTypes = new HashSet<SqlDataType> {
                SqlDataType.Binary,
                SqlDataType.VarBinary,
                SqlDataType.VarBinaryMax,
                SqlDataType.Char,
                SqlDataType.NChar,
                SqlDataType.NVarChar,
                SqlDataType.NVarCharMax,
                SqlDataType.VarChar,
                SqlDataType.VarCharMax,
            };
            var shortenedNumericTypes = new HashSet<SqlDataType>{
                SqlDataType.Decimal,
                SqlDataType.Numeric
            };
            var cols = new List<Col>();
            foreach (Column col in table.Columns) {
                string typeName = col.DataType.Name;

                string modDataType = DataType.MapDataType(SqlFlavor.MSSQL, SqlFlavor.Netezza, typeName);
                if (typeName != modDataType) {
                    cols.Add(new Col(col.Name, modDataType, col.DataType));
                    continue;
                }
                if (shortenedTypes.Contains(col.DataType.SqlDataType)) {
                    ColumnModifier mod = null;
                    //see if there are any column modifiers which override our length defaults
                    IEnumerable<TableConf> tables = Config.tables.Where(t => t.Name == originalTableName);
                    ColumnModifier[] modifiers = tables.FirstOrDefault().columnModifiers;
                    if (modifiers != null) {
                        IEnumerable<ColumnModifier> mods = modifiers.Where(c => ((c.columnName == col.Name) && (c.type == "ShortenField")));
                        mod = mods.FirstOrDefault();
                    }

                    if (mod != null) {
                        typeName += "(" + mod.length + ")";
                    } else if (Config.netezzaStringLength > 0) {
                        typeName += "(" + ((col.DataType.MaximumLength > Config.netezzaStringLength || col.DataType.MaximumLength < 1) ? Config.netezzaStringLength : col.DataType.MaximumLength) + ")";
                    } else {
                        typeName += "(" + (col.DataType.MaximumLength > 0 ? col.DataType.MaximumLength : 16000) + ")";
                    }
                } else if (shortenedNumericTypes.Contains(col.DataType.SqlDataType)) {
                    typeName += "(" + col.DataType.NumericPrecision + ")";
                }
                cols.Add(new Col(NetezzaDataUtils.MapReservedWord(col.Name), typeName, col.DataType));
            }
            return cols;
        }
    }
}
