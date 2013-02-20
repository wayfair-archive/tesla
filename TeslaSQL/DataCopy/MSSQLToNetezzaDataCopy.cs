using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using TeslaSQL.DataUtils;
using System.Data.SqlClient;
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

        private static void CreateDirectoryIfNotExists(string directory) {
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
            string directory = Config.BcpPath.TrimEnd('\\') + @"\" + sourceDB.ToLower();
            CreateDirectoryIfNotExists(directory);
            string password = new cTripleDes().Decrypt(Config.RelayPassword);
            var bcpArgs = string.Format(@"""{0}"" queryout {1}\{2}.txt -c -S{3} -U {4} -P {5} -t""|"" -r\n",
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
            logger.Log("BCP successful for " + sourceTableName, LogLevel.Trace);
            string plinkArgs = string.Format(@"-ssh -v -batch -l {0} -i {1} {2} {3} {4} {5}",
                                                nzUser,
                                                nzPrivateKeyPath,
                                                nzServer,
                                                Config.NzLoadScriptPath,
                                                destDB.ToLower(),
                                                destTableName);
            logger.Log("nzload command: " + Config.PlinkPath + " " + plinkArgs, LogLevel.Trace);
            var plink = new Process();
            outputBuilder.Clear();
            errorBuilder.Clear();
            plink.StartInfo.FileName = Config.PlinkPath;
            plink.StartInfo.Arguments = plinkArgs;
            plink.StartInfo.UseShellExecute = false;
            plink.StartInfo.RedirectStandardError = true;
            plink.StartInfo.RedirectStandardOutput = true;
            plink.OutputDataReceived += delegate(object sender, DataReceivedEventArgs e) {
                lock (outputBuilder) {
                    outputBuilder.AppendLine(e.Data);
                }
            };
            plink.ErrorDataReceived += delegate(object sender, DataReceivedEventArgs e) {
                lock (errorBuilder) {
                    errorBuilder.AppendLine(e.Data);
                }
            };
            plink.Start();
            plink.BeginOutputReadLine();
            plink.BeginErrorReadLine();
            status = plink.WaitForExit(Config.DataCopyTimeout * 1000);

            if (!status) {
                plink.Kill();
                throw new Exception("plink/nzload timed out for table " + sourceTableName);
            }

            //plink seems to make odd decisions about what to put in stdout vs. stderr, so we just lump them together
            string output = outputBuilder + "\r\n" + errorBuilder;
            if (plink.ExitCode != 0) {
                logger.Log(output, LogLevel.Critical);
                throw new Exception("plink error: " + output);
            }
            if (output.Contains("Disconnected: User aborted at host key verification")) {
                throw new Exception("Error connecting to Netezza server: Please verify host key as the user that runs Tesla");
            }
            if (Regex.IsMatch(output, "Cannot open input file .* No such file or directory")
                || !output.Contains("completed successfully")) {
                throw new Exception("Netezza load failed: " + output);
            }

            logger.Log("nzload successful for table " + destTableName, LogLevel.Trace);
        }

        struct Col {
            public string name;
            public string typeName;
            public DataType dataType;

            public Col(string name, string typeName, DataType dataType) {
                this.name = name;
                this.typeName = typeName;
                this.dataType = dataType;
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
            //get actual field list on the source table
            var includeColumns = new List<string>() { "SYS_CHANGE_VERSION", "SYS_CHANGE_OPERATION" };
            var columns = sourceDataUtils.GetFieldList(sourceDB, sourceTableName, schema, originalTableName, includeColumns);
            //get the table config object
            var table = Config.TableByName(originalTableName);

            var cols = new List<Col>();
            foreach (TColumn col in columns) {
                string typeName = col.dataType.BaseType;

                ColumnModifier mod = null;
                //see if there are any column modifiers which override our length defaults
                ColumnModifier[] modifiers = table.ColumnModifiers;
                if (modifiers != null) {
                    IEnumerable<ColumnModifier> mods = modifiers.Where(c => ((c.columnName == col.name) && (c.type == "ShortenField")));
                    mod = mods.FirstOrDefault();
                }

                string modDataType = DataType.MapDataType(SqlFlavor.MSSQL, SqlFlavor.Netezza, typeName);
                if (typeName != modDataType) {
                    if (mod != null && Regex.IsMatch(modDataType, @".*\(\d+\)$")) {
                        modDataType = Regex.Replace(modDataType, @"\d+", mod.length.ToString());
                    }
                    cols.Add(new Col(NetezzaDataUtils.MapReservedWord(col.name), modDataType, col.dataType));
                    continue;
                }

                if (col.dataType.UsesMaxLength()) {
                    if (mod != null) {
                        typeName += "(" + mod.length + ")";
                    } else if (Config.NetezzaStringLength > 0) {
                        typeName += "(" + ((col.dataType.CharacterMaximumLength > Config.NetezzaStringLength
                            || col.dataType.CharacterMaximumLength < 1) ? Config.NetezzaStringLength : col.dataType.CharacterMaximumLength) + ")";
                    } else {
                        typeName += "(" + (col.dataType.CharacterMaximumLength > 0 ? col.dataType.CharacterMaximumLength : 16000) + ")";
                    }
                } else if (col.dataType.UsesPrecisionScale()) {
                    typeName += "(" + col.dataType.NumericPrecision + "," + col.dataType.NumericScale + ")";
                }
                cols.Add(new Col(NetezzaDataUtils.MapReservedWord(col.name), typeName, col.dataType));
            }
            return cols;
        }
    }
}
