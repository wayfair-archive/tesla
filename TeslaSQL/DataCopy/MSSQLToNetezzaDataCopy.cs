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

        public void CopyTable(string sourceDB, string sourceTableName, string schema, string destDB, int timeout, string destTableName = null) {
            //by default the dest table will have the same name as the source table
            destTableName = (destTableName == null) ? sourceTableName : destTableName;

            //drop table at destination and create from source schema
            CopyTableDefinition(sourceDB, sourceTableName, schema, destDB, destTableName);

            var cols = GetColumns(sourceDB, sourceTableName, schema);
            var bcpSelect = string.Format("SELECT {0} FROM {1}..{2};",
                                          string.Join(",", cols.Select(col => col.name)),
                                          sourceDB, sourceTableName);
            if (bcpSelect.Length > 3800) {
                //BCP commands fail if their text length is over 4000 characters, and we need some padding
                //drop view CTVWtablename if exists
                //create view CTVWtablename AS $bcpSelect
                string viewName = "CTVW" + sourceTableName;
                sourceDataUtils.RecreateView(sourceDB, viewName, bcpSelect);
                bcpSelect = string.Format("SELECT * FROM {0}..{1}", sourceDB, viewName);
            }
            var bcpArgs = string.Format(@"""{0}"" queryout \\bonas1a\sql_temp\{1}\{2}.txt -T -c -S{3} -t""|"" -r\n",
                                            bcpSelect,
                                            sourceDB.ToLower(),
                                            destTableName,
                                            "owl\\feeds"
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
                string err =bcp.StandardError.ReadToEnd();
                logger.Log(err, LogLevel.Critical);
                throw new Exception("BCP error: " + err);
            }

            string plinkArgs = string.Format(@"-ssh -v -l {0} -i {1} {2} /export/home/nz/management_scripts/load_data_tesla.sh {3} {4}",
                                              nzUser,
                                              nzPrivateKeyPath,
                                              nzServer,
                                              destDB.ToLower(),
                                              sourceTableName);

            var plink = new Process();
            plink.StartInfo.FileName = "plink.exe";
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
            public string datatype;
            public Col(string name, string datatype) {
                this.name = name;
                this.datatype = datatype;
            }
            public override string ToString() {
                return name + " " + datatype;
            }
        }

        public void CopyTableDefinition(string sourceDB, string sourceTableName, string schema, string destDB, string destTableName) {
            var cols = GetColumns(sourceDB, sourceTableName, schema);
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

        private List<Col> GetColumns(string sourceDB, string sourceTableName, string schema) {
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
                string dataType = col.DataType.Name;

                string modDataType = DataType.MapDataType(SqlFlavor.MSSQL, SqlFlavor.Netezza, dataType);
                if (dataType != modDataType) {
                    cols.Add(new Col(col.Name, modDataType));
                    continue;
                }
                if (shortenedTypes.Contains(col.DataType.SqlDataType)) {
                    //see if there are any column modifiers which override our length defaults
                    ColumnModifier[] modifiers = Config.tables.Where(t => t.Name == table.Name).FirstOrDefault().columnModifiers;
                    ColumnModifier mod = modifiers.Where(c => ((c.columnName == col.Name) && (c.type == "ShortenField"))).FirstOrDefault();

                    if (mod != null) {
                        dataType += "(" + mod.length + ")";
                    } else {
                        dataType += "(" + ((col.DataType.MaximumLength > Config.netezzaStringLength || col.DataType.MaximumLength < 1) ? Config.netezzaStringLength : col.DataType.MaximumLength) + ")";
                    }
                } else if (shortenedNumericTypes.Contains(col.DataType.SqlDataType)) {
                    dataType += "(" + col.DataType.NumericPrecision + ")";
                }
                cols.Add(new Col(NetezzaDataUtils.MapReservedWord(col.Name), dataType));
            }
            return cols;
        }
    }
}
