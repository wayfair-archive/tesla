#region Using Statements
using System;
using System.Collections;
using System.Collections.Specialized;
using System.Collections.Generic;
using System.Text;
using System.Linq;
using System.Text.RegularExpressions;
using System.IO;
using System.Reflection;
using System.Xml;
using System.Xml.Serialization;
using Xunit;
using TeslaSQL.DataUtils;
#endregion

namespace TeslaSQL {

    public static class Config {
        //method to initialize configuration by deserializing config file into objects
        public static void Load(string configFile) {
            ConfigLoader c = null;
            XmlSerializer serializer = new XmlSerializer(typeof(ConfigLoader));

            StreamReader reader = new StreamReader(configFile);
            c = (ConfigLoader)serializer.Deserialize(reader);
            reader.Close();

            MasterDB = ValidateNullableIdentifier(c.masterDB);
            MasterCTDB = ValidateNullableIdentifier(c.masterCTDB);
            MasterEngine = ValidateNullableIdentifier(c.masterEngine);
            SlaveCTDB = ValidateNullableIdentifier(c.slaveCTDB);
            SlaveDB = ValidateNullableIdentifier(c.slaveDB);
            SlaveEngine = ValidateNullableIdentifier(c.slaveEngine);
            RelayDB = ValidateNullableIdentifier(c.relayDB);
            ErrorLogDB = ValidateNullableIdentifier(c.errorLogDB);
            MasterUser = ValidateNullableIdentifier(c.masterUser);
            MasterPassword = c.masterPassword;
            SlaveUser = ValidateNullableIdentifier(c.slaveUser);
            SlavePassword = c.slavePassword;
            RelayUser = ValidateNullableIdentifier(c.relayUser);
            RelayPassword = c.relayPassword;
            ChangeRetentionHours = c.changeRetentionHours;
            batchRecordRetentionDays = c.batchRecordRetentionDays;
            MaxBatchSize = c.maxBatchSize;
            BatchConsolidationThreshold = c.batchConsolidationThreshold;
            StatsdHost = c.statsdHost;
            StatsdPort = c.statsdPort;
            RelayServer = c.relayServer;
            Slave = c.slave;
            Master = c.master;
            EmailServerHost = c.emailServerHost;
            EmailServerPort = c.emailServerPort;
            EmailFromAddress = c.emailFromAddress;
            EmailErrorRecipient = c.emailErrorRecipient;
            Sharding = c.sharding;
            shardDatabases_local = c.shardDatabases;
            MasterShard = c.masterShard;
            DataCopyTimeout = c.dataCopyTimeout > 0 ? c.dataCopyTimeout : 36000;
            QueryTimeout = c.queryTimeout > 0 ? c.queryTimeout : 12000;
            NetezzaPrivateKeyPath = c.netezzaPrivateKeyPath;
            NetezzaUser = c.netezzaUser;
            RefreshViews = c.refreshViews;
            maxThreads_local = c.maxThreads;
            BcpPath = c.bcpPath;
            NzLoadScriptPath = c.nzLoadScriptPath;
            NetezzaStringLength = c.netezzaStringLength;
            PlinkPath = c.plinkPath;
            IgnoreCase = c.ignoreCase;
            EmailMessage = c.emailMessage;

            if (c.magicHours != null) {
                MagicHours = c.magicHours.Select(fmt => DateTime.Parse(fmt).TimeOfDay).ToArray();
            }

            if (c.thresholdIgnoreStartTime != null) {
                ThresholdIgnoreStartTime = TimeSpan.Parse(c.thresholdIgnoreStartTime);
            }
            if (c.thresholdIgnoreEndTime != null) {
                ThresholdIgnoreEndTime = TimeSpan.Parse(c.thresholdIgnoreEndTime);
            }

            RelayType = c.relayType;
            MasterType = c.masterType;
            SlaveType = c.slaveType;

            Tables = c.tables;
            //this is the simplest way to simulate a "default value" when doing this deserialization
            foreach (TableConf t in Tables) {
                if (t.SchemaName == null) {
                    t.SchemaName = "dbo";
                }
            }
        }


        /// <summary>
        /// Validates a hostname or ip identifier
        /// </summary>
        /// <param name="host">Hostname or IP</param>
        /// <returns>The host string if it is valid. Throws an exception otherwise.</returns>
        public static string ValidateRequiredHost(string host) {
            /*
             * We can't use real regexes for hostname or IP here because a SQL server identifier can also contain instance name and/or port in a few
             * formats, so validation is more basic.
             */
            string ValidRegex = "^(\\w|\\d)[\\w\\d,\\\\$._]+$";

            //this variable has to be a valid host (we don't allow null or empty) so we throw
            if (String.IsNullOrEmpty(host) || !Regex.IsMatch(host, ValidRegex)) {
                throw new InvalidDataException("Invalid server " + host + "!");
            }
            return host;
        }


        /// <summary>
        /// Validates a sql flavor (MSSQL, Netezza, etc.) and returns a SqlFlavor enum
        /// </summary>
        /// <param name="sqltype">String representing the sql flavor</param>
        /// <returns>SqlFlavor enum</returns>
        public static SqlFlavor ValidateSqlFlavor(string sqltype) {
            SqlFlavor flavor = new SqlFlavor();

            if (String.IsNullOrEmpty(sqltype)) {
                throw new InvalidDataException("Sql type must be specified!");
            } else {
                try {
                    flavor = (SqlFlavor)Enum.Parse(typeof(SqlFlavor), sqltype);
                } catch {
                    throw new InvalidDataException("Invalid SQL type: " + sqltype);
                }
            }
            return flavor;
        }


        /// <summary>
        /// Validates a SQL identifier (database name, table name, login name, etc.)
        /// </summary>
        /// <param name="identifier">Identifier string, which can also be null or empty</param>
        /// <returns>The identifier if it is valid. Throws an exception otherwise. </returns>
        public static string ValidateNullableIdentifier(string identifier) {
            //the following regex represents a valid SQL identifier
            //it must start with a letter or underscore, followed by any combination
            //of word characters (letters, digits, underscores), the dollar sign, or spaces
            string pattern = @"^([a-zA-Z_])[\w$\s]+$";

            //the identifier must either be null or empty, or match the regex
            if (String.IsNullOrEmpty(identifier) || Regex.IsMatch(identifier, pattern)) {
                return identifier;
            } else {
                throw new InvalidDataException("Invalid configuration parameter: " + identifier);
            }
        }


        /// <summary>
        /// Writes all string properties to the console for debugging purposes.
        /// </summary>
        /// <param name="more">How many lines to print before stopping and writing "...more"</param>
        public static void DumpConfig(int more) {
            int linecount = 0;
            //keep track of the existing console color so we can reset it at the end
            ConsoleColor prevColor = Console.ForegroundColor;

            //iterate through all properties of the Config class and recursively print them
            foreach (var prop in typeof(Config).GetProperties()) {
                linecount = DumpConfigObject(prop, typeof(Config), linecount, more, null);
            }
            //reset console color to whatever it was before this
            Console.ForegroundColor = prevColor;
        }


        /// <summary>
        /// Recursively prints objects property names and values, as well as child objects
        /// </summary>
        /// <param name="prop">PropertyInfo object</param>
        /// <param name="o">The object it belongs to</param>
        /// <param name="linecount">How many lines we've printed since the last "...more"</param>
        /// <param name="more">How many lines to print before stopping and writing "...more"</param>
        /// <param name="prefix">Prefix for indentation of nested properties</param>
        /// <returns>Int - the current line counter</returns>
        public static int DumpConfigObject(System.Reflection.PropertyInfo prop, Object o, int linecount, int more, string prefix = null) {
            //don't print empty/null properties
            if (!String.IsNullOrEmpty(Convert.ToString(prop.GetValue(o, null)))) {
                //some nice color highlighting of the names/values just to make the output more readable
                ConsoleColor propColor = ConsoleColor.Gray;
                ConsoleColor nameColor = ConsoleColor.Green;
                ConsoleColor valueColor = ConsoleColor.Yellow;
                Console.ForegroundColor = propColor;

                //prefix for indenting nested properties
                if (prefix != null) {
                    //use Cyan for nested properties names to break up the monotony
                    nameColor = ConsoleColor.Cyan;
                    Console.Write(prefix + "Property: ");
                } else {
                    Console.Write("Property: ");
                }

                Console.ForegroundColor = nameColor;
                Console.Write(prop.Name);
                Console.ForegroundColor = propColor;
                Console.Write(" = ");
                Console.ForegroundColor = valueColor;
                //write the property's value and retrieve current line count
                linecount = WriteLine(Convert.ToString(prop.GetValue(o, null)), linecount, more);

                //get the type of the property's value
                Type type = prop.GetValue(o, null).GetType();

                //for "primitive" types (or enums, strings) we are done. for anything else they can have nested objects
                //so we go through those and recurseively print them. arrays need to be handled somewhat specially because
                //they can
                if (!(type.IsPrimitive || type.Equals(typeof(string)) || type.BaseType.Equals(typeof(Enum)))) {
                    var getMethod = prop.GetGetMethod();
                    if (prop.GetValue(o, null) is IEnumerable) {
                        prefix = ((prefix != null) ? prefix : "") + "    ";
                        IEnumerable enumerableObject = (IEnumerable)getMethod.Invoke(o, null);
                        if (enumerableObject != null) {
                            foreach (object element in enumerableObject) {
                                Type elemType = element.GetType();
                                //if it's an array of primitives, just print each element. otherwise we need the recursive call
                                if (elemType.IsPrimitive || elemType.Equals(typeof(string)) || elemType.BaseType.Equals(typeof(Enum))) {
                                    Console.ForegroundColor = propColor;
                                    Console.Write(prefix + "Element = ");
                                    Console.ForegroundColor = valueColor;
                                    linecount = WriteLine(Convert.ToString(element), linecount, more);
                                } else {
                                    //recursive call for arrays of objects
                                    foreach (PropertyInfo p in element.GetType().GetProperties()) {
                                        linecount = DumpConfigObject(p, element, linecount, more, prefix);
                                    }
                                }
                            }
                        }
                    } else {
                        //this handles other types of objects that aren't arrays such as tables.table[x].columnList
                        prefix = ((prefix != null) ? prefix : "") + "    ";
                        foreach (PropertyInfo p in prop.GetValue(o, null).GetType().GetProperties()) {
                            linecount = DumpConfigObject(p, prop.GetValue(o, null), linecount, more, prefix);
                        }
                    }
                }
            }
            return linecount;
        }


        /// <summary>
        /// Writes a line to the console, but if the counter is > more it prints "...more" afterwards and waits for input.
        /// </summary>
        /// <param name="message">The message to write</param>
        /// <param name="counter">Current line counter</param>
        /// <param name="more">How many lines to print before stopping and writing "...more"</param>
        /// <returns>The current line counter</returns>
        public static int WriteLine(string message, int counter, int more) {
            if (more > 0) {
                counter++;
                if (counter >= more) {
                    Console.Write(message);
                    counter = 0;
                    Console.CursorLeft = Console.BufferWidth - 7;
                    Console.Write("...more");
                    ConsoleKeyInfo cki = Console.ReadKey(true);
                } else {
                    Console.WriteLine(message);
                }
                return counter;
            } else {
                Console.WriteLine(message);
                return counter;
            }
        }


        #region properties
        //a configurable message when e-mailing errors
        public static string EmailMessage { get; set; }

        //array of table objects for global configuration        
        public static List<TableConf> Tables { get; set; }

        //the agent type we should run (i.e. master, slave)
        public static AgentType AgentType { get; set; }

        //hostname or IP of the master server
        public static string Master { get; set; }

        //hostname or IP of the slave server
        public static string Slave { get; set; }

        //hostname or IP of the relay server
        public static string RelayServer { get; set; }

        //type of relay server (i.e. MSSQL, MySQL, PostgreSQL)
        public static SqlFlavor RelayType { get; set; }

        //type of master server (i.e. MSSQL, MySQL, PostgreSQL)
        public static SqlFlavor MasterType { get; set; }

        //type of slave server (i.e. MSSQL, MySQL, PostgreSQL)
        public static SqlFlavor SlaveType { get; set; }

        //engine of master server. gets ignored except for mySQL right now
        public static string MasterEngine { get; set; }

        //engine of slave server. gets ignored except for mySQL right now
        public static string SlaveEngine { get; set; }

        //master database name
        public static string MasterDB { get; set; }

        //master CT database name
        public static string MasterCTDB { get; set; }

        //slave database name
        public static string SlaveDB { get; set; }

        //slave CT database name
        public static string SlaveCTDB { get; set; }

        //relay database name        
        public static string RelayDB { get; set; }

        //database to log errors to
        public static string ErrorLogDB { get; set; }

        /// <summary>
        /// how many hours to retain changes for in the relay server
        /// </summary>
        public static int ChangeRetentionHours { get; set; }

        /// <summary>
        /// how many days to retain records in CTVersion tables
        /// </summary>
        public static int batchRecordRetentionDays { get; set; }

        //username to use when connecting to the master
        public static string MasterUser { get; set; }

        //password to use when connecting to the master
        public static string MasterPassword { get; set; }

        //username to use when connecting to the slave
        public static string SlaveUser { get; set; }

        //password to use when connecting to the slave
        public static string SlavePassword { get; set; }

        //username to use when connecting to the relay server
        public static string RelayUser { get; set; }

        //password to use when connecting to the relay server
        public static string RelayPassword { get; set; }

        //how many CT versions to include in a batch on the master
        public static int MaxBatchSize { get; set; }

        //start time for when we ignore the max batch size each day (to catch up if we are behind)
        public static TimeSpan? ThresholdIgnoreStartTime { get; set; }

        //end time for when we ignore the max batch size each day (to catch up if we are behind)
        public static TimeSpan? ThresholdIgnoreEndTime { get; set; }

        //once a slave gets this many batches behind it will start consolidating them into a bigger batch
        public static int BatchConsolidationThreshold { get; set; }

        //hostname or ip to write statsd calls to
        public static string StatsdHost { get; set; }

        //port to write statsd calls to
        public static string StatsdPort { get; set; }

        //smtp server hostname or ip
        public static string EmailServerHost { get; set; }

        //smtp port
        public static int EmailServerPort { get; set; }

        //address to send notifications from in Notifier agent
        public static string EmailFromAddress { get; set; }

        //address or list of addresses to send notifications to in Notifier agent
        public static string EmailErrorRecipient { get; set; }

        //is this a master agent that takes part in sharding?
        public static bool Sharding { get; set; }

        //for shardcoordinator, list of shard databases
        private static string[] shardDatabases_local { get; set; }
        public static IEnumerable<string> ShardDatabases { get { return shardDatabases_local != null ? shardDatabases_local.ToList() : new List<string>(); } }

        //one shard to rule them all (which one we pull schema changes from). in a standard sharded setup this can be arbitrary.
        public static string MasterShard { get; set; }

        //ssh user for sshing to a netezza slave
        public static string NetezzaUser { get; set; }

        //private key for sshing to a netezza slave
        public static string NetezzaPrivateKeyPath { get; set; }

        //array of times after which a slave will sync changes
        public static TimeSpan[] MagicHours { get; set; }

        //timeout for copying data from one server to another
        public static int DataCopyTimeout { get; set; }

        //timeout for various queries that run as part of tesla
        public static int QueryTimeout { get; set; }

        //views to be refreshed when a table is altered on a slave
        public static List<RefreshView> RefreshViews { get; set; }

        //default maximum for string columns in netezza slaves. can be overriden using 
        //column modifiers. Since Netezza has pretty restrictive row size limits and you generally
        //don't use big strings in data warehouses, this helps avoid those limits.
        public static int NetezzaStringLength { get; set; }

        //path to BCP data out to for copying to netezza (should be an NFS share that netezza can mount)
        public static string BcpPath { get; set; }

        //path to a shell script on the Netezza server that wraps an nzload command for loading data
        //must be executable by the nzuser that we ssh as
        public static string NzLoadScriptPath { get; set; }

        //path to plink executable for sshing into netezza
        public static string PlinkPath { get; set; }

        //maximum number of threads to use in multithreaded portions of tesla
        private static int maxThreads_local;
        public static int MaxThreads {
            get { return maxThreads_local > 0 ? maxThreads_local : -1; }
            set { maxThreads_local = value; }
        }

        //when replicating data from a case insensitive technology to a case sensitive
        //technology, specify this as true to wrap all comparisons of primary keys
        //when applying changes on the slave side in UPPER() functions to effectively ignore case.
        public static bool IgnoreCase { get; set; }

        #endregion

        /// <summary>
        /// string indexer for Tables
        /// </summary>
        public static TableConf TableByName(string tablename) {
            //get the table config object
            IEnumerable<TableConf> tables = Tables.Where(t => string.Compare(t.Name, tablename, StringComparison.OrdinalIgnoreCase) == 0);
            return tables.FirstOrDefault();
        }

        /// <summary>
        /// Parses the column modifiers into a list object
        /// </summary>
        /// <param name="columnModifiers">Array of column modifier objects</param>
        /// <returns>List with column name as a key and a modifier string as a value</returns>
        public static Dictionary<string, string> ParseColumnModifiers(ColumnModifier[] columnModifiers) {
            Dictionary<string, string> dictionary = new Dictionary<string, string>();
            if (columnModifiers == null || columnModifiers.Length == 0)
                return dictionary;

            foreach (ColumnModifier cm in columnModifiers) {
                if (cm.type == "ShortenField") {
                    if (dictionary.ContainsKey(cm.columnName))
                        throw new NotSupportedException(cm.columnName + " has multiple modifiers, which is not supported");

                    dictionary.Add(cm.columnName, "LEFT(CAST(P.[" + cm.columnName + "] AS NVARCHAR(MAX))," + Convert.ToString(cm.length) + ") as '" + cm.columnName + "'");
                } else {
                    throw new NotSupportedException("Modifier type " + cm.type + " not supported - modifier for column " + cm.columnName + " violates this");
                }
            }
            return dictionary;
        }


    }
    //This needs to be a class for the XmlRoot attribute to deserialize properly
    [XmlRoot("conf")]
    public class ConfigLoader {
        public string master { get; set; }
        public SqlFlavor masterType { get; set; }
        public string slave { get; set; }
        public SqlFlavor slaveType { get; set; }
        public string relayServer { get; set; }
        public SqlFlavor relayType { get; set; }
        public string masterDB { get; set; }
        public string masterCTDB { get; set; }
        public string masterEngine { get; set; }
        public string slaveDB { get; set; }
        public string slaveCTDB { get; set; }
        public string slaveEngine { get; set; }
        public string relayDB { get; set; }
        public string errorLogDB { get; set; }
        public string masterUser { get; set; }
        public string masterPassword { get; set; }
        public string slaveUser { get; set; }
        public string slavePassword { get; set; }
        public string relayUser { get; set; }
        public string relayPassword { get; set; }
        public int changeRetentionHours { get; set; }
        public int batchRecordRetentionDays { get; set; }
        public int maxBatchSize { get; set; }
        public string thresholdIgnoreStartTime { get; set; }
        public string thresholdIgnoreEndTime { get; set; }
        public int batchConsolidationThreshold { get; set; }
        public string statsdHost { get; set; }
        public string statsdPort { get; set; }
        public string masterShard { get; set; }
        public bool sharding { get; set; }
        public string emailServerHost { get; set; }
        public int emailServerPort { get; set; }
        public string emailFromAddress { get; set; }
        public string emailErrorRecipient { get; set; }
        public int dataCopyTimeout { get; set; }
        public int queryTimeout { get; set; }
        public string netezzaUser { get; set; }
        public string netezzaPrivateKeyPath { get; set; }
        public int netezzaStringLength { get; set; }
        public int maxThreads { get; set; }
        public List<RefreshView> refreshViews { get; set; }
        public string bcpPath { get; set; }
        public string nzLoadScriptPath { get; set; }
        public string plinkPath { get; set; }
        public bool ignoreCase { get; set; }
        public string emailMessage { get; set; }

        [XmlArrayItem("magicHour")]
        public string[] magicHours { get; set; }

        [XmlArray("tables")]
        public List<TableConf> tables { get; set; }


        [XmlArrayItem("shardDatabase")]
        public string[] shardDatabases { get; set; }
    }

    [XmlType("refreshView")]
    public class RefreshView {
        public string ViewName { get; set; }
        public string Db { get; set; }
        public string Command { get; set; }
        [XmlIgnore]
        public string TableName { get { return Regex.Replace(ViewName, "vw", "TBL", RegexOptions.IgnoreCase); } }

        public override string ToString() {
            return string.Format("{0}..{1}: command = {2}", Db, ViewName, Command);
        }
    }


    public class TColumn : IEquatable<TColumn> {
        public readonly string name;
        public bool isPk;
        public DataType dataType {get;set;}
        public readonly bool isNullable;

        public TColumn(string name, bool isPk, DataType dataType, bool isNullable) {
            this.name = name;
            this.isPk = isPk;
            this.dataType = dataType;
            this.isNullable = isNullable;
        }
        public override string ToString() {
            return name;
        }

        /// <summary>
        /// Returns a string representation of the column for use in CREATE TABLE statements
        /// </summary>
        public string ToExpression(SqlFlavor flavor = SqlFlavor.MSSQL) {
            switch (flavor)
            {
                case SqlFlavor.MSSQL:
                    return string.Format("[{0}] {1} {2}", name, dataType.ToString(), isNullable ? "NULL" : "NOT NULL");
                case SqlFlavor.MySQL:
                    return string.Format("{0} {1} {2}", name, dataType.ToString(), isNullable ? "NULL" : "NOT NULL");
                default:
                    throw new NotImplementedException("No defined ToExpression for sql flavor: " + flavor.ToString());
            }
        }

        public bool Equals(TColumn other) {
            return name == other.name && isPk == other.isPk && DataType.Equals(dataType, other.dataType);
        }
        public override int GetHashCode() {
            return name.GetHashCode() ^ isPk.GetHashCode();
        }
    }

    public class ColumnModifier {
        [XmlAttribute("type")]
        public string type { get; set; }

        [XmlAttribute("length")]
        public int length { get; set; }

        [XmlAttribute("columnName")]
        public string columnName { get; set; }
    }
}
