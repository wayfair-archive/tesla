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
using Microsoft.SqlServer.Management.Smo;
using Xunit;
#endregion

namespace TeslaSQL {

    public class Config {

        //static method to initialize configuration by deserializing config file into objects
        public static void Load(string configFile) {
            //try {
            ConfigLoader c = null;
            XmlSerializer serializer = new XmlSerializer(typeof(ConfigLoader));

            StreamReader reader = new StreamReader(configFile);
            c = (ConfigLoader)serializer.Deserialize(reader);
            reader.Close();

            //set global static configuration properties                  
            masterDB_m = ValidateNullableIdentifier(c.masterDB);
            masterCTDB_m = ValidateNullableIdentifier(c.masterCTDB);
            slaveCTDB_m = ValidateNullableIdentifier(c.slaveCTDB);
            slaveDB_m = ValidateNullableIdentifier(c.slaveDB);
            relayDB_m = ValidateNullableIdentifier(c.relayDB);
            errorLogDB_m = ValidateNullableIdentifier(c.errorLogDB);
            masterUser_m = ValidateNullableIdentifier(c.masterUser);
            masterPassword_m = c.masterPassword;
            slaveUser_m = ValidateNullableIdentifier(c.slaveUser);
            slavePassword_m = c.slavePassword;
            relayUser_m = ValidateNullableIdentifier(c.relayUser);
            relayPassword_m = c.relayPassword;
            changeRetentionHours_m = c.changeRetentionHours;
            maxBatchSize_m = c.maxBatchSize;
            batchConsolidationThreshold_m = c.batchConsolidationThreshold;
            statsdHost_m = c.statsdHost;
            statsdPort_m = c.statsdPort;
            relayServer_m = c.relayServer;
            slave_m = c.slave;
            master_m = c.master;

            if (c.thresholdIgnoreStartTime != null) {
                thresholdIgnoreStartTime_m = TimeSpan.Parse(c.thresholdIgnoreStartTime);
            }
            if (c.thresholdIgnoreEndTime != null) {
                thresholdIgnoreEndTime_m = TimeSpan.Parse(c.thresholdIgnoreEndTime);
            }

            if (!Enum.TryParse(c.agentType, out agentType_m)) {
                Logger.Log("Invalid agent type in configuration file!", LogLevel.Critical);
                Environment.Exit(1); //TODO throw instead
            }
            if (!String.IsNullOrEmpty(c.relayType)) {
                try {
                    relayType_m = (SqlFlavor)Enum.Parse(typeof(SqlFlavor), c.relayType);
                } catch {
                    throw new InvalidDataException("Invalid SQL type: " + c.relayType);
                }
            }
            if (!String.IsNullOrEmpty(c.masterType)) {
                try {
                    masterType_m = (SqlFlavor)Enum.Parse(typeof(SqlFlavor), c.masterType);
                } catch {
                    throw new InvalidDataException("Invalid SQL type: " + c.masterType);
                }
            }

            if (!String.IsNullOrEmpty(c.slaveType)) {
                try {
                    slaveType_m = (SqlFlavor)Enum.Parse(typeof(SqlFlavor), c.slaveType);
                } catch {
                    throw new InvalidDataException("Invalid SQL type: " + c.slaveType);
                }
            }

            if (logLevelOverride > 0) {
                logLevel_m = logLevelOverride;
            } else {
                try {
                    logLevel_m = (LogLevel)Enum.Parse(typeof(LogLevel), c.logLevel);
                } catch {
                    Logger.Log("Invalid log level in configuration file!", LogLevel.Critical);
                    Environment.Exit(1); //TODO throw instead
                }
            }

            tables_m = c.t;
            //TODO figure out appropriate exceptions to catch and what we want to throw
            //by testing various scenarios such as the file being invalid xml, etc.
            //catch (Exception e) {
            //throw e;
            //}

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
        private static string ValidateNullableIdentifier(string identifier) {
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

            //iterate through all properties of the static Config class and recursively print them
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
                } else
                    Console.Write("Property: ");

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
                    if (getMethod.ReturnType.IsArray) {
                        prefix = ((prefix != null) ? prefix : "") + "    ";
                        Array arrayObject = (Array)getMethod.Invoke(o, null);
                        if (arrayObject != null) {
                            foreach (object element in arrayObject) {
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
                } else
                    Console.WriteLine(message);
                return counter;
            } else {
                Console.WriteLine(message);
                return counter;
            }
        }


        #region properties
        //the main method can set this if it is passed in as a cli argument
        public static LogLevel logLevelOverride { get; set; }

        //log level from config file
        private static LogLevel logLevel_m;
        public static LogLevel logLevel { get { return logLevel_m; } }

        //array of table objects for global configuration
        private static TableConf[] tables_m;
        public static TableConf[] tables { get { return tables_m; } }

        //the agent type we should run (i.e. master, slave)
        private static AgentType agentType_m;
        public static AgentType agentType { get { return agentType_m; } }

        //hostname or IP of the master server
        private static string master_m;
        public static string master { get { return master_m; } }

        //hostname or IP of the slave server
        private static string slave_m;
        public static string slave { get { return slave_m; } }

        //hostname or IP of the relay server
        private static string relayServer_m;
        public static string relayServer { get { return relayServer_m; } }

        //type of relay server (i.e. MSSQL, MySQL, PostgreSQL)
        private static SqlFlavor? relayType_m;
        public static SqlFlavor? relayType { get { return relayType_m; } }

        //type of master server (i.e. MSSQL, MySQL, PostgreSQL)
        private static SqlFlavor? masterType_m;
        public static SqlFlavor? masterType { get { return masterType_m; } }

        //type of slave server (i.e. MSSQL, MySQL, PostgreSQL)
        private static SqlFlavor? slaveType_m;
        public static SqlFlavor? slaveType { get { return slaveType_m; } }

        //master database name 
        private static string masterDB_m;
        public static string masterDB { get { return masterDB_m; } }

        //master CT database name
        private static string masterCTDB_m;
        public static string masterCTDB { get { return masterCTDB_m; } }

        //slave database name 
        private static string slaveDB_m;
        public static string slaveDB { get { return slaveDB_m; } }

        //slave CT database name
        private static string slaveCTDB_m;
        public static string slaveCTDB { get { return slaveCTDB_m; } }

        //relay database name
        private static string relayDB_m;
        public static string relayDB { get { return relayDB_m; } }

        //database to log errors to
        private static string errorLogDB_m;
        public static string errorLogDB { get { return errorLogDB_m; } }

        //how many hours to retain changes for in the relay server
        private static int changeRetentionHours_m;
        public static int changeRetentionHours { get { return changeRetentionHours_m; } }

        //username to use when connecting to the master
        public static string masterUser_m;
        public static string masterUser { get { return masterUser_m; } }

        //password to use when connecting to the master
        public static string masterPassword_m;
        public static string masterPassword { get { return masterPassword_m; } }

        //username to use when connecting to the slave
        public static string slaveUser_m;
        public static string slaveUser { get { return slaveUser_m; } }

        //password to use when connecting to the slave
        public static string slavePassword_m;
        public static string slavePassword { get { return slavePassword_m; } }

        //username to use when connecting to the relay server
        public static string relayUser_m;
        public static string relayUser { get { return relayUser_m; } }

        //password to use when connecting to the relay server
        public static string relayPassword_m;
        public static string relayPassword { get { return relayPassword_m; } }

        //how many CT versions to include in a batch on the master
        private static int maxBatchSize_m;
        public static int maxBatchSize { get { return maxBatchSize_m; } }

        //start time for when we ignore the max batch size each day (to catch up if we are behind)
        private static TimeSpan? thresholdIgnoreStartTime_m;
        public static TimeSpan? thresholdIgnoreStartTime { get { return thresholdIgnoreStartTime_m; } }

        //end time for when we ignore the max batch size each day (to catch up if we are behind)
        private static TimeSpan? thresholdIgnoreEndTime_m;
        public static TimeSpan? thresholdIgnoreEndTime { get { return thresholdIgnoreEndTime_m; } }

        //once a slave gets this many batches behind it will start consolidating them into a bigger batch
        private static int batchConsolidationThreshold_m;
        public static int batchConsolidationThreshold { get { return batchConsolidationThreshold_m; } }

        //hostname or ip to write statsd calls to
        private static string statsdHost_m;
        public static string statsdHost { get { return statsdHost_m; } }

        //port to write statsd calls to
        private static string statsdPort_m;
        public static string statsdPort { get { return statsdPort_m; } }
        #endregion

        //This needs to be a class for the XmlRoot attribute to deserialize properly        
        [System.Xml.Serialization.XmlRoot("conf")]
        public class ConfigLoader {

            [XmlElement("logLevel")]
            public string logLevel { get; set; }

            [XmlElement("agentType")]
            public string agentType { get; set; }

            [XmlElement("master")]
            public string master { get; set; }

            [XmlElement("masterType")]
            public string masterType { get; set; }

            [XmlElement("slave")]
            public string slave { get; set; }

            [XmlElement("slaveType")]
            public string slaveType { get; set; }

            [XmlElement("relayServer")]
            public string relayServer { get; set; }

            [XmlElement("relayType")]
            public string relayType { get; set; }

            [XmlElement("masterDB")]
            public string masterDB { get; set; }

            [XmlElement("masterCTDB")]
            public string masterCTDB { get; set; }

            [XmlElement("slaveDB")]
            public string slaveDB { get; set; }

            [XmlElement("slaveCTDB")]
            public string slaveCTDB { get; set; }

            [XmlElement("relayDB")]
            public string relayDB { get; set; }

            [XmlElement("errorLogDB")]
            public string errorLogDB { get; set; }

            [XmlElement("masterUser")]
            public string masterUser { get; set; }

            [XmlElement("masterPassword")]
            public string masterPassword { get; set; }

            [XmlElement("slaveUser")]
            public string slaveUser { get; set; }

            [XmlElement("slavePassword")]
            public string slavePassword { get; set; }

            [XmlElement("relayUser")]
            public string relayUser { get; set; }

            [XmlElement("relayPassword")]
            public string relayPassword { get; set; }

            [XmlElement("changeRetentionHours")]
            public int changeRetentionHours { get; set; }

            [XmlElement("maxBatchSize")]
            public int maxBatchSize { get; set; }

            [XmlElement("thresholdIgnoreStartTime")]
            public string thresholdIgnoreStartTime { get; set; }

            [XmlElement("thresholdIgnoreEndTime")]
            public string thresholdIgnoreEndTime { get; set; }

            [XmlElement("batchConsolidationThreshold")]
            public int batchConsolidationThreshold { get; set; }

            [XmlElement("statsdHost")]
            public string statsdHost { get; set; }

            [XmlElement("statsdPort")]
            public string statsdPort { get; set; }

            [XmlArray("tables")]
            public TableConf[] t { get; set; }
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

                    dictionary.Add(cm.columnName, "LEFT(CAST(P." + cm.columnName + " AS NVARCHAR(MAX))," + Convert.ToString(cm.length) + ") as " + cm.columnName);
                } else {
                    //TODO do we want to throw this, or only if it's a stopOnError table, or not at all?
                    throw new NotSupportedException("Exception type " + cm.type + " not supported - exception for column " + cm.columnName + " violates this");
                }
            }

            return dictionary;
        }




        /// <summary>
        /// List of supported sql databases
        /// </summary>
        public enum SqlFlavor {
            MSSQL,
            Netezza
        }


        #region Unit Tests
        //unit tests for TestValidateNullableIdentifier method
        [Fact]
        public void TestValidateNullableIdentifier() {
            //valid database identifiers
            Assert.Equal("test", ValidateNullableIdentifier("test"));
            Assert.Equal("test_1", ValidateNullableIdentifier("test_1"));
            Assert.Equal("_test", ValidateNullableIdentifier("_test"));
            Assert.Equal("test1", ValidateNullableIdentifier("test1"));
            Assert.Equal("te$t moar", ValidateNullableIdentifier("te$t moar"));

            //null and empty are okay too
            Assert.Equal("", ValidateNullableIdentifier(""));
            Assert.Equal(null, ValidateNullableIdentifier(null));

            //invalid identifiers should all throw
            Assert.Throws<InvalidDataException>(delegate { ValidateNullableIdentifier("$test"); });
            Assert.Throws<InvalidDataException>(delegate { ValidateNullableIdentifier("1test"); });
            Assert.Throws<InvalidDataException>(delegate { ValidateNullableIdentifier("#test"); });
            Assert.Throws<InvalidDataException>(delegate { ValidateNullableIdentifier(" test"); });
            Assert.Throws<InvalidDataException>(delegate { ValidateNullableIdentifier("@test"); });
            Assert.Throws<InvalidDataException>(delegate { ValidateNullableIdentifier(" "); });
            Assert.Throws<InvalidDataException>(delegate { ValidateNullableIdentifier("\t"); });
            Assert.Throws<InvalidDataException>(delegate { ValidateNullableIdentifier("\r\n"); });
            Assert.Throws<InvalidDataException>(delegate { ValidateNullableIdentifier("\r\ntest"); });
        }

        //unit tests for ValidateRequiredHost
        [Fact]
        public void TestValidateRequiredHost() {
            //valid hostnames or ips
            Assert.Equal("testhost", ValidateRequiredHost("testhost"));
            Assert.Equal("192.168.1.1", ValidateRequiredHost("192.168.1.1"));
            Assert.Equal("10.25.30.40", ValidateRequiredHost("10.25.30.40"));
            Assert.Equal("testhost01", ValidateRequiredHost("testhost01"));
            Assert.Equal("test\\instance", ValidateRequiredHost("test\\instance"));

            //null and empty are not okay
            Assert.Throws<InvalidDataException>(delegate { ValidateRequiredHost(""); });
            Assert.Throws<InvalidDataException>(delegate { ValidateRequiredHost(null); });

            //invalid hostnames and ips
            //TODO decide whether we care that bogus ips like 256.0.0.0 will get through because they are valid hostnames?
            Assert.Throws<InvalidDataException>(delegate { ValidateRequiredHost(" startswithspace"); });
            Assert.Throws<InvalidDataException>(delegate { ValidateRequiredHost("has a space"); });
            Assert.Throws<InvalidDataException>(delegate { ValidateRequiredHost("has\twhitespace"); });
            Assert.Throws<InvalidDataException>(delegate { ValidateRequiredHost("has\r\nnewline"); });
            Assert.Throws<InvalidDataException>(delegate { ValidateRequiredHost(" "); });

        }

        //unit tests for ValidateSqlFlavor
        [Fact]
        public void TestValidateSqlFlavor() {
            Assert.Equal(SqlFlavor.MSSQL, ValidateSqlFlavor("MSSQL"));
            Assert.Equal(SqlFlavor.Netezza, ValidateSqlFlavor("Netezza"));

            Assert.Throws<InvalidDataException>(delegate { ValidateSqlFlavor(""); });
            Assert.Throws<InvalidDataException>(delegate { ValidateSqlFlavor(null); });
            Assert.Throws<InvalidDataException>(delegate { ValidateSqlFlavor("SomethingElseInvalid"); });
        }

        #endregion
    }

    //TODO figure out a way to do a similar public/private thing as above if possible?
    //currently even though Config.Tables is private, the elements of it are public
    [XmlType("table")]
    public class TableConf {
        [XmlElement("name")]
        public string Name { get; set; }

        [XmlElement("stopOnError")]
        public bool stopOnError { get; set; }

        //TODO decide whether this should be a string or an array of strings
        [XmlElement("columnList")]
        public ColumnList columnList { get; set; }

        [XmlElement("columnModifier")]
        public ColumnModifier[] columnModifiers { get; set; }

        //these properties get set later by the agents, not during deserialization
        public string masterColumnList { get; set; }

        //slave version removes the "CT." and "P." settings and doesn't care about length exceptions
        public string slaveColumnList { get; set; }

        public string mergeUpdateList { get; set; }

        public string pkList { get; set; }

        public string notNullPKList { get; set; }
    }

    public class ColumnList {
        [XmlElement("column")]
        public string[] columns { get; set; }

    }

    //TODO put this as a child or attribute of the <column> 
    public class ColumnModifier {
        [XmlAttribute("type")]
        public string type { get; set; }

        [XmlAttribute("length")]
        public int length { get; set; }

        [XmlAttribute("columnName")]
        public string columnName { get; set; }
    }
}
