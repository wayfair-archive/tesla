#region Using Statements
using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;
using System.IO;
using System.Data;
using NDesk.Options;
using Xunit;
using TeslaSQL.Agents;
using TeslaSQL.DataUtils;
#endregion

/*
 * TeslaSQL - the Tesla Replicator:
 *
 * This application is used to replicate data in batches. It can be run as several different agents (see Config.AgentType enum)
 * which do things like publish changes, subscribe to changes, or clean up old data.
 *
 * Tesla is designed to be as easy to troubleshoot as possible without opening up the code. As a result there are a nearly
 * absurd number of Trace and Debug logging statements which explain what the application is doing when the LogLevel is set low enough.
 * As a programmer reading the source code, you'll notice these also effectively act as replacements for comments that would have contained the same text.
 *
 * Testing changes to this program:
 *
 * Unit tests can be run using the xunit.net test runner (use the x86 .NET 4.0 version).
 * This will run all methods decorated with the [Fact] attribute (they must be public and return void, and have no arguments).
 * If any of the Assert statements in those methods fail, the tests are considered failed.
 * Note, xunit.net will only see [Fact] methods that are inside of a public class.
 *
 * Authors:
 * Scott Sandler - <ssandler@wayfair.com>
 * Alexander Corwin - <acorwin@wayfair.com>
 * Copyright (C) 2012 Wayfair, LLC (http://www.wayfair.com)
 */

namespace TeslaSQL {
    public class Program {
        static void Main(string[] args) {
            Params parameters = new Params();
            try {
                parameters = ParseArgs(args);
            } catch (Exception e) {
                Console.WriteLine("Try `TeslaSQL --help' for more information.");
                throw e;
            }

            if (parameters.showHelp) {
                ShowHelp(parameters.optionSet);
                return;
            }

            if (String.IsNullOrEmpty(parameters.configFile) || !ValidatePath(parameters.configFile)) {
                throw new Exception("Please specify a valid config file path!");
            }           

            Console.WriteLine("TeslaSQL -- loading configuration file");
            var config = Config.Load(parameters.configFile);
            Console.Title = config.agentType + " | TeslaSQL";
            var logger = new Logger(config.logLevel, config.statsdHost, config.statsdPort, config.errorLogDB, parameters.logFile);
            logger.Log("Configuration file successfully loaded", LogLevel.Debug);
            
            if (parameters.validate) {
                config.DumpConfig(parameters.more, config);
                return;
            }

            if (parameters.dataMappingFile != null) {
                DataType.LoadDataMappingsFromFile(parameters.dataMappingFile);
            }
            
            if (!String.IsNullOrEmpty(parameters.logLevelOverride)) {
                try {
                    config.logLevel = (LogLevel)Enum.Parse(typeof(LogLevel), parameters.logLevelOverride);
                } catch {
                    Console.WriteLine("Try `TeslaSQL --help' for more information.");
                    throw new Exception("Invalid log level!");
                }
            }

            //run appropriate agent type and exit with resulting exit code
            int responseCode = 0;
            try {
                Agent a = CreateAgent(config.agentType, config, logger);
                logger.Log("Running agent of type " + config.agentType, LogLevel.Info);
                a.Run();
                logger.Log("Agent completed successfully", LogLevel.Info);
            } catch (Exception e) {
                logger.Log("ERROR: " + e.Message + " - Stack Trace: " + e.StackTrace, LogLevel.Critical);
                responseCode = 1;
            }
            Environment.Exit(responseCode);
        }

        private static Agent CreateAgent(AgentType agentType, Config config, Logger logger) {
            IDataUtils sourceDataUtils;
            IDataUtils destDataUtils;
            switch (agentType) {
                case AgentType.Master:
                    sourceDataUtils = DataUtilsFactory.GetInstance(config, logger, TServer.MASTER, (SqlFlavor)config.masterType);
                    destDataUtils = DataUtilsFactory.GetInstance(config, logger, TServer.RELAY, (SqlFlavor)config.relayType);
                    logger.dataUtils = destDataUtils;
                    var master = new Master(config, sourceDataUtils, destDataUtils, logger);
                    return master;
                case AgentType.Slave:
                    sourceDataUtils = DataUtilsFactory.GetInstance(config, logger, TServer.RELAY, (SqlFlavor)config.relayType);
                    destDataUtils = DataUtilsFactory.GetInstance(config, logger, TServer.SLAVE, (SqlFlavor)config.slaveType);
                    logger.dataUtils = sourceDataUtils;
                    var slave = new Slave(config, sourceDataUtils, destDataUtils, logger);
                    return slave;
                case AgentType.ShardCoordinator:
                    sourceDataUtils = DataUtilsFactory.GetInstance(config, logger, TServer.RELAY, (SqlFlavor)config.relayType);
                    logger.dataUtils = sourceDataUtils;
                    var shardCoordinator = new ShardCoordinator(config, sourceDataUtils, logger);
                    return shardCoordinator;
                case AgentType.Notifier:
                    sourceDataUtils = DataUtilsFactory.GetInstance(config, logger, TServer.RELAY, (SqlFlavor)config.relayType);
                    var notifier = new Notifier(config, sourceDataUtils, new SimpleEmailClient(config.emailServerHost, config.emailFromAddress, config.emailServerPort), logger);
                    logger.dataUtils = sourceDataUtils;
                    return notifier;
                case AgentType.MasterMaintenance:
                    sourceDataUtils = DataUtilsFactory.GetInstance(config, logger, TServer.MASTER, (SqlFlavor)config.masterType);
                    var masterMaintenance = new MasterMaintenance(config, sourceDataUtils);
                    return masterMaintenance;
                case AgentType.RelayMaintenance:
                    sourceDataUtils = DataUtilsFactory.GetInstance(config, logger, TServer.RELAY, (SqlFlavor)config.relayType);
                    var relayMaintenance = new RelayMaintenance(config, sourceDataUtils);
                    return relayMaintenance;
                case AgentType.SlaveMaintenance:
                    sourceDataUtils = DataUtilsFactory.GetInstance(config, logger, TServer.SLAVE, (SqlFlavor)config.slaveType);
                    var slaveMaintenance = new SlaveMaintenance(config, sourceDataUtils);
                    return slaveMaintenance;
            }
            throw new Exception("Invalid agent type: " + agentType);
        }


        /// <summary>
        /// Struct holding data related to command line arguments
        /// </summary>
        protected struct Params {
            public string configFile { get; set; }
            public string logLevelOverride { get; set; }
            public bool validate { get; set; }
            public int more { get; set; }
            public bool showHelp { get; set; }
            public string logFile { get; set; }
            public string dataMappingFile { get; set; }
            public OptionSet optionSet { get; set; }
        }


        /// <summary>
        /// Test that a path is well formed and exists on the filesystem
        /// </summary>
        /// <param name="path">File path</param>
        /// <returns>Boolean, true meaning the path is valid and false being invalid.</returns>
        private static bool ValidatePath(string path) {
            try {
                FileInfo fi = new System.IO.FileInfo(path);
                return fi.Exists;
            } catch {
                return false;
            }
        }


        /// <summary>
        /// Parse cli args using the NDesk library
        /// </summary>
        /// <param name="args">Array of arguments</param>
        /// <returns>Params struct represnting the options</returns>
        protected static Params ParseArgs(string[] args) {
            Params parameters = new Params();
            //Create a list of possible values of LogLevels to include in the help info
            string logLevels = "";
            foreach (var value in Enum.GetValues(typeof(LogLevel))) {
                if (logLevels != "") {
                    logLevels += ", ";
                }
                logLevels += Convert.ToString((LogLevel)value);
            }

            /*
             * Initialize NDesk.OptionSet object.
             * For each block the first value represents the parameter name and whether it takes a value or is a switch.
             * Arguments that take values have an = sign at the end, the others are switches.
             * The second element is the help info to print.
             * The third is a lambra expression that NDesk executes if that argument is passed in.
             */
            var p = new OptionSet() {
            { "c|configfile=" , "The config file {PATH}." ,
              v => parameters.configFile = v },
            { "l|loglevel=" , "How much log information to print. " +
                "Valid values are " + logLevels + "." ,
              v => parameters.logLevelOverride = v },
            { "v|validate" , "Validate configuration file, print the parsed contents " +
                "to the console and exit." ,
              v => parameters.validate = true },
            { "m|more=" , "Used with --validate, emulates the unix more utility. Prints {NUM} lines" +
                " and then waits for input before continuing." ,
              ( int v) => parameters.more = v },
            { "h|help" ,  "show this message and exit" ,
              v => parameters.showHelp = v != null },
            {"f|logfile=", "The log file {PATH}.",
                v => parameters.logFile = v },
            { "p|datamappingfile=", "The data type mappings file {PATH} used by Slave agents.",
                v => parameters.dataMappingFile = v}
            };

            //Save the option set object to the params struct. This is required to run ShowHelp if --help is passed in.
            parameters.optionSet = p;

            //parse the arguments against the OptionSet
            //extra will hold a list of unprocessed/unsupported args, which we will ignore.
            List<string> extra = p.Parse(args);
            foreach (string s in extra) {
                //can't use Logger class because LogLevel wouldn't have been loaded into config yet.
                //just warn the user via cli about this
                Console.WriteLine("Warning: unprocessed CLI arg: " + s);
            }

            return parameters;
        }

        /// <summary>
        /// Prints usage/option info based on the passed in OptionSet
        /// </summary>
        /// <param name="p">NDesk option set</param>
        private static void ShowHelp(OptionSet p) {
            Console.WriteLine("Usage: TeslaSQL --configfile PATH [OPTIONS]");
            Console.WriteLine("Tesla Replicator.");
            Console.WriteLine();
            Console.WriteLine("Options:");
            p.WriteOptionDescriptions(Console.Out);
        }


    }


}
