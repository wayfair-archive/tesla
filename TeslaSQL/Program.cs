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
using log4net.Config;
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
                Console.WriteLine("Error parsing arguments: " + e.Message);
                Console.WriteLine("Try `TeslaSQL --help' for more information.");
                Environment.Exit(1);
            }
            if (parameters.showHelp) {
                ShowHelp(parameters.optionSet);
                return;
            }
            if (parameters.agentType == AgentType.None) {
                Console.WriteLine("Please specify a valid agent type. use --help for more information.");
                Environment.Exit(1);
            }

            if (String.IsNullOrEmpty(parameters.configFile) || !ValidatePath(parameters.configFile)) {
                Console.WriteLine("Please specify a valid config file path!");
                Environment.Exit(1);
            }
            Config.agentType = parameters.agentType;


            Console.WriteLine("TeslaSQL -- loading configuration file");
            try {
                Config.Load(parameters.configFile);
            } catch (Exception e) {
                Console.WriteLine("Unable to load configuration file due to error: " + e.Message + ". Inner exception: " + e.InnerException.Message
                    + " Stack Trace: " + e.StackTrace);
                Environment.Exit(1);
            }
            Console.Title = Config.agentType + " | TeslaSQL";

            var logger = new Logger(Config.statsdHost, Config.statsdPort, Config.errorLogDB, parameters.logFile);

            try {
                XmlConfigurator.Configure(new System.IO.FileInfo(parameters.log4NetConfigPath));
            } catch (Exception e) {
                Console.WriteLine("Unable to initialize logging facility due to error: " + e.Message + ". Stack Trace: " + e.StackTrace);
                Environment.Exit(1);
            }
            if (parameters.logFile != null) {
                Logger.SetLogFilePath(parameters.logFile);
            }
            logger.Log("Configuration file successfully loaded", LogLevel.Debug);

            if (parameters.validate) {
                Config.DumpConfig(parameters.more);
                return;
            }

            if (parameters.dataMappingFile != null) {
                try {
                    DataType.LoadDataMappingsFromFile(parameters.dataMappingFile);
                } catch (Exception e) {
                    Console.WriteLine("UNable to initialize logging facility due to error: " + e.Message + ". Stack Trace: " + e.StackTrace);
                    Environment.Exit(1);
                }
            }

            //run appropriate agent type and exit with resulting exit code
            int responseCode = 0;

            try {
                Agent a = CreateAgent(Config.agentType, logger);
                logger.Log("Running agent of type " + Config.agentType, LogLevel.Info);
                a.Run();
                logger.Log("Agent completed successfully", LogLevel.Info);
            } catch (AggregateException ae) {
                logger.Log("Parallelization error", LogLevel.Critical);
                foreach (var e in ae.InnerExceptions) {
                    logger.Log(e.Message + '\n' + e.StackTrace, LogLevel.Critical);
                }
                responseCode = 1;
            } catch (Exception e) {
                logger.Log("ERROR: " + e.Message + " - Stack Trace: " + e.StackTrace, LogLevel.Critical);
                responseCode = 1;
            }
            Environment.Exit(responseCode);
        }



        private static Agent CreateAgent(AgentType agentType, Logger logger) {
            IDataUtils sourceDataUtils;
            IDataUtils destDataUtils;
            switch (agentType) {
                case AgentType.Master:
                    sourceDataUtils = DataUtilsFactory.GetInstance(logger, TServer.MASTER, Config.masterType);
                    destDataUtils = DataUtilsFactory.GetInstance(logger, TServer.RELAY, Config.relayType);
                    logger.dataUtils = destDataUtils;
                    var master = new Master(sourceDataUtils, destDataUtils, logger);
                    return master;
                case AgentType.Slave:
                    sourceDataUtils = DataUtilsFactory.GetInstance(logger, TServer.RELAY, Config.relayType);
                    destDataUtils = DataUtilsFactory.GetInstance(logger, TServer.SLAVE, Config.slaveType);
                    logger.dataUtils = sourceDataUtils;
                    var slave = new Slave(sourceDataUtils, destDataUtils, logger);
                    return slave;
                case AgentType.ShardCoordinator:
                    sourceDataUtils = DataUtilsFactory.GetInstance(logger, TServer.RELAY, Config.relayType);
                    logger.dataUtils = sourceDataUtils;
                    var shardCoordinator = new ShardCoordinator(sourceDataUtils, logger);
                    return shardCoordinator;
                case AgentType.Notifier:
                    sourceDataUtils = DataUtilsFactory.GetInstance(logger, TServer.RELAY, Config.relayType);
                    var notifier = new Notifier(sourceDataUtils, new SimpleEmailClient(Config.emailServerHost, Config.emailFromAddress, Config.emailServerPort), logger);
                    logger.dataUtils = sourceDataUtils;
                    return notifier;
                case AgentType.MasterMaintenance:
                    sourceDataUtils = DataUtilsFactory.GetInstance(logger, TServer.MASTER, Config.masterType);
                    destDataUtils = DataUtilsFactory.GetInstance(logger, TServer.RELAY, Config.relayType);
                    var masterMaintenance = new MasterMaintenance(sourceDataUtils, destDataUtils, logger);
                    return masterMaintenance;
                case AgentType.RelayMaintenance:
                    sourceDataUtils = DataUtilsFactory.GetInstance(logger, TServer.RELAY, Config.relayType);
                    var relayMaintenance = new RelayMaintenance(sourceDataUtils, logger);
                    return relayMaintenance;
                case AgentType.SlaveMaintenance:
                    sourceDataUtils = DataUtilsFactory.GetInstance(logger, TServer.RELAY, Config.relayType);
                    destDataUtils = DataUtilsFactory.GetInstance(logger, TServer.SLAVE, Config.slaveType);
                    var slaveMaintenance = new SlaveMaintenance(sourceDataUtils, destDataUtils, logger);
                    return slaveMaintenance;
            }
            throw new Exception("Invalid agent type: " + agentType);
        }


        /// <summary>
        /// Struct holding data related to command line arguments
        /// </summary>
        protected struct Params {
            public string configFile { get; set; }
            public bool validate { get; set; }
            public int more { get; set; }
            public bool showHelp { get; set; }
            public string logFile { get; set; }
            public string dataMappingFile { get; set; }
            public string log4NetConfigPath { get; set; }
            public AgentType agentType { get; set; }
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
            { "v|validate" , "Validate configuration file, print the parsed contents " +
                "to the console and exit." ,
              v => parameters.validate = true },
            { "m|more=" , "Used with --validate, emulates the unix more utility. Prints {NUM} lines" +
                " and then waits for input before continuing." ,
              ( int v) => parameters.more = v },
            { "h|help" ,  "show this message and exit" ,
              v => parameters.showHelp = v != null },
            { "f|logfile=", "The log file {PATH}.",
                v => parameters.logFile = v },
            { "p|datamappingfile=", "The data type mappings file {PATH} used by Slave agents.",
                v => parameters.dataMappingFile = v},
            { "n|log4netfile=", "The log4net configuration file {PATH}.",
                v => parameters.log4NetConfigPath = v},
            { "a|agent=", "The agent type that you wish to run. Valid options are 'Master', 'Slave', 'ShardCoordinator', 'MasterMaintenance', 'RelayMaintenance', 'SlaveMaintenance', 'Notifier'",
                v => parameters.agentType = (AgentType)Enum.Parse(typeof(AgentType),v)}
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
