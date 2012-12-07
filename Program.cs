#region Using Statements
using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using NDesk.Options;
using Xunit;
using TeslaSQL.Agents;
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
        //various methods can call this to shut down
        //TODO decide if we need this, where to put it, and what it should do besides exit.
        public static void ShutdownHandler() {
            Environment.Exit(1);
        }

        //TODO figure out why --logLevel is unprocessed
        //TODO figure out why latest batch didn't do anything

        static void Main(string[] args) {            
            Params parameters = new Params();
            try {
                parameters = ParseArgs(args);
            } catch (Exception e) {
                Console.Write("TeslaSQL: ");
                Console.WriteLine(e.Message);
                Console.WriteLine("Try `TeslaSQL --help' for more information.");
                Environment.Exit(1); //TODO throw; instead?
            }

            if (parameters.showHelp) {
                ShowHelp(parameters.optionSet);
                return;
            }

            if (String.IsNullOrEmpty(parameters.configFile) || !ValidatePath(parameters.configFile)) {
                Console.WriteLine("Please specify a valid config file path!");
                Environment.Exit(1);
            }

            Console.WriteLine("TeslaSQL -- loading configuration file");
            var config = new Config();
            config.Load(parameters.configFile);
            var logger = new Logger(config.logLevel, config.statsdHost, config.statsdPort, config.errorLogDB);
            logger.Log("Configuration file successfully loaded", LogLevel.Debug);

            if (parameters.validate) {
                config.DumpConfig(parameters.more, config);
                return;
            }

            if (!String.IsNullOrEmpty(parameters.logLevelOverride)) {
                try {
                    config.logLevelOverride = (LogLevel)Enum.Parse(typeof(LogLevel), parameters.logLevelOverride);
                } catch {
                    Console.WriteLine("Invalid log level!");
                    Console.WriteLine("Try `TeslaSQL --help' for more information.");
                    Environment.Exit(1);
                }
            }

            //int res = Functions.Run();
            //Console.Write("Test returned: ");
            //Console.WriteLine(res);

            //run appropriate agent type and exit with resulting exit code
            int responseCode = 0;
            var dataUtils = (IDataUtils)new DataUtils(config, logger);
            try {                
                Agent a = createAgent(config.agentType, config, dataUtils);
                logger.Log("Running agent of type " + Convert.ToString(config.agentType), LogLevel.Info);
                a.Run();
            } catch (Exception e) {
                logger.Log("ERROR: " + e.Message + " - Stack Trace: " + e.StackTrace, LogLevel.Critical);
                responseCode = 1;
            }
            //TODO remove this
            logger.Log("Agent completed successfully", LogLevel.Info);
            Console.ReadLine();
            Environment.Exit(responseCode);
        }        

        private static Agent createAgent(AgentType agentType, Config config, IDataUtils dataUtils) {
            switch (agentType) {
                case AgentType.Master:
                    var master = new Master(config, dataUtils);
                    return master;
                case AgentType.Slave:
                    var slave = new Slave(config, dataUtils);
                    return slave;
                case AgentType.ShardCoordinator:
                    var shardCoordinator = new ShardCoordinator(config, dataUtils);
                    return shardCoordinator;
                case AgentType.Notifier:
                    var notifier = new Notifier(config, dataUtils);
                    return notifier;
                case AgentType.MasterMaintenance:
                    var masterMaintenance = new MasterMaintenance(config, dataUtils);
                    return masterMaintenance;
                case AgentType.RelayMaintenance:
                    var relayMaintenance = new RelayMaintenance(config, dataUtils);
                    return relayMaintenance;
                case AgentType.SlaveMaintenance:
                    var slaveMaintenance = new SlaveMaintenance(config, dataUtils);
                    return slaveMaintenance;
            }
            throw new Exception("Invalid agent type: " + agentType);
        }


        /// <summary>
        /// Struct holding data related to command line arguments
        /// </summary>
        private struct Params {
            public string configFile { get; set; }
            public string logLevelOverride { get; set; }
            public bool validate { get; set; }
            public int more { get; set; }
            public bool showHelp { get; set; }
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
        private static Params ParseArgs(string[] args) {
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

        #region ParseArgs Unit Tests
        [Fact]
        public void TestParseArgs() {
            Params parameters = new Params();
            string[] testargs;
            //test basic parameter
            testargs = new string[2] { "-c", "C:\test.txt" };
            parameters = ParseArgs(testargs);
            Assert.Equal("C:\test.txt", parameters.configFile);
            Assert.Equal(false, parameters.validate);
            Assert.Equal(false, parameters.showHelp);
            Assert.True(String.IsNullOrEmpty(parameters.logLevelOverride));

            //config file and validate param
            testargs = new string[3] { "-c", "C:\test.txt", "--validate" };
            parameters = ParseArgs(testargs);
            Assert.Equal("C:\test.txt", parameters.configFile);
            Assert.Equal(true, parameters.validate);
            Assert.Equal(false, parameters.showHelp);

            //config file and logLevelOverride
            testargs = new string[4] { "-c", "C:\test.txt", "-l", "Debug" };
            parameters = ParseArgs(testargs);
            Assert.Equal("C:\test.txt", parameters.configFile);
            Assert.Equal("Debug", parameters.logLevelOverride);
            Assert.Equal(false, parameters.validate);
            Assert.Equal(false, parameters.showHelp);

            //help param
            testargs = new string[1] { "--help" };
            parameters = ParseArgs(testargs);
            Assert.True(String.IsNullOrEmpty(parameters.configFile));
            Assert.Equal(false, parameters.validate);
            Assert.Equal(true, parameters.showHelp);

            //help param with another param
            testargs = new string[3] { "-c", "C:\test.txt", "-h" };
            parameters = ParseArgs(testargs);
            Assert.Equal(false, parameters.validate);
            Assert.Equal(true, parameters.showHelp);

            //add the more param
            testargs = new string[5] { "-c", "C:\test.txt", "--validate", "--more", "30" };
            parameters = ParseArgs(testargs);
            Assert.Equal("C:\test.txt", parameters.configFile);
            Assert.Equal(true, parameters.validate);
            Assert.Equal(false, parameters.showHelp);
            Assert.Equal(30, parameters.more);

            //add logLevel
            testargs = new string[7] { "-c", "C:\test.txt", "--validate", "-m", "30", "--loglevel", "Warn" };
            parameters = ParseArgs(testargs);
            Assert.Equal("C:\test.txt", parameters.configFile);
            Assert.Equal(true, parameters.validate);
            Assert.Equal(false, parameters.showHelp);
            Assert.Equal(30, parameters.more);
            Assert.Equal("Warn", parameters.logLevelOverride);

            //invalid --more param should throw an OptionException
            testargs = new string[5] { "-c", "C:\test.txt", "--validate", "--more", "notanint" };
            Assert.Throws<OptionException>(delegate { ParseArgs(testargs); });
        }

        #endregion


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
