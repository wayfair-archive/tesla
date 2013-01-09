using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Net.Sockets;
using TeslaSQL.DataUtils;
using System.IO;
using System.Diagnostics;
using log4net;
using TeslaSQL;

namespace TeslaSQL {
    public class Logger {
        private string errorLogDB { get; set; }
        public IDataUtils dataUtils { private get; set; }
        private readonly string logFile;

        private static IEnumerable<ILog> logs;

        static Logger() {
            ILog consoleLog = LogManager.GetLogger("console");
            ILog fileLog = LogManager.GetLogger("file");
            ILog grayLog = LogManager.GetLogger("graylog");
            logs = new List<ILog> { 
                consoleLog,
                fileLog,
                grayLog
            };
        }
        private StatsdPipe statsd;

        public Logger(LogLevel logLevel, string statsdHost, string statsdPort, string errorLogDB, string logFile) {
            this.errorLogDB = errorLogDB;
            try {
                if (!File.Exists(logFile)) {
                    //holds onto a file handle if you dont close it.
                    File.Create(logFile).Close();
                }
                this.logFile = logFile;
            } catch (Exception) {
                this.logFile = null;
            }

            try {
                this.statsd = new StatsdPipe(statsdHost, int.Parse(statsdPort));
                Log(String.Format("Building statsdpipe: {0}:{1}", statsdHost, statsdPort), LogLevel.Trace);
            } catch {
                Log("Invalid or empty config values for statsdHost and statsdPipe; not logging to StatsD this run", LogLevel.Warn);
            }

        }

        public Logger(LogLevel logLevel, string statsdHost, string statsdPort, string errorLogDB, string logFile, IDataUtils dataUtils)
            : this(logLevel, statsdHost, statsdPort, errorLogDB, logFile) {
            this.dataUtils = dataUtils;
        }

        public void Timing(string key, int value, double sampleRate = 1.0) {
            if (statsd == null) { return; }
            statsd.Timing(key, value, sampleRate);
            Log(String.Format("Timing: {0}, {1} @{2}", key, value, sampleRate), LogLevel.Trace);
        }

        public void Increment(string key, int magnitude, double sampleRate = 1.0) {
            if (statsd == null) { return; }
            statsd.Increment(key, magnitude, sampleRate);
        }
        /// <summary>
        /// Logs information and writes it to the console
        /// </summary>
        /// <param name="message">The message to log</param>
        /// <param name="level">LogLevel value, gets compared to the configured logLevel variable</param>
        public void Log(object message, LogLevel level) {
            switch (level) {
                case LogLevel.Trace:
                    foreach (var log in logs) { log.Debug(message); }
                    break;
                case LogLevel.Debug:
                    foreach (var log in logs) { log.Debug(message); }
                    break;
                case LogLevel.Info:
                    foreach (var log in logs) { log.Info(message); }
                    break;
                case LogLevel.Warn:
                    foreach (var log in logs) { log.Warn(message); }
                    break;
                case LogLevel.Error:
                    foreach (var log in logs) { log.Error(message); }
                    break;
                case LogLevel.Critical:
                    foreach (var log in logs) { log.Fatal(message); }
                    break;
            }

            //errors are special - they are exceptions that don't stop the program but we want to write them to a database
            //table
            if (level.Equals(LogLevel.Error) && errorLogDB != null && dataUtils != null) {
                string error = "Agent: " + Config.agentType;
                if (Config.master != null) {
                    error += " Master: " + Config.master;
                    error += " DB: " + Config.masterDB;
                } else if (Config.slave != null) {
                    error += " Slave: " + Config.slave;
                    error += " DB: " + Config.slaveDB;
                } else {
                    error += " Relay: " + Config.relayServer;
                    error += " DB: " + Config.relayDB;
                }
                error += " - " + message;
                dataUtils.LogError(message.ToString());
            }
        }
        public void Log(Exception e, string message = null) {
            if (message == null) {
                message = e.StackTrace.ToString();
            } else {
                message = message + "\r\n" + e.ToString();
            }
            Log(message, LogLevel.Error);
        }

        /// <summary>
        /// Subclass for logging statistics to statsd
        /// Taken from https://github.com/etsy/statsd/blob/master/examples/csharp_example.cs
        ///
        /// Class to send UDP packets to a statsd instance.
        /// It is advisable to use StatsdSingleton instead of this class directly, due to overhead of opening/closing connections.
        /// </summary>
        /// <example>
        /// //Non-singleton version
        /// StatsdPipe statsd = new StatsdPipe("10.20.30.40", "8125");
        /// statsd.Increment("mysuperstat");
        /// </example>
        public class StatsdPipe : IDisposable {
            private readonly UdpClient udpClient;

            [ThreadStatic]
            private static Random random;

            private static Random Random {
                get {
                    return random ?? (random = new Random());
                }
            }

            public StatsdPipe(string host, int port) {
                udpClient = new UdpClient(host, port);
            }

            public bool Gauge(string key, int value) {
                return Gauge(key, value, 1.0);
            }

            public bool Gauge(string key, int value, double sampleRate) {
                return Send(sampleRate, String.Format("{0}:{1:d}|g", key, value));
            }

            public bool Timing(string key, int value) {
                return Timing(key, value, 1.0);
            }

            public bool Timing(string key, int value, double sampleRate) {
                return Send(sampleRate, String.Format("{0}:{1:d}|ms", key, value));
            }

            public bool Decrement(string key) {
                return Increment(key, -1, 1.0);
            }

            public bool Decrement(string key, int magnitude) {
                return Decrement(key, magnitude, 1.0);
            }

            public bool Decrement(string key, int magnitude, double sampleRate) {
                magnitude = magnitude < 0 ? magnitude : -magnitude;
                return Increment(key, magnitude, sampleRate);
            }

            public bool Decrement(params string[] keys) {
                return Increment(-1, 1.0, keys);
            }

            public bool Decrement(int magnitude, params string[] keys) {
                magnitude = magnitude < 0 ? magnitude : -magnitude;
                return Increment(magnitude, 1.0, keys);
            }

            public bool Decrement(int magnitude, double sampleRate, params string[] keys) {
                magnitude = magnitude < 0 ? magnitude : -magnitude;
                return Increment(magnitude, sampleRate, keys);
            }

            public bool Increment(string key) {
                return Increment(key, 1, 1.0);
            }

            public bool Increment(string key, int magnitude) {
                return Increment(key, magnitude, 1.0);
            }

            public bool Increment(string key, int magnitude, double sampleRate) {
                string stat = String.Format("{0}:{1}|c", key, magnitude);
                return Send(stat, sampleRate);
            }

            public bool Increment(int magnitude, double sampleRate, params string[] keys) {
                return Send(sampleRate, keys.Select(key => String.Format("{0}:{1}|c", key, magnitude)).ToArray());
            }

            protected bool Send(String stat, double sampleRate) {
                return Send(sampleRate, stat);
            }

            protected bool Send(double sampleRate, params string[] stats) {
                var retval = false; // didn't send anything
                if (sampleRate < 1.0) {
                    foreach (var stat in stats) {
                        if (Random.NextDouble() <= sampleRate) {
                            var statFormatted = String.Format("{0}|@{1:f}", stat, sampleRate);
                            if (DoSend(statFormatted)) {
                                retval = true;
                            }
                        }
                    }
                } else {
                    foreach (var stat in stats) {
                        if (DoSend(stat)) {
                            retval = true;
                        }
                    }
                }

                return retval;
            }

            protected bool DoSend(string stat) {
                var data = Encoding.Default.GetBytes(stat + "\n");

                udpClient.Send(data, data.Length);
                return true;
            }

            #region IDisposable Members

            public void Dispose() {
                try {
                    if (udpClient != null) {
                        udpClient.Close();
                    }
                } catch {
                }
            }

            #endregion
        }


    }
}
