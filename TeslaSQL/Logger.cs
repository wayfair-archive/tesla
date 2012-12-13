using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Net.Sockets;
using TeslaSQL.DataUtils;
using System.IO;
using System.Diagnostics;

namespace TeslaSQL {
    public class Logger {

        private LogLevel logLevel { get; set; }
        private string statsdHost { get; set; }
        private string statsdPort { get; set; }
        private string errorLogDB { get; set; }
        public IDataUtils dataUtils { private get; set; }
        private readonly string fileName = "tesla.log";

        public Logger(LogLevel logLevel, string statsdHost, string statsdPort, string errorLogDB) {
            this.logLevel = logLevel;
            this.statsdHost = statsdHost;
            this.statsdPort = statsdPort;
            this.errorLogDB = errorLogDB;
        }

        public Logger(LogLevel logLevel, string statsdHost, string statsdPort, string errorLogDB, IDataUtils dataUtils) {
            this.logLevel = logLevel;
            this.statsdHost = statsdHost;
            this.statsdPort = statsdPort;
            this.errorLogDB = errorLogDB;
            this.dataUtils = dataUtils;
        }

        /// <summary>
        /// Logs information and writes it to the console
        /// </summary>
        /// <param name="message">The message to log</param>
        /// <param name="logLevel">LogLevel value, gets compared to the configured logLevel variable</param>
        public void Log(string message, LogLevel logLevel) {
            //compareto method returns a number less than 0 if logLevel is less than configured
            if (logLevel.CompareTo(this.logLevel) >= 0) {
                var frame = new StackFrame(1);
                var method = frame.GetMethod();
                var obj = method.DeclaringType.ToString();
                var newMessage = DateTime.Now + ": " + logLevel + ": " + obj + " " + method + "\r\n\t" + message;
                Console.WriteLine(newMessage);
                using (var writer = new StreamWriter(fileName, true)) {
                    writer.WriteLine(newMessage);
                }
            }

            //errors are special - they are exceptions that don't stop the program but we want to write them to a database
            //table
            if (logLevel.Equals(LogLevel.Error) && dataUtils != null) {
                dataUtils.LogError(message);
            }
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

        /// <summary>
        /// A singleton to handle Statd messages.
        /// ***Requires "statsdhost" and "statsdport" in Config***
        /// </summary>
        /// <example>
        /// StatsdSingleton.Instance.Increment("hitcount");
        /// </example>
        /* TODO fix
        public sealed class StatsdSingleton {

            private static readonly Lazy<StatsdPipe> lazy = new Lazy<StatsdPipe>(() => new StatsdPipe(statsdHost, Int16.Parse(statsdPort)));

            public static StatsdPipe Instance { get { return lazy.Value; } }
        }
         */
    }
}
