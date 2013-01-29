using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using TeslaSQL.DataUtils;
using System.IO;

namespace TeslaSQL.Agents {
    /// <summary>
    /// This class is used for cleaning up old data on the slave (such as changetables for old batches)
    /// </summary>
    class SlaveMaintenance : Agent {
        private IDataUtils relayDataUtils { get { return sourceDataUtils; } }
        private IDataUtils slaveDataUtils { get { return destDataUtils; } }
        //base keyword invokes the base class's constructor
        public SlaveMaintenance(IDataUtils relayDataUtils, IDataUtils slaveDataUtils, Logger logger)
            : base(relayDataUtils, slaveDataUtils, logger) {
        }

        public override void ValidateConfig() {
            Config.ValidateRequiredHost(Config.Slave);
            if (Config.SlaveType == SqlFlavor.None) {
                throw new Exception("SlaveMaintenance agent requires a valid SQL flavor for slave");
            }
        }
        public override void Run() {
            var chopDate = DateTime.Now - new TimeSpan(Config.ChangeRetentionHours, 0, 0);
            IEnumerable<long> CTIDs = relayDataUtils.GetOldCTIDsSlave(Config.RelayDB, chopDate, Config.Slave);
            var tables = slaveDataUtils.GetTables(Config.SlaveCTDB);
            if (tables.Count() > 0) {
                logger.Log("Deleting {" + string.Join(",", CTIDs) + "} from { " + string.Join(",", tables.Select(t => t.name)) + "}", LogLevel.Info);
                MaintenanceHelper.DeleteOldTables(CTIDs, tables, slaveDataUtils, Config.SlaveCTDB);
            } else {
                logger.Log("No tables to delete", LogLevel.Info);
            }
            if (Config.BcpPath.Length > 0) {
                DeleteOldFiles(CTIDs, Config.SlaveCTDB, Config.BcpPath);
            }
        }
        
        private void DeleteOldFiles(IEnumerable<long> CTIDs, string dbName, string BcpPath) {
            string path = BcpPath.TrimEnd('\\') + @"\" + dbName.ToLower();
            logger.Log("Deleting files in " + path + " for old CTIDs", LogLevel.Info);
            var dir = new DirectoryInfo(path);  
            if (dir.Exists) {
                foreach(long CTID in CTIDs) {
                    logger.Log("Deleting files for CTID " + CTID, LogLevel.Debug);
                    FileInfo[] files = dir.GetFiles(string.Format("*_{0}.txt", CTID));
                    foreach (FileInfo file in files) {
                        try {
                            logger.Log("Deleting file " + file.FullName, LogLevel.Trace);
                            file.Delete();
                        } catch (IOException) {
                            //this means someone else has the file open, we'll just log a warning
                            logger.Log("Unable to delete file " + file.FullName + " because it is in use.", LogLevel.Warn);
                        } catch (System.Security.SecurityException) {
                            //this means we don't have permissions. if that's the case none of the files are going to work
                            //so we should just throw
                            throw;
                        } catch (UnauthorizedAccessException) {
                            //this means somehow we tried to delete a directory as though it were a file.
                            //this is unlikely unless someone creates a directory that looks like a file
                            logger.Log("Unable to delete file " + file.FullName + " because it is a directory.", LogLevel.Warn);
                        }
                    }

                }
            }
        }
    }
}
