using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using log4net.Core;

namespace TeslaSQL {

    /// <summary>
    /// Defines which task this agent should run
    /// </summary>
    public enum AgentType {
        Master,
        Slave,
        ShardCoordinator,
        Notifier,
        RelayMaintenance,
        MasterMaintenance,
        SlaveMaintenance
    }


    /// <summary>
    /// Log levels that define how much information to write to logs
    /// </summary>
    public enum LogLevel {
        //the most detailed, outputs to console only
        Trace = 1,
        //debugging info
        Debug = 2,
        //information that is generally relevant even in a non-debugging scenario
        Info = 3,
        //warnings
        Warn = 4,
        //errors that are not critical (i.e. tables that don't stopOnError)
        Error = 5,
        //critical errors that cause us to exit the program
        Critical = 6
    }


    /// <summary>
    /// Bitwise values for tracking the progress of a batch
    /// </summary>
    [FlagsAttribute]
    public enum SyncBitWise {
        PublishSchemaChanges = 1,
        CaptureChanges = 2,
        UploadChanges = 4,
        ApplySchemaChanges = 8,
        ConsolidateBatches = 16,
        DownloadChanges = 32,
        ApplyChanges = 64,
        SyncHistoryTables = 128
    }

    /// <summary>
    /// Servers we can connect to
    /// </summary>
    public enum TServer {
        MASTER,
        SLAVE,
        RELAY
    }

    /// <summary>
    /// List of supported sql databases
    /// </summary>
    public enum SqlFlavor {
        MSSQL,
        Netezza
    }

    /// <summary>
    /// Types of table schema changes that can be published/applied by Tesla
    /// </summary>
    public enum SchemaChangeType {
        //adding a column
        Add,
        //dropping a column
        Drop,
        //changing a column's data type/length
        Modify,
        //Renaming a column
        Rename
    }

}
