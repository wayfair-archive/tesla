using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

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
        //warnings 
        Warn = 3,
        //errors that are not critical (i.e. tables that don't stopOnError)
        Error = 4,
        //critical errors that cause us to exit the program
        Critical = 5
    }


    /// <summary>
    /// Bitwise values for tracking the progress of a batch
    /// </summary>
    public enum SyncBitWise {
        PublishSchemaChanges = 1,
        CaptureChanges = 2,
        UploadChanges = 4,
        DownloadChanges = 8,
        ApplySchemaChanges = 16,
        ConsolidateBatches = 32,
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
    /// Different types of query results that the SqlQuery method supports
    /// </summary>
    public enum ResultType {
        DATASET,
        DATATABLE,
        DATAROW,
        INT32,
        INT64,
        STRING,
        DATETIME
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
