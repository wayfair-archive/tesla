using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace TeslaSQL
{

    /// <summary>
    /// Defines which task this agent should run
    /// </summary>
    public enum AgentType
    {
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
    public enum LogLevel
    {
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
    public enum SyncBitWise
    {
        CaptureChanges = 1,
        UploadChanges = 2,
        DownloadChanges = 4,
        ApplySchemaChanges = 8,
        ConsolidateBatches = 16,
        ApplyChanges = 32,
        SyncHistoryTables = 64
    }

    /// <summary>
    /// Servers we can connect to
    /// </summary>
    public enum TServer
    {
        MASTER,
        SLAVE,
        RELAY
    }


    /// <summary>
    /// Different types of query results that the SqlQuery method supports
    /// </summary>
    public enum ResultType
    {
        DATASET,
        DATATABLE,
        DATAROW,
        INT32,
        INT64,
        STRING,
        DATETIME
    }

}
