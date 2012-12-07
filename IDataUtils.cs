using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using System.Text;
using System.Data.SqlClient;
using System.Data;

namespace TeslaSQL {
    public interface IDataUtils {

        DataRow GetLastCTBatch(TServer server, string dbName, AgentType agentType, string slaveIdentifier = "");

        DataTable GetPendingCTVersions(TServer server, string dbName, Int64 CTID, int syncBitWise);

        DateTime GetLastStartTime(TServer server, string dbName, Int64 CTID, int syncBitWise);

        Int64 GetCurrentCTVersion(TServer server, string dbName);

        Int64 GetMinValidVersion(TServer server, string dbName, string table);

        Int64 CreateCTVersion(TServer server, string dbName, Int64 syncStartVersion, Int64 syncStopVersion);

        int SelectIntoCTTable(TServer server, string sourceCTDB, string masterColumnList, string ctTableName,
            string sourceDB, string tableName, Int64 startVersion, string pkList, Int64 stopVersion, string notNullPkList, int timeout);

        void CreateSlaveCTVersion(TServer server, string dbName, Int64 CTID, string slaveIdentifier,
            Int64 syncStartVersion, Int64 syncStopVersion, DateTime syncStartTime, Int32 syncBitWise);

        void CreateSchemaChangeTable(TServer server, string dbName, Int64 ct_id);

        DataTable GetDDLEvents(TServer server, string dbName, DateTime afterDate);

        void WriteSchemaChange(TServer server, string dbName, Int64 CTID, int ddeID, string eventType, string schemaName, string tableName,
            string columnName, string newColumnName, string baseType, int? characterMaximumLength, int? numericPrecision, int? numericScale);

        DataRow GetDataType(TServer server, string dbName, string table, string column);

        void UpdateSyncStopVersion(TServer server, string dbName, Int64 syncStopVersion, Int64 CTID);

        bool CheckTableExists(TServer server, string dbName, string table);

        string GetIntersectColumnList(TServer server, string dbName, string table1, string table2);

        bool HasPrimaryKey(TServer server, string dbName, string table);

        bool DropTableIfExists(TServer server, string dbName, string table);

        void CopyDataFromQuery(TServer sourceServer, string sourceDB, TServer destServer, string destDB,
            SqlCommand cmd, string destinationTable, int queryTimeout = 36000, int bulkCopyTimeout = 36000);

        void CopyTable(TServer sourceServer, string sourceDB, string table, TServer destServer, string destDB, int timeout);

        void CopyTableDefinition(TServer sourceServer, string sourceDB, string table, TServer destServer, string destDB);

        string ScriptTable(TServer server, string dbName, string table);

        Dictionary<string, bool> GetFieldList(TServer server, string dbName, string table);

        void WriteBitWise(TServer server, string dbName, Int64 ct_id, int value, AgentType agentType);

        int ReadBitWise(TServer server, string dbName, Int64 ct_id, AgentType agentType);

        void MarkBatchComplete(TServer server, string dbName, Int64 ct_id, Int32 syncBitWise, DateTime syncStopTime,
            AgentType agentType, string slaveIdentifier = "");

        DataTable GetSchemaChanges(TServer server, string dbName, Int64 CTID);

        Int64 GetTableRowCount(TServer server, string dbName, string table);

        bool IsChangeTrackingEnabled(TServer server, string dbName, string table);
    }
}
