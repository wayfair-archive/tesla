using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml;
using System.Data;

namespace TeslaSQL {
    /// <summary>
    /// An instance of this class represents a parsed schema change event
    /// </summary>
    public class SchemaChange {
        public int ddeID { get; set; }

        public SchemaChangeType eventType { get; set; }

        public string schemaName { get; set; }

        public string tableName { get; set; }

        public string columnName { get; set; }

        public string newColumnName { get; set; }

        public DataType dataType { get; set; }

        /// <summary>
        /// Construct a SchemaChange event, used when parsing DDL events
        /// </summary>
        /// <param name="ddeID">Pointer to ddl event ID generated on the master</param>
        /// <param name="eventType">The type of event (i.e. Add, Drop, Modify, Rename a column)</param>
        /// <param name="schemaName">Database table schema name (i.e. dbo)</param>
        /// <param name="tableName">Table the change applies to</param>
        /// <param name="columnName">Column the change applies to </param>
        /// <param name="newColumnName">New column name (for rename events only)</param>
        /// <param name="dataType">Data type of the column (for modify/add events only)</param>
        public SchemaChange(int ddeID, SchemaChangeType eventType, string schemaName, string tableName, string columnName, string newColumnName, DataType dataType = null) {
            this.ddeID = ddeID;
            this.eventType = eventType;
            this.schemaName = schemaName;
            this.tableName = tableName;
            this.columnName = columnName;
            this.newColumnName = newColumnName;
            this.dataType = dataType;
        }


        /// <summary>
        /// Constructor used on the slave side when reading published schema change events
        /// </summary>
        /// <param name="row">DataRow from querying schema change table</param>
        public SchemaChange(DataRow row) {
            ddeID = row.Field<int>("CscDdeID");
            eventType = (SchemaChangeType)Enum.Parse(typeof(SchemaChangeType), row.Field<string>("CscEventType"));
            schemaName = row.Field<string>("CscSchema");
            tableName = row.Field<string>("CscTableName");
            columnName = row.Field<string>("CscColumnName");
            newColumnName = row.Field<string>("CscNewColumnName");
            dataType = new DataType(row.Field<string>("CscBaseDataType"), row.Field<int?>("CscCharacterMaximumLength"), 
                row.Field<int?>("CscNumericPrecision"), row.Field<int?>("CscNumericScale"));
        }

        /// <summary>
        /// Compare two schema changes, used for unit tests to compare expected and actual schema change objects
        /// </summary>
        /// <param name="toCompare">The schema change to compare to</param>
        /// <returns>True if all properties are the same, false otherwise</returns>
        public bool Equals(SchemaChange toCompare) {
            if (ddeID != toCompare.ddeID
                || !eventType.Equals(toCompare.eventType)
                || schemaName != toCompare.schemaName
                || tableName != toCompare.tableName
                || columnName != toCompare.columnName
                || newColumnName != toCompare.newColumnName
                || !DataType.Equals(dataType, toCompare.dataType)
               ) {
                return false;
            } else {
                return true;
            }
        }
    }

}
