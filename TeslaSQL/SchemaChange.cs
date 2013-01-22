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
        public int DdeID { get; private set; }

        public SchemaChangeType EventType { get; private set; }

        public string SchemaName { get; private set; }

        public string TableName { get; private set; }

        public string ColumnName { get; private set; }

        public string NewColumnName { get; private set; }

        public DataType DataType { get; private set; }

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
            this.DdeID = ddeID;
            this.EventType = eventType;
            this.SchemaName = schemaName;
            this.TableName = tableName;
            this.ColumnName = columnName;
            this.NewColumnName = newColumnName;
            this.DataType = dataType;
        }


        /// <summary>
        /// Constructor used on the slave side when reading published schema change events
        /// </summary>
        /// <param name="row">DataRow from querying schema change table</param>
        public SchemaChange(DataRow row) {
            DdeID = row.Field<int>("CscDdeID");
            EventType = (SchemaChangeType)Enum.Parse(typeof(SchemaChangeType), row.Field<string>("CscEventType"));
            SchemaName = row.Field<string>("CscSchema");
            TableName = row.Field<string>("CscTableName");
            ColumnName = row.Field<string>("CscColumnName");
            NewColumnName = row.Field<string>("CscNewColumnName");
            DataType = new DataType(row.Field<string>("CscBaseDataType"), row.Field<int?>("CscCharacterMaximumLength"),
                row.Field<int?>("CscNumericPrecision"), row.Field<int?>("CscNumericScale"));
        }

        /// <summary>
        /// Compare two schema changes, used for unit tests to compare expected and actual schema change objects
        /// </summary>
        /// <param name="toCompare">The schema change to compare to</param>
        /// <returns>True if all properties are the same, false otherwise</returns>
        public bool Equals(SchemaChange toCompare) {
            return (DdeID == toCompare.DdeID
                && EventType == toCompare.EventType
                && SchemaName == toCompare.SchemaName
                && TableName == toCompare.TableName
                && ColumnName == toCompare.ColumnName
                && NewColumnName == toCompare.NewColumnName
                && DataType.Equals(DataType, toCompare.DataType));
        }

        public override string ToString() {
            return new { SchemaChangeType = EventType, Schema = SchemaName, Table = TableName, Column = ColumnName, NewColumn = NewColumnName }.ToString();
        }
    }

}
