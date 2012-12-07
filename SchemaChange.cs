using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml;

namespace TeslaSQL {
    /// <summary>
    /// An instance of this class represents a parsed schema change event
    /// </summary>
    class SchemaChange {
        public int ddeID { get; set; }

        public SchemaChangeType eventType { get; set; }

        public string schemaName { get; set; }

        public string tableName { get; set; }

        public string columnName { get; set; }

        public string newColumnName { get; set; }
        
        public DataType dataType { get; set; }

        /// <summary>
        /// Construct a SchemaChange event
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
    }

}
