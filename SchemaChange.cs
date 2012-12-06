using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml;

namespace TeslaSQL {
    class SchemaChange {
        public SchemaChangeType eventType { get; set; }

        public int ddeID { get; set; }

        public string schemaName { get; set; }

        public string tableName { get; set; }

        public string columnName { get; set; }

        public string newColumnName { get; set; }

        public DataType dataType { get; set; }

        public SchemaChange(SchemaChangeType eventType, string schemaName, string tableName, string columnName, string newColumnName, DataType dataType = null) {
            this.eventType = eventType;
            this.schemaName = schemaName;
            this.tableName = tableName;
            this.columnName = columnName;
            this.newColumnName = newColumnName;
            this.dataType = dataType;
        }
    }

}
