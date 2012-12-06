﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace TeslaSQL {
    class DDLEvent {
        public SchemaChangeType eventType { get; set; }

        public string schemaName { get; set; }

        public string tableName { get; set; }

        public string columnName { get; set; }

        public string previousColumnName { get; set; }

        public string commandText { get; set; }

        public DataType dataType { get; set; }

        public DDLEvent(SchemaChangeType eventType, string schemaName, string tableName, string columnName, string previousColumnName, string commandText, DataType dataType = null) {
            this.eventType = eventType;
            this.schemaName = schemaName;
            this.tableName = tableName;
            this.columnName = columnName;
            this.previousColumnName = previousColumnName;
            this.commandText = commandText;
            this.dataType = dataType;
        }
    }

}