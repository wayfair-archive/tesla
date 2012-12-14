#region Using Statements
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml;
using System.Data;
using System.Data.SqlTypes;
using Xunit;
using Xunit.Extensions;
using TeslaSQL.DataUtils;
#endregion

namespace TeslaSQL {
    /// <summary>
    /// Class repsenting a DDL event captured by a t-sql DDL trigger
    /// </summary>
    public class DDLEvent {
        public int ddeID { get; set; }

        public string eventData { get; set; }

        /// <summary>
        /// Constructor used when initializing this object based on data from a DDL trigger
        /// </summary>
        /// <param name="ddeID">Unique id for this event</param>
        /// <param name="eventData">XmlDocument from the EVENTDATA() SQL function</param>
        public DDLEvent(int ddeID, string eventData) {
            this.ddeID = ddeID;
            this.eventData = eventData;
        }

        /// <summary>
        /// Parse XML EVENTDATA and create zero or more SchemaChange objects from that
        /// </summary>
        /// <param name="tables">Array of table configuration objects</param>
        /// <param name="dbName">Database for retrieving data type info</param>
        public List<SchemaChange> Parse(TableConf[] tables, IDataUtils dataUtils, string dbName) {
            var schemaChanges = new List<SchemaChange>();
            string columnName;
            string tableName;
            SchemaChangeType changeType;
            DataType dataType;
            SchemaChange sc;
            string newColumnName;
            XmlNode node;
            var xml = new XmlDocument();
            xml.LoadXml(eventData);

            string eventType = xml.SelectSingleNode("EVENT_INSTANCE/EventType").InnerText;

            if (eventType == "ALTER_TABLE") {
                node = xml.SelectSingleNode("EVENT_INSTANCE/AlterTableActionList");
            } else if (eventType == "RENAME") {
                node = xml.SelectSingleNode("EVENT_INSTANCE/Parameters");
            } else {
                //this is a DDL event type that we don't care about publishing, so ignore it
                return schemaChanges;
            }

            if (node == null) {
                //we'll get here on events that do an ALTER_TABLE but don't change any columns,
                //such as "alter table enable change_tracking"
                return schemaChanges;
            }

            if (node.FirstChild.Name == "Param") {
                tableName = xml.SelectSingleNode("/EVENT_INSTANCE/TargetObjectName").InnerText;
            } else {
                tableName = xml.SelectSingleNode("/EVENT_INSTANCE/ObjectName").InnerText;
            }

            string schemaName = xml.SelectSingleNode("/EVENT_INSTANCE/SchemaName").InnerText;

            //String.Compare method returns 0 if the strings are equal, the third "true" flag is for a case insensitive comparison
            //Get table config object
            TableConf t = tables.SingleOrDefault(item => String.Compare(item.Name, tableName, ignoreCase: true) == 0);

            if (t == null) {
                //the DDL event applies to a table not in our config, so we just ignore it
                return schemaChanges;
            }

            switch (node.FirstChild.Name) {
                case "Param":
                    changeType = SchemaChangeType.Rename;
                    columnName = xml.SelectSingleNode("/EVENT_INSTANCE/ObjectName").InnerText;
                    newColumnName = xml.SelectSingleNode("/EVENT_INSTANCE/NewObjectName").InnerText;
                    if (t.columnList == null || t.columnList.Contains(columnName, StringComparer.OrdinalIgnoreCase)) {
                        sc = new SchemaChange(ddeID, changeType, schemaName, tableName, columnName, newColumnName);
                        schemaChanges.Add(sc);
                    }
                    break;
                case "Alter":
                    changeType = SchemaChangeType.Modify;
                    foreach (XmlNode xColumn in xml.SelectNodes("/EVENT_INSTANCE/AlterTableActionList/Alter/Columns/Name")) {
                        columnName = xColumn.InnerText;
                        if (t.columnList == null || t.columnList.Contains(columnName, StringComparer.OrdinalIgnoreCase)) {
                            dataType = ParseDataType(dataUtils.GetDataType(dbName, tableName, schemaName, columnName));
                            sc = new SchemaChange(ddeID, changeType, schemaName, tableName, columnName, null, dataType);
                            schemaChanges.Add(sc);
                        }
                    }
                    break;
                case "Create":
                    changeType = SchemaChangeType.Add;
                    tableName = xml.SelectSingleNode("/EVENT_INSTANCE/ObjectName").InnerText;
                    foreach (XmlNode xColumn in xml.SelectNodes("/EVENT_INSTANCE/AlterTableActionList/Create/Columns/Name")) {
                        columnName = xColumn.InnerText;
                        //if column list is specified, only publish schema changes if the column is already in the list. we don't want
                        //slaves adding a new column that we don't plan to publish changes for.
                        //if column list is null, we want changes associated with all columns.
                        if (t.columnList == null || t.columnList.Contains(columnName, StringComparer.OrdinalIgnoreCase)) {
                            var type = dataUtils.GetDataType(dbName, tableName, schemaName, columnName);
                            dataType = ParseDataType(type);
                            sc = new SchemaChange(ddeID, changeType, schemaName, tableName, columnName, null, dataType);
                            schemaChanges.Add(sc);
                        }
                    }
                    break;
                case "Drop":
                    changeType = SchemaChangeType.Drop;
                    tableName = xml.SelectSingleNode("/EVENT_INSTANCE/ObjectName").InnerText;
                    foreach (XmlNode xColumn in xml.SelectNodes("/EVENT_INSTANCE/AlterTableActionList/Drop/Columns/Name")) {                        
                        columnName = xColumn.InnerText;
                        if (t.columnList == null || t.columnList.Contains(columnName, StringComparer.OrdinalIgnoreCase)) {
                            sc = new SchemaChange(ddeID, changeType, schemaName, tableName, columnName, null, null);
                            schemaChanges.Add(sc);
                        }
                    }
                    break;
            }
            return schemaChanges;
        }

        /// <summary>
        /// Given a datarow from GetDataType, turns it into a data type object
        /// </summary>
        /// <param name="row">DataRow containing the appropriate columns from GetDataType</param>
        /// <returns>A TeslaSQL.DataType object</returns>
        public DataType ParseDataType(DataRow row) {
            string dataType = row.Field<string>("DATA_TYPE");
            var characterMaximumLength = row.Field<int?>("CHARACTER_MAXIMUM_LENGTH");
            //Nullable<byte> because there is no such thing as "byte?"
            var numericPrecision = row.Field<Nullable<byte>>("NUMERIC_PRECISION");
            var numericScale = row.Field<int?>("NUMERIC_SCALE");
            return new DataType(
                dataType, characterMaximumLength, numericPrecision, numericScale
                );
        }
    }
}
