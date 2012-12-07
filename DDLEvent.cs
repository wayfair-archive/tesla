using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml;
using System.Data;
using System.Data.SqlTypes;
using Xunit;

namespace TeslaSQL {
    /// <summary>
    /// Class repsenting a DDL event captured by a t-sql DDL trigger
    /// </summary>
    class DDLEvent {
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
        /// <param name="t_array">Array of table configuration objects</param>
        /// <param name="TServer">Server to connect to if we need to retrieve data type info</param>
        /// <param name="dbName">Database for retrieving data type info</param>
        public List<SchemaChange> Parse(TableConf[] t_array, IDataUtils dataUtils, TServer server, string dbName) {
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

            if (node.FirstChild.Name == "Param") {
                tableName = xml.SelectSingleNode("/EVENT_INSTANCE/TargetObjectName").InnerText;
            } else {
                tableName = xml.SelectSingleNode("/EVENT_INSTANCE/ObjectName").InnerText;
            }

            string schemaName = xml.SelectSingleNode("/EVENT_INSTANCE/SchemaName").InnerText;

            //String.Compare method returns 0 if the strings are equal, the third "true" flag is for a case insensitive comparison
            //Get table config object
            TableConf t = t_array.SingleOrDefault(item => String.Compare(item.Name, tableName, ignoreCase: true) == 0);

            if (t == null) {
                //the DDL event applies to a table not in our config, so we just ignore it
                return schemaChanges;
            }


            switch (node.FirstChild.Name) {
                case "Param":
                    changeType = SchemaChangeType.Rename;
                    columnName = xml.SelectSingleNode("/EVENT_INSTANCE/ObjectName").InnerText;
                    newColumnName = xml.SelectSingleNode("/EVENT_INSTANCE/NewObjectName").InnerText;
                    sc = new SchemaChange(ddeID, changeType, schemaName, tableName, columnName, newColumnName);
                    schemaChanges.Add(sc);
                    break;
                case "Alter":
                    changeType = SchemaChangeType.Modify;
                    foreach (XmlNode xColumn in xml.SelectNodes("/EVENT_INSTANCE/AlterTableActionList/Alter/Columns/Name")) {
                        columnName = xColumn.InnerText;
                        dataType = ParseDataType(dataUtils.GetDataType(server, dbName, tableName, columnName));
                        sc = new SchemaChange(ddeID, changeType, schemaName, tableName, columnName, null, dataType);
                        schemaChanges.Add(sc);
                    }
                    break;
                case "Create":
                    changeType = SchemaChangeType.Add;
                    tableName = xml.SelectSingleNode("/EVENT_INSTANCE/ObjectName").InnerText;
                    foreach (XmlNode xColumn in xml.SelectNodes("/EVENT_INSTANCE/AlterTableActionList/Create/Columns/Name")) {
                        columnName = xColumn.InnerText;
                        //if column list is specified, only publish schema changes if the column is already in the list. we don't want
                        //slaves adding a new column that we don't plan to publish changes for. 
                        if (t.columnList != null && t.columnList.Contains(columnName, StringComparer.OrdinalIgnoreCase)) {
                            dataType = ParseDataType(dataUtils.GetDataType(server, dbName, tableName, columnName));
                            sc = new SchemaChange(ddeID, changeType, schemaName, tableName, columnName, null, dataType);
                            schemaChanges.Add(sc);
                        }
                    }
                    break;
                case "Drop":
                    changeType = SchemaChangeType.Drop;
                    tableName = xml.SelectSingleNode("/EVENT_INSTANCE/ObjectName").InnerText;
                    foreach (XmlNode xColumn in xml.SelectNodes("/EVENT_INSTANCE/AlterTableActionList/Drop/Columns/Name")) {
                        //if columnlist for this table is specified
                        columnName = xColumn.InnerText;
                        sc = new SchemaChange(ddeID, changeType, schemaName, tableName, columnName, null);
                        schemaChanges.Add(sc);
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
            return new DataType(
                row.Field<string>("DATA_TYPE"),
                row.Field<int>("CHARACTER_MAXIMUM_LENGTH"),
                row.Field<int>("NUMERIC_PRECISION"),
                row.Field<int>("NUMERIC_SCALE")
                );
        }
        #region unittests
        [Fact]
        public void TestParse() {
            String xml = @"<EVENT_INSTANCE>
  <EventType>ALTER_TABLE</EventType>
  <PostTime>2012-12-07T12:19:17.287</PostTime>
  <SPID>121</SPID>
  <ServerName>BRONCO</ServerName>
  <LoginName>CSNZOO\ssandler</LoginName>
  <UserName>dbo</UserName>
  <DatabaseName>csn_cttest</DatabaseName>
  <SchemaName>dbo</SchemaName>
  <ObjectName>tblTest1</ObjectName>
  <ObjectType>TABLE</ObjectType>
  <AlterTableActionList>
    <Create>
      <Columns>
        <Name>col4</Name>
      </Columns>
    </Create>
  </AlterTableActionList>
  <TSQLCommand>
    <SetOptions ANSI_NULLS=""ON"" ANSI_NULL_DEFAULT=""ON"" ANSI_PADDING=""ON"" QUOTED_IDENTIFIER=""ON"" ENCRYPTED=""FALSE"" />
    <CommandText>alter table tblTest1 add col4 int
</CommandText>
  </TSQLCommand>
</EVENT_INSTANCE>";

        }
        #endregion
    }
}
