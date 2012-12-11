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
        /// <param name="TServer">Server to connect to if we need to retrieve data type info</param>
        /// <param name="dbName">Database for retrieving data type info</param>
        public List<SchemaChange> Parse(TableConf[] tables, IDataUtils dataUtils, TServer server, string dbName) {
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
                            dataType = ParseDataType(dataUtils.GetDataType(server, dbName, tableName, schemaName, columnName));
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
                            var type = dataUtils.GetDataType(server, dbName, tableName, schemaName, columnName);
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

        #region Unit Tests
        //very thorough unit tests below since the above code is easy to get wrong for edge cases if one is not careful
        /// <summary>
        /// Subclass so that we can implement the IUseFixture feature
        /// </summary>
        public class DDLEventTest : IUseFixture<DDLEventTestData> {
            public TableConf[] tables;
            public TestDataUtils dataUtils;

            /// <summary>
            /// xunit will automatically create an instance of DDLEventTestData and pass it to this method before running tests
            /// </summary>
            public void SetFixture(DDLEventTestData data) {
                this.tables = data.tables;
                this.dataUtils = data.dataUtils;
            }

            [Fact]
            public void TestParse_AddSingleColumn_WithoutColumnList() {
                //add a column and make sure it is published
                String xml = @"<EVENT_INSTANCE><EventType>ALTER_TABLE</EventType><SchemaName>dbo</SchemaName>
                <ObjectName>test1</ObjectName><ObjectType>TABLE</ObjectType><AlterTableActionList>
                <Create><Columns><Name>column1</Name></Columns></Create></AlterTableActionList></EVENT_INSTANCE>";
                DDLEvent dde = new DDLEvent(100, xml);
                SchemaChange expected = new SchemaChange(100, SchemaChangeType.Add, "dbo", "test1", "column1", null, new DataType("int", null, (byte)10, 0));
                List<SchemaChange> result = dde.Parse(tables, (IDataUtils)dataUtils, TServer.MASTER, "testdb");
                //assert.equal doesn't work for objects but this does
                Assert.True(result[0].Equals(expected));
            }

            [Fact]
            public void TestParse_AddMultiColumn_WithoutColumnList() {
                //add multiple columns, we should publish each of them
                String xml = @"<EVENT_INSTANCE><EventType>ALTER_TABLE</EventType><SchemaName>dbo</SchemaName>
                <ObjectName>test1</ObjectName><ObjectType>TABLE</ObjectType><AlterTableActionList>
                <Create><Columns><Name>column1</Name><Name>column2</Name></Columns></Create></AlterTableActionList></EVENT_INSTANCE>";
                DDLEvent dde = new DDLEvent(100, xml);
                SchemaChange expected1 = new SchemaChange(100, SchemaChangeType.Add, "dbo", "test1", "column1", null, new DataType("int", null, (byte)10, 0));
                SchemaChange expected2 = new SchemaChange(100, SchemaChangeType.Add, "dbo", "test1", "column2", null, new DataType("varchar", 100, null, null));
                List<SchemaChange> result = dde.Parse(tables, (IDataUtils)dataUtils, TServer.MASTER, "testdb");
                Assert.True(result[0].Equals(expected1));
                Assert.True(result[1].Equals(expected2));
            }

            [Fact]
            public void TestParse_AddColumn_WithColumnNotInList() {
                //schema change for a column which isn't the list, so we shouldn't publish it
                String xml = @"<EVENT_INSTANCE><EventType>ALTER_TABLE</EventType><SchemaName>dbo</SchemaName>
                <ObjectName>test2</ObjectName><ObjectType>TABLE</ObjectType><AlterTableActionList>
                <Create><Columns><Name>column3</Name></Columns></Create></AlterTableActionList></EVENT_INSTANCE>";
                DDLEvent dde = new DDLEvent(100, xml);                
                List<SchemaChange> result = dde.Parse(tables, (IDataUtils)dataUtils, TServer.MASTER, "testdb");
                Assert.Equal(0, result.Count);             
            }

            [Fact]
            public void TestParse_AddColumn_WithColumnInList() {
                //schema change for a column which is in the list, so we should publish it
                String xml = @"<EVENT_INSTANCE><EventType>ALTER_TABLE</EventType><SchemaName>dbo</SchemaName>
                <ObjectName>test2</ObjectName><ObjectType>TABLE</ObjectType><AlterTableActionList>
                <Create><Columns><Name>column2</Name></Columns></Create></AlterTableActionList></EVENT_INSTANCE>";
                DDLEvent dde = new DDLEvent(100, xml);
                SchemaChange expected = new SchemaChange(100, SchemaChangeType.Add, "dbo", "test2", "column2", null, new DataType("datetime", null, null, null));
                List<SchemaChange> result = dde.Parse(tables, (IDataUtils)dataUtils, TServer.MASTER, "testdb");                
                Assert.True(result[0].Equals(expected));     
            }

            [Fact]
            public void TestParse_AddColumn_WithTableNotInConfig() {
                //schema change on a table not in the config. it should be ignored.
                String xml = @"<EVENT_INSTANCE><EventType>ALTER_TABLE</EventType><SchemaName>dbo</SchemaName>
                <ObjectName>testnotinconfig</ObjectName><ObjectType>TABLE</ObjectType><AlterTableActionList>
                <Create><Columns><Name>column1</Name></Columns></Create></AlterTableActionList></EVENT_INSTANCE>";
                DDLEvent dde = new DDLEvent(100, xml);
                List<SchemaChange> result = dde.Parse(tables, (IDataUtils)dataUtils, TServer.MASTER, "testdb");
                Assert.Equal(0, result.Count);  
            }

            [Fact]
            public void TestParse_AddColumn_WithDefault() {
                //adding in a constraint, we should still publish the new column but not the constraint
                String xml = @"<EVENT_INSTANCE><EventType>ALTER_TABLE</EventType>
                <SchemaName>dbo</SchemaName><ObjectName>test1</ObjectName><ObjectType>TABLE</ObjectType>
                <AlterTableActionList><Create><Columns><Name>column1</Name></Columns>
                <Constraints><Name>DF_test1_column3</Name></Constraints></Create></AlterTableActionList></EVENT_INSTANCE>";
                DDLEvent dde = new DDLEvent(100, xml);
                SchemaChange expected = new SchemaChange(100, SchemaChangeType.Add, "dbo", "test1", "column1", null, new DataType("int", null, (byte)10, 0));
                List<SchemaChange> result = dde.Parse(tables, (IDataUtils)dataUtils, TServer.MASTER, "testdb");                
                Assert.True(result[0].Equals(expected));
            }

            [Fact]
            public void TestParse_EnableChangeTracking_ShouldNotPublish() {
                //any action that doesn't operate on columns, such as enabling change tracking, shouldn't be published
                String xml = @"<EVENT_INSTANCE><EventType>ALTER_TABLE</EventType>
                <SchemaName>dbo</SchemaName><ObjectName>test1</ObjectName><ObjectType>TABLE</ObjectType>
                <TSQLCommand><CommandText>alter table test1 enable change_tracking</CommandText></TSQLCommand></EVENT_INSTANCE>";
                DDLEvent dde = new DDLEvent(100, xml);
                List<SchemaChange> result = dde.Parse(tables, (IDataUtils)dataUtils, TServer.MASTER, "testdb");
                Assert.Equal(0, result.Count);
            }

            [Fact]
            public void TestParse_AddConstraint_ShouldNotPublish() {
                //this change just adds a constraint to an existing column, so it should be ignored
                String xml = @"<EVENT_INSTANCE><EventType>ALTER_TABLE</EventType>
                <SchemaName>dbo</SchemaName><ObjectName>test1</ObjectName><ObjectType>TABLE</ObjectType><AlterTableActionList>
                <Create><Constraints><Name>DF_test1_column1</Name></Constraints></Create></AlterTableActionList></EVENT_INSTANCE>";
                DDLEvent dde = new DDLEvent(100, xml);
                List<SchemaChange> result = dde.Parse(tables, (IDataUtils)dataUtils, TServer.MASTER, "testdb");
                Assert.Equal(0, result.Count);
            }

            [Fact]
            public void TestParse_RenameColumn_WithoutColumnList() {
                //renaming a column, without a column list this should be published
                String xml = @"<EVENT_INSTANCE><EventType>RENAME</EventType><SchemaName>dbo</SchemaName>
                <ObjectName>column2</ObjectName><ObjectType>COLUMN</ObjectType><TargetObjectName>test1</TargetObjectName>
                <TargetObjectType>TABLE</TargetObjectType><NewObjectName>col2</NewObjectName>
                <Parameters><Param>test1.column2</Param><Param>col2</Param><Param>COLUMN</Param></Parameters></EVENT_INSTANCE>";
                DDLEvent dde = new DDLEvent(100, xml);
                SchemaChange expected = new SchemaChange(100, SchemaChangeType.Rename, "dbo", "test1", "column2", "col2", null);
                List<SchemaChange> result = dde.Parse(tables, (IDataUtils)dataUtils, TServer.MASTER, "testdb");
                Assert.True(result[0].Equals(expected));
            }

            [Fact]
            public void TestParse_RenameColumn_WithColumnInList() {
                //renaming a column when a column list is present in config. if the column is in the list, it should be published.
                String xml = @"<EVENT_INSTANCE><EventType>RENAME</EventType><SchemaName>dbo</SchemaName>
                <ObjectName>column2</ObjectName><ObjectType>COLUMN</ObjectType><TargetObjectName>test2</TargetObjectName>
                <TargetObjectType>TABLE</TargetObjectType><NewObjectName>col2</NewObjectName>
                <Parameters><Param>test2.column2</Param><Param>col2</Param><Param>COLUMN</Param></Parameters></EVENT_INSTANCE>";
                DDLEvent dde = new DDLEvent(100, xml);
                SchemaChange expected = new SchemaChange(100, SchemaChangeType.Rename, "dbo", "test2", "column2", "col2", null);
                List<SchemaChange> result = dde.Parse(tables, (IDataUtils)dataUtils, TServer.MASTER, "testdb");
                Assert.True(result[0].Equals(expected));
            }

            [Fact]
            public void TestParse_RenameColumn_WithColumnNotInList() {
                //renaming a column when a column list is present in config. if the column isn't the list, it shouldn't be published.
                String xml = @"<EVENT_INSTANCE><EventType>RENAME</EventType><SchemaName>dbo</SchemaName>
                <ObjectName>col2</ObjectName><ObjectType>COLUMN</ObjectType><TargetObjectName>test2</TargetObjectName>
                <TargetObjectType>TABLE</TargetObjectType><NewObjectName>col3</NewObjectName>
                <Parameters><Param>test1.col2</Param><Param>col3</Param><Param>COLUMN</Param></Parameters></EVENT_INSTANCE>";
                DDLEvent dde = new DDLEvent(100, xml);
                List<SchemaChange> result = dde.Parse(tables, (IDataUtils)dataUtils, TServer.MASTER, "testdb");
                Assert.Equal(0, result.Count);
            }

            [Fact]
            public void TestParse_RenameTable() {    
                //don't publish rename table events. 
                String xml = @"<EVENT_INSTANCE><EventType>RENAME</EventType>
                <SchemaName>dbo</SchemaName><ObjectName>test1</ObjectName><ObjectType>TABLE</ObjectType>
                <TargetObjectName /><TargetObjectType /><NewObjectName>test1_renamed</NewObjectName>
                <Parameters><Param>test1</Param><Param>test1_renamed</Param><Param /></Parameters></EVENT_INSTANCE>";
                DDLEvent dde = new DDLEvent(100, xml);
                List<SchemaChange> result = dde.Parse(tables, (IDataUtils)dataUtils, TServer.MASTER, "testdb");
                Assert.Equal(0, result.Count);
            }
            

            [Fact]
            public void TestParse_ModifyColumn_WithoutColumnList() {
                //changing a column's data type should be published
                String xml = @"<EVENT_INSTANCE><EventType>ALTER_TABLE</EventType>
                <SchemaName>dbo</SchemaName><ObjectName>test1</ObjectName><ObjectType>TABLE</ObjectType>
                <AlterTableActionList><Alter><Columns><Name>column1</Name></Columns></Alter></AlterTableActionList></EVENT_INSTANCE>";
                DDLEvent dde = new DDLEvent(100, xml);
                SchemaChange expected = new SchemaChange(100, SchemaChangeType.Modify, "dbo", "test1", "column1", null, new DataType("int", null, (byte)10, 0));
                List<SchemaChange> result = dde.Parse(tables, (IDataUtils)dataUtils, TServer.MASTER, "testdb");
                Assert.True(result[0].Equals(expected));
            }

            [Fact]
            public void TestParse_ModifyColumn_CaseSensitivity() {
                //same as previous test but with casing on the table name not matching the casing in the dataset.
                //doing this to make sure the case ignoring code works.
                String xml = @"<EVENT_INSTANCE><EventType>ALTER_TABLE</EventType>
                <SchemaName>dbo</SchemaName><ObjectName>TEST1</ObjectName><ObjectType>TABLE</ObjectType>
                <AlterTableActionList><Alter><Columns><Name>COLUMN1</Name></Columns></Alter></AlterTableActionList></EVENT_INSTANCE>";
                DDLEvent dde = new DDLEvent(100, xml);
                SchemaChange expected = new SchemaChange(100, SchemaChangeType.Modify, "dbo", "TEST1", "COLUMN1", null, new DataType("int", null, (byte)10, 0));
                List<SchemaChange> result = dde.Parse(tables, (IDataUtils)dataUtils, TServer.MASTER, "testdb");
                Assert.True(result[0].Equals(expected));
            }

            [Fact]
            public void TestParse_ModifyColumn_WithColumnInList() {
                //changing a column's data type with a column list, with the column in the list should be published
                String xml = @"<EVENT_INSTANCE><EventType>ALTER_TABLE</EventType>
                <SchemaName>dbo</SchemaName><ObjectName>test2</ObjectName><ObjectType>TABLE</ObjectType>
                <AlterTableActionList><Alter><Columns><Name>column2</Name></Columns></Alter></AlterTableActionList></EVENT_INSTANCE>";
                DDLEvent dde = new DDLEvent(100, xml);
                SchemaChange expected = new SchemaChange(100, SchemaChangeType.Modify, "dbo", "test2", "column2", null, new DataType("datetime", null, null, null));
                List<SchemaChange> result = dde.Parse(tables, (IDataUtils)dataUtils, TServer.MASTER, "testdb");
                Assert.True(result[0].Equals(expected));
            }

            [Fact]
            public void TestParse_ModifyColumn_WithColumnNotInList() {
                //changing a column's data type with a column list, with the column not in the list shouldn't be published
                String xml = @"<EVENT_INSTANCE><EventType>ALTER_TABLE</EventType>
                <SchemaName>dbo</SchemaName><ObjectName>test2</ObjectName><ObjectType>TABLE</ObjectType>
                <AlterTableActionList><Alter><Columns><Name>col2</Name></Columns></Alter></AlterTableActionList></EVENT_INSTANCE>";
                DDLEvent dde = new DDLEvent(100, xml);
                List<SchemaChange> result = dde.Parse(tables, (IDataUtils)dataUtils, TServer.MASTER, "testdb");
                Assert.Equal(0, result.Count);
            }

            [Fact]
            public void TestParse_DropColumn_WithoutColumnList() {
                //dropping a column should be published
                String xml = @"<EVENT_INSTANCE><EventType>ALTER_TABLE</EventType>
                <SchemaName>dbo</SchemaName><ObjectName>test1</ObjectName><ObjectType>TABLE</ObjectType>
                <AlterTableActionList><Drop><Columns><Name>column1</Name></Columns></Drop></AlterTableActionList></EVENT_INSTANCE>";
                DDLEvent dde = new DDLEvent(100, xml);
                SchemaChange expected = new SchemaChange(100, SchemaChangeType.Drop, "dbo", "test1", "column1", null, null);
                List<SchemaChange> result = dde.Parse(tables, (IDataUtils)dataUtils, TServer.MASTER, "testdb");
                Assert.True(result[0].Equals(expected));
            }

            [Fact]
            public void TestParse_DropColumn_WithColumnInList() {
                //dropping a column with a column list, with the column in the list should be published
                String xml = @"<EVENT_INSTANCE><EventType>ALTER_TABLE</EventType>
                <SchemaName>dbo</SchemaName><ObjectName>test2</ObjectName><ObjectType>TABLE</ObjectType>
                <AlterTableActionList><Drop><Columns><Name>column1</Name></Columns></Drop></AlterTableActionList></EVENT_INSTANCE>";
                DDLEvent dde = new DDLEvent(100, xml);
                SchemaChange expected = new SchemaChange(100, SchemaChangeType.Drop, "dbo", "test2", "column1", null, null);
                List<SchemaChange> result = dde.Parse(tables, (IDataUtils)dataUtils, TServer.MASTER, "testdb");
                Assert.True(result[0].Equals(expected));
            }

            [Fact]
            public void TestParse_DropColumn_WithColumnNotInList() {
                //dropping a column with a column list, with the column not in the list shouldn't be published
                String xml = @"<EVENT_INSTANCE><EventType>ALTER_TABLE</EventType>
                <SchemaName>dbo</SchemaName><ObjectName>test2</ObjectName><ObjectType>TABLE</ObjectType>
                <AlterTableActionList><Drop><Columns><Name>col2</Name></Columns></Drop></AlterTableActionList></EVENT_INSTANCE>";
                DDLEvent dde = new DDLEvent(100, xml);
                List<SchemaChange> result = dde.Parse(tables, (IDataUtils)dataUtils, TServer.MASTER, "testdb");
                Assert.Equal(0, result.Count);
            }
        }
       
        /// <summary>
        /// This class is invoked by xunit.net and passed to SetFixture in the DDLEventTest class
        /// </summary>
        public class DDLEventTestData {
            public DataSet testData;
            public TableConf[] tables;
            public TestDataUtils dataUtils;

            public DDLEventTestData() {
                tables = new TableConf[2];
                //first table has no column list
                tables[0] = new TableConf();
                tables[0].Name = "test1";

                //second one has column list
                tables[1] = new TableConf();
                tables[1].Name = "test2";
                tables[1].columnList = new string[2] { "column1", "column2" };

                dataUtils = new TestDataUtils();
                testData = new DataSet();
                var dt = new DataTable("INFORMATION_SCHEMA.COLUMNS", dataUtils.GetTableSpace(TServer.MASTER, "testdb"));
                dt.Columns.Add("TABLE_SCHEMA", typeof(string));
                dt.Columns.Add("TABLE_CATALOG", typeof(string));
                dt.Columns.Add("TABLE_NAME", typeof(string));
                dt.Columns.Add("COLUMN_NAME", typeof(string));
                dt.Columns.Add("DATA_TYPE", typeof(string));
                dt.Columns.Add("CHARACTER_MAXIMUM_LENGTH", typeof(int));
                dt.Columns.Add("NUMERIC_PRECISION", typeof(byte));
                dt.Columns.Add("NUMERIC_SCALE", typeof(int));

                DataRow row = dt.NewRow();
                row["TABLE_SCHEMA"] = "dbo";
                row["TABLE_CATALOG"] = "testdb";
                row["TABLE_NAME"] = "test1";
                row["COLUMN_NAME"] = "column1";
                row["DATA_TYPE"] = "int";
                row["CHARACTER_MAXIMUM_LENGTH"] = DBNull.Value;
                row["NUMERIC_PRECISION"] = (byte)10;
                row["NUMERIC_SCALE"] = 0;
                dt.Rows.Add(row);

                row = dt.NewRow();
                row["TABLE_SCHEMA"] = "dbo";
                row["TABLE_CATALOG"] = "testdb";
                row["TABLE_NAME"] = "test1";
                row["COLUMN_NAME"] = "column2";
                row["DATA_TYPE"] = "varchar";
                row["CHARACTER_MAXIMUM_LENGTH"] = 100;
                row["NUMERIC_PRECISION"] = DBNull.Value;
                row["NUMERIC_SCALE"] = DBNull.Value;
                dt.Rows.Add(row);

                row = dt.NewRow();
                row["TABLE_SCHEMA"] = "dbo";
                row["TABLE_CATALOG"] = "testdb";
                row["TABLE_NAME"] = "test1";
                row["COLUMN_NAME"] = "column1";
                row["DATA_TYPE"] = "nchar";
                row["CHARACTER_MAXIMUM_LENGTH"] = 8;
                row["NUMERIC_PRECISION"] = DBNull.Value;
                row["NUMERIC_SCALE"] = DBNull.Value;
                dt.Rows.Add(row);

                row = dt.NewRow();
                row["TABLE_SCHEMA"] = "dbo";
                row["TABLE_CATALOG"] = "testdb";
                row["TABLE_NAME"] = "test2";
                row["COLUMN_NAME"] = "column2";
                row["DATA_TYPE"] = "datetime";
                row["CHARACTER_MAXIMUM_LENGTH"] = DBNull.Value;
                row["NUMERIC_PRECISION"] = DBNull.Value;
                row["NUMERIC_SCALE"] = DBNull.Value;
                dt.Rows.Add(row);

                //add the datatable to the data set
                testData.Tables.Add(dt);
                //add the dataset to the TestDataUtils instance
                dataUtils.testData = testData;
            }
        }
        #endregion
    }
}
