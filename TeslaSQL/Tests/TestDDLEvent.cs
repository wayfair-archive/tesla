using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Xunit;
using TeslaSQL.DataUtils;
using System.Data;

namespace TeslaSQL.Tests {
    public class TestDDLEvent  {

        public TestDDLEvent() {
        }

        ///very thorough unit tests below since the above code is easy to get wrong for edge cases if one is not careful
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
                List<SchemaChange> result = dde.Parse(tables, (IDataUtils)dataUtils, "testdb");
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
                List<SchemaChange> result = dde.Parse(tables, (IDataUtils)dataUtils, "testdb");
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
                List<SchemaChange> result = dde.Parse(tables, (IDataUtils)dataUtils, "testdb");
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
                List<SchemaChange> result = dde.Parse(tables, (IDataUtils)dataUtils, "testdb");                
                Assert.True(result[0].Equals(expected));     
            }

            [Fact]
            public void TestParse_AddColumn_WithTableNotInConfig() {
                //schema change on a table not in the Config. it should be ignored.
                String xml = @"<EVENT_INSTANCE><EventType>ALTER_TABLE</EventType><SchemaName>dbo</SchemaName>
                <ObjectName>testnotinconfig</ObjectName><ObjectType>TABLE</ObjectType><AlterTableActionList>
                <Create><Columns><Name>column1</Name></Columns></Create></AlterTableActionList></EVENT_INSTANCE>";
                DDLEvent dde = new DDLEvent(100, xml);
                List<SchemaChange> result = dde.Parse(tables, (IDataUtils)dataUtils, "testdb");
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
                List<SchemaChange> result = dde.Parse(tables, (IDataUtils)dataUtils, "testdb");                
                Assert.True(result[0].Equals(expected));
            }

            [Fact]
            public void TestParse_EnableChangeTracking_ShouldNotPublish() {
                //any action that doesn't operate on columns, such as enabling change tracking, shouldn't be published
                String xml = @"<EVENT_INSTANCE><EventType>ALTER_TABLE</EventType>
                <SchemaName>dbo</SchemaName><ObjectName>test1</ObjectName><ObjectType>TABLE</ObjectType>
                <TSQLCommand><CommandText>alter table test1 enable change_tracking</CommandText></TSQLCommand></EVENT_INSTANCE>";
                DDLEvent dde = new DDLEvent(100, xml);
                List<SchemaChange> result = dde.Parse(tables, (IDataUtils)dataUtils, "testdb");
                Assert.Equal(0, result.Count);
            }

            [Fact]
            public void TestParse_AddConstraint_ShouldNotPublish() {
                //this change just adds a constraint to an existing column, so it should be ignored
                String xml = @"<EVENT_INSTANCE><EventType>ALTER_TABLE</EventType>
                <SchemaName>dbo</SchemaName><ObjectName>test1</ObjectName><ObjectType>TABLE</ObjectType><AlterTableActionList>
                <Create><Constraints><Name>DF_test1_column1</Name></Constraints></Create></AlterTableActionList></EVENT_INSTANCE>";
                DDLEvent dde = new DDLEvent(100, xml);
                List<SchemaChange> result = dde.Parse(tables, (IDataUtils)dataUtils, "testdb");
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
                List<SchemaChange> result = dde.Parse(tables, (IDataUtils)dataUtils, "testdb");
                Assert.True(result[0].Equals(expected));
            }

            [Fact]
            public void TestParse_RenameColumn_WithColumnInList() {
                //renaming a column when a column list is present in Config. if the column is in the list, it should be published.
                String xml = @"<EVENT_INSTANCE><EventType>RENAME</EventType><SchemaName>dbo</SchemaName>
                <ObjectName>column2</ObjectName><ObjectType>COLUMN</ObjectType><TargetObjectName>test2</TargetObjectName>
                <TargetObjectType>TABLE</TargetObjectType><NewObjectName>col2</NewObjectName>
                <Parameters><Param>test2.column2</Param><Param>col2</Param><Param>COLUMN</Param></Parameters></EVENT_INSTANCE>";
                DDLEvent dde = new DDLEvent(100, xml);
                SchemaChange expected = new SchemaChange(100, SchemaChangeType.Rename, "dbo", "test2", "column2", "col2", null);
                List<SchemaChange> result = dde.Parse(tables, (IDataUtils)dataUtils, "testdb");
                Assert.True(result[0].Equals(expected));
            }

            [Fact]
            public void TestParse_RenameColumn_WithColumnNotInList() {
                //renaming a column when a column list is present in Config. if the column isn't the list, it shouldn't be published.
                String xml = @"<EVENT_INSTANCE><EventType>RENAME</EventType><SchemaName>dbo</SchemaName>
                <ObjectName>col2</ObjectName><ObjectType>COLUMN</ObjectType><TargetObjectName>test2</TargetObjectName>
                <TargetObjectType>TABLE</TargetObjectType><NewObjectName>col3</NewObjectName>
                <Parameters><Param>test1.col2</Param><Param>col3</Param><Param>COLUMN</Param></Parameters></EVENT_INSTANCE>";
                DDLEvent dde = new DDLEvent(100, xml);
                List<SchemaChange> result = dde.Parse(tables, (IDataUtils)dataUtils, "testdb");
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
                List<SchemaChange> result = dde.Parse(tables, (IDataUtils)dataUtils, "testdb");
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
                List<SchemaChange> result = dde.Parse(tables, (IDataUtils)dataUtils, "testdb");
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
                List<SchemaChange> result = dde.Parse(tables, (IDataUtils)dataUtils, "testdb");
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
                List<SchemaChange> result = dde.Parse(tables, (IDataUtils)dataUtils, "testdb");
                Assert.True(result[0].Equals(expected));
            }

            [Fact]
            public void TestParse_ModifyColumn_WithColumnNotInList() {
                //changing a column's data type with a column list, with the column not in the list shouldn't be published
                String xml = @"<EVENT_INSTANCE><EventType>ALTER_TABLE</EventType>
                <SchemaName>dbo</SchemaName><ObjectName>test2</ObjectName><ObjectType>TABLE</ObjectType>
                <AlterTableActionList><Alter><Columns><Name>col2</Name></Columns></Alter></AlterTableActionList></EVENT_INSTANCE>";
                DDLEvent dde = new DDLEvent(100, xml);
                List<SchemaChange> result = dde.Parse(tables, (IDataUtils)dataUtils, "testdb");
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
                List<SchemaChange> result = dde.Parse(tables, (IDataUtils)dataUtils, "testdb");
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
                List<SchemaChange> result = dde.Parse(tables, (IDataUtils)dataUtils, "testdb");
                Assert.True(result[0].Equals(expected));
            }

            [Fact]
            public void TestParse_DropColumn_WithColumnNotInList() {
                //dropping a column with a column list, with the column not in the list shouldn't be published
                String xml = @"<EVENT_INSTANCE><EventType>ALTER_TABLE</EventType>
                <SchemaName>dbo</SchemaName><ObjectName>test2</ObjectName><ObjectType>TABLE</ObjectType>
                <AlterTableActionList><Drop><Columns><Name>col2</Name></Columns></Drop></AlterTableActionList></EVENT_INSTANCE>";
                DDLEvent dde = new DDLEvent(100, xml);
                List<SchemaChange> result = dde.Parse(tables, (IDataUtils)dataUtils, "testdb");
                Assert.Equal(0, result.Count);
            }
        }
       
        /// <summary>
        /// This class is invoked by xunit.net and passed to SetFixture in the DDLEventTest class
        /// </summary>
        public class DDLEventTestData {
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

                dataUtils = new TestDataUtils(TServer.MASTER);                
                dataUtils.ReloadData("test1");
            }
        }
    }
}
