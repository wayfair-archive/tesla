using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Xunit;
using System.Data;
using TeslaSQL.DataUtils;
using TeslaSQL.DataCopy;
using TeslaSQL.Agents;
using Moq;
namespace TeslaSQL.Tests.Agents {
    public class TestSlave : Slave{
        /// <summary>
        /// Subclass so that we can implement the IUseFixture feature
        /// </summary>
        public class TestApplySchemaChanges : IUseFixture<ApplySchemaChangeTestData> {
            public TableConf[] tables;
            public TestDataUtils sourceDataUtils;
            public TestDataUtils destDataUtils;
            public Slave slave;

            /// <summary>
            /// xunit will automatically create an instance of DDLEventTestData and pass it to this method before running tests
            /// </summary>
            public void SetFixture(ApplySchemaChangeTestData data) {
                this.tables = data.tables;
                this.sourceDataUtils = data.sourceDataUtils;
                this.destDataUtils = data.destDataUtils;
                this.slave = data.slave;
            }

            [Fact]
            public void TestApply_AddSingleColumn_WithoutColumnList() {
                //add a column and make sure it is published
                DataTable dt = sourceDataUtils.testData.Tables["dbo.tblCTSchemaChange_1", "RELAY.CT_testdb"];

                dt.Clear();
                DataRow row = dt.NewRow();
                row["CscDdeID"] = 1;
                row["CscTableName"] = "test1";
                row["CscEventType"] = "Add";
                row["CscSchema"] = "dbo";
                row["CscColumnName"] = "testadd";
                row["CscNewColumnName"] = DBNull.Value;
                row["CscBaseDataType"] = "int";
                row["CscCharacterMaximumLength"] = DBNull.Value;
                row["CscNumericPrecision"] = DBNull.Value;
                row["CscNumericScale"] = DBNull.Value;
                dt.Rows.Add(row);

                destDataUtils.DropTableIfExists("testdb", "test1", "dbo");
                DataTable test1 = new DataTable("dbo.test1", "SLAVE.testdb");
                test1.Columns.Add("column1", typeof(Int32));
                test1.Columns.Add("column2", typeof(Int32));
                destDataUtils.testData.Tables.Add(test1);

                slave.ApplySchemaChanges(tables, "testdb", "testdb", 1);

                var expected = new DataColumn("testadd", typeof(Int32));
                var actual = destDataUtils.testData.Tables["dbo.test1", "SLAVE.testdb"].Columns["testadd"];
                //assert.equal doesn't work for objects but this does
                Assert.True(expected.ColumnName == actual.ColumnName && expected.DataType == actual.DataType);
            }

            [Fact]
            public void TestApply_AddSingleColumn_WithColumnInList() {
                //add a column and make sure it is published
                DataTable dt = sourceDataUtils.testData.Tables["dbo.tblCTSchemaChange_1", "RELAY.CT_testdb"];

                dt.Clear();
                DataRow row = dt.NewRow();
                row["CscDdeID"] = 1;
                row["CscTableName"] = "test2";
                row["CscEventType"] = "Add";
                row["CscSchema"] = "dbo";
                row["CscColumnName"] = "column2";
                row["CscNewColumnName"] = DBNull.Value;
                row["CscBaseDataType"] = "int";
                row["CscCharacterMaximumLength"] = DBNull.Value;
                row["CscNumericPrecision"] = DBNull.Value;
                row["CscNumericScale"] = DBNull.Value;
                dt.Rows.Add(row);

                destDataUtils.DropTableIfExists("testdb", "test2", "dbo");
                DataTable test2 = new DataTable("dbo.test2", "SLAVE.testdb");
                test2.Columns.Add("column1", typeof(Int32));
                test2.Columns.Add("column3", typeof(string));
                destDataUtils.testData.Tables.Add(test2);

                slave.ApplySchemaChanges(tables, "testdb", "testdb", 1);

                var expected = new DataColumn("column2", typeof(Int32));
                var actual = destDataUtils.testData.Tables["dbo.test2", "SLAVE.testdb"].Columns["column2"];
                Assert.True(expected.ColumnName == actual.ColumnName && expected.DataType == actual.DataType);
            }

            [Fact]
            public void TestApply_AddSingleColumn_WithColumnNotInList() {
                //add a column and make sure it is published
                DataTable dt = sourceDataUtils.testData.Tables["dbo.tblCTSchemaChange_1", "RELAY.CT_testdb"];
                //gets rid of all the rows
                dt.Clear();
                //create a row                
                DataRow row = dt.NewRow();
                row["CscDdeID"] = 1;
                row["CscTableName"] = "test2";
                row["CscEventType"] = "Add";
                row["CscSchema"] = "dbo";
                row["CscColumnName"] = "testadd";
                row["CscNewColumnName"] = DBNull.Value;
                row["CscBaseDataType"] = "int";
                row["CscCharacterMaximumLength"] = DBNull.Value;
                row["CscNumericPrecision"] = DBNull.Value;
                row["CscNumericScale"] = DBNull.Value;
                dt.Rows.Add(row);

                destDataUtils.DropTableIfExists("testdb", "test2", "dbo");
                DataTable test2 = new DataTable("dbo.test2", "SLAVE.testdb");
                test2.Columns.Add("column1", typeof(Int32));
                test2.Columns.Add("column3", typeof(string));
                destDataUtils.testData.Tables.Add(test2);

                slave.ApplySchemaChanges(tables, "testdb", "testdb", 1);

                Assert.False(destDataUtils.testData.Tables["dbo.test2", "SLAVE.testdb"].Columns.Contains("testadd"));
            }

            [Fact]
            public void TestApply_RenameColumn() {
                //add a column and make sure it is published
                DataTable dt = sourceDataUtils.testData.Tables["dbo.tblCTSchemaChange_1", "RELAY.CT_testdb"];
                //gets rid of all the rows
                dt.Clear();
                //create a row                
                DataRow row = dt.NewRow();
                row["CscDdeID"] = 1;
                row["CscTableName"] = "test1";
                row["CscEventType"] = "Rename";
                row["CscSchema"] = "dbo";
                row["CscColumnName"] = "column2";
                row["CscNewColumnName"] = "column2_new";
                row["CscBaseDataType"] = DBNull.Value;
                row["CscCharacterMaximumLength"] = DBNull.Value;
                row["CscNumericPrecision"] = DBNull.Value;
                row["CscNumericScale"] = DBNull.Value;
                dt.Rows.Add(row);

                destDataUtils.DropTableIfExists("testdb", "test1", "dbo");
                DataTable test1 = new DataTable("dbo.test1", "SLAVE.testdb");
                test1.Columns.Add("column1", typeof(Int32));
                test1.Columns.Add("column2", typeof(Int32));
                destDataUtils.testData.Tables.Add(test1);

                slave.ApplySchemaChanges(tables, "testdb", "testdb", 1);

                Assert.True(destDataUtils.testData.Tables["dbo.test1", "SLAVE.testdb"].Columns.Contains("column2_new"));
                destDataUtils.testData.Tables["dbo.test1", "SLAVE.testdb"].RejectChanges();
            }

            [Fact]
            public void TestApply_DropColumn() {
                //add a column and make sure it is published
                DataTable dt = sourceDataUtils.testData.Tables["dbo.tblCTSchemaChange_1", "RELAY.CT_testdb"];
                //gets rid of all the rows
                dt.Clear();
                //create a row                
                DataRow row = dt.NewRow();
                row["CscDdeID"] = 1;
                row["CscTableName"] = "test1";
                row["CscEventType"] = "Drop";
                row["CscSchema"] = "dbo";
                row["CscColumnName"] = "column2";
                row["CscNewColumnName"] = DBNull.Value;
                row["CscBaseDataType"] = DBNull.Value;
                row["CscCharacterMaximumLength"] = DBNull.Value;
                row["CscNumericPrecision"] = DBNull.Value;
                row["CscNumericScale"] = DBNull.Value;
                dt.Rows.Add(row);

                destDataUtils.DropTableIfExists("testdb", "test1", "dbo");
                DataTable test1 = new DataTable("dbo.test1", "SLAVE.testdb");
                test1.Columns.Add("column1", typeof(Int32));
                test1.Columns.Add("column2", typeof(Int32));
                destDataUtils.testData.Tables.Add(test1);

                //if this assert fails it means the test setup got borked
                Assert.True(destDataUtils.testData.Tables["dbo.test1", "SLAVE.testdb"].Columns.Contains("column2"));
                slave.ApplySchemaChanges(tables, "testdb", "testdb", 1);
                Assert.False(destDataUtils.testData.Tables["dbo.test1", "SLAVE.testdb"].Columns.Contains("column2"));
            }

            [Fact]
            public void TestApply_ModifyColumn() {
                //add a column and make sure it is published
                DataTable dt = sourceDataUtils.testData.Tables["dbo.tblCTSchemaChange_1", "RELAY.CT_testdb"];
                //gets rid of all the rows
                dt.Clear();
                //create a row                
                DataRow row = dt.NewRow();
                row["CscDdeID"] = 1;
                row["CscTableName"] = "test1";
                row["CscEventType"] = "Modify";
                row["CscSchema"] = "dbo";
                row["CscColumnName"] = "column2";
                row["CscNewColumnName"] = DBNull.Value;
                row["CscBaseDataType"] = "datetime";
                row["CscCharacterMaximumLength"] = DBNull.Value;
                row["CscNumericPrecision"] = DBNull.Value;
                row["CscNumericScale"] = DBNull.Value;
                dt.Rows.Add(row);

                destDataUtils.DropTableIfExists("testdb", "test1", "dbo");
                DataTable test1 = new DataTable("dbo.test1", "SLAVE.testdb");
                test1.Columns.Add("column1", typeof(Int32));
                test1.Columns.Add("column2", typeof(Int32));
                destDataUtils.testData.Tables.Add(test1);

                //if this assert fails it means the test setup got borked
                var expected = new DataColumn("column2", typeof(DateTime));

                slave.ApplySchemaChanges(tables, "testdb", "testdb", 1);
                var actual = destDataUtils.testData.Tables["dbo.test1", "SLAVE.testdb"].Columns["column2"];

                Assert.True(expected.ColumnName == actual.ColumnName && expected.DataType == actual.DataType);
            }

        }



        /// <summary>
        /// This class is instantiated by xunit.net and passed to SetFixture in the TestApplySchemaChanges class
        /// </summary>
        public class ApplySchemaChangeTestData {
            public DataSet testData;
            public TableConf[] tables;
            public TestDataUtils sourceDataUtils;
            public TestDataUtils destDataUtils;
            public Slave slave;

            public ApplySchemaChangeTestData() {
                tables = new TableConf[2];
                //first table has no column list
                tables[0] = new TableConf();
                tables[0].Name = "test1";

                //second one has column list
                tables[1] = new TableConf();
                tables[1].Name = "test2";
                tables[1].columnList = new string[2] { "column1", "column2" };

                sourceDataUtils = new TestDataUtils(TServer.RELAY);
                destDataUtils = new TestDataUtils(TServer.SLAVE);

                testData = new DataSet();
                sourceDataUtils.testData = new DataSet();
                destDataUtils.testData = new DataSet();
                //this method, conveniently, sets up the datatable schema we need
                sourceDataUtils.CreateSchemaChangeTable("CT_testdb", 1);

                var config = new Config();
                config.tables = tables;
                config.relayDB = "CT_testdb";
                config.logLevel = LogLevel.Critical;
                slave = new Slave(config, sourceDataUtils, destDataUtils, null);
            }
        }

        struct MagicHourTest {
            public TimeSpan[] magicHours;
            public DateTime lastRun;
            public DateTime now;
            public bool isFullRunTime;
            public MagicHourTest(TimeSpan[] magicHours, DateTime lastRun, DateTime now, bool pass) {
                this.magicHours = magicHours;
                this.lastRun = lastRun;
                this.now = now;
                this.isFullRunTime = pass;
            }
        }

        [Fact]
        public void TestMagicHour() {
            var now = DateTime.Now;
            var testCases = new List<MagicHourTest>();
            testCases.Add(
                new MagicHourTest(
                    new TimeSpan[] { new TimeSpan(3, 0, 0) },
                    new DateTime(now.Year, now.Month, now.Day, 3, 1, 0),
                    new DateTime(now.Year, now.Month, now.Day, 3, 2, 0),
                    false
                    ));
            testCases.Add(
                new MagicHourTest(
                    new TimeSpan[] { new TimeSpan(3, 0, 0) },
                    new DateTime(now.Year, now.Month, now.Day - 1, 3, 2, 0),
                    new DateTime(now.Year, now.Month, now.Day, 3, 1, 0),
                    true
                    ));
            var cfg = new Mock<Config>();
            
            foreach (var test in testCases) {
                cfg.Setup(c => c.magicHours).Returns(test.magicHours);
                config = cfg.Object;
                var mockDataUtils = new Mock<IDataUtils>();
                mockDataUtils.Setup(du => du.GetLastStartTime(It.IsAny<string>(), It.IsAny<long>(), It.IsAny<int>(), It.IsAny<AgentType>()))
                    .Returns(test.lastRun);
                sourceDataUtils = mockDataUtils.Object;
                Assert.Equal(FullRunTime(test.now), test.isFullRunTime);
            }
        }
    }
}
