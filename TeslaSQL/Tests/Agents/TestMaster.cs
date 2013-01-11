using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Xml;
using System.Data.SqlTypes;
using Xunit;
using System.Diagnostics;
using TeslaSQL.DataUtils;
using TeslaSQL.DataCopy;
using TeslaSQL.Agents;

namespace TeslaSQL.Tests.Agents {
    public class TestMaster : Master, IUseFixture<MasterTestFixture> {

        Dictionary<string, Int64> changesCaptured = new Dictionary<string, Int64> {
                {"dbo.test1", 1},
                {"dbo.test2", 0}
            };

        public void SetFixture(MasterTestFixture fixture) {
            this.sourceDataUtils = fixture.sourceDataUtils;
            this.destDataUtils = fixture.destDataUtils;
            ((TestDataUtils)sourceDataUtils).ReloadData("test1");
            ((TestDataUtils)destDataUtils).ReloadData("test1");
            SetFieldLists("testdb", Config.tables, sourceDataUtils);
        }

        [Fact]
        public void TestInitializeBatch_LastBatchSuccessful() {
            DataTable tblCTVersion = ((TestDataUtils)destDataUtils).testData.Tables["dbo.tblCTVersion", "RELAY.CT_testdb"];
            DataRow row = tblCTVersion.Rows[tblCTVersion.Rows.Count - 1];

            Int64 CTID = row.Field<Int64>("CTID");
            row["syncBitWise"] = SyncBitWise.UploadChanges;
            row["syncStartVersion"] = 1000;
            row["syncStopVersion"] = 2000;

            ChangeTrackingBatch expected = new ChangeTrackingBatch(CTID + 1, 2000, 2500, 0);
            ChangeTrackingBatch actual = InitializeBatch(2500);
            Assert.True(actual.Equals(expected));
            //undo any writes
            ((TestDataUtils)destDataUtils).ReloadData("test1");
        }

        [Fact]
        public void TestInitializeBatch_LastBatchFailed() {
            DataTable tblCTVersion = ((TestDataUtils)destDataUtils).testData.Tables["dbo.tblCTVersion", "RELAY.CT_testdb"];
            DataRow row = tblCTVersion.Rows[tblCTVersion.Rows.Count - 1];

            Int64 CTID = row.Field<Int64>("CTID");
            row["syncBitWise"] = 0;
            row["syncStartVersion"] = 1000;
            row["syncStopVersion"] = 2000;

            //in this case we expect syncStopVersion to get updated from 2000 to 2500 but the rest of the batch to stay the same
            ChangeTrackingBatch expected = new ChangeTrackingBatch(CTID, 1000, 2500, 0);
            ChangeTrackingBatch actual = InitializeBatch(2500);

            Assert.True(actual.Equals(expected));
            //undo any writes
            ((TestDataUtils)destDataUtils).ReloadData("test1");
        }

        [Fact]
        public void TestInitializeBatch_LastBatchPartial() {
            DataTable tblCTVersion = ((TestDataUtils)destDataUtils).testData.Tables["dbo.tblCTVersion", "RELAY.CT_testdb"];
            DataRow row = tblCTVersion.Rows[tblCTVersion.Rows.Count - 1];

            Int64 CTID = row.Field<Int64>("CTID");
            Int32 syncBitWise = Convert.ToInt32(SyncBitWise.CaptureChanges) + Convert.ToInt32(SyncBitWise.PublishSchemaChanges);
            row["syncBitWise"] = syncBitWise;
            row["syncStartVersion"] = 1000;
            row["syncStopVersion"] = 2000;

            //in this case we expect to just continue working on the same batch with no changes
            ChangeTrackingBatch expected = new ChangeTrackingBatch(CTID, 1000, 2000, syncBitWise);
            ChangeTrackingBatch actual = InitializeBatch(2500);

            Assert.True(actual.Equals(expected));
        }

        [Fact]
        public void TestPublishSchemaChanges() {
            //create tblCTSchemaChange_100
            destDataUtils.CreateSchemaChangeTable("CT_testdb", 101);
            //publish schema changes from tblDDLevent
            PublishSchemaChanges(Config.tables, "testdb", "CT_testdb", 101, new DateTime(2000, 1, 1));
            //retrieve results from tblCTSchemaChange_101
            DataTable results = destDataUtils.GetSchemaChanges("CT_testdb", 101);
            //parse schema change object for the resulting row
            SchemaChange actual = new SchemaChange(results.Rows[0]);
            //it should be equal to this
            SchemaChange expected = new SchemaChange(10, SchemaChangeType.Add, "dbo", "test1", "column2", null, new DataType("varchar", 100, null, null));

            Assert.True(actual.Equals(expected));
            //undo changes
            ((TestDataUtils)destDataUtils).ReloadData("test1");
        }

        [Fact]
        public void TestCreateChangeTables() {
            IDictionary<string, Int64> result = CreateChangeTables(Config.tables, "testdb", "CT_testdb", new ChangeTrackingBatch(101, 1000, 2000, 0));
            Assert.Equal(1, result["dbo.test1"]);
            Assert.Equal(0, result["dbo.test2"]);
        }

        [Fact]
        public void TestPublishChangeTables() {
            PublishChangeTables(Config.tables, "CT_testdb", "CT_testdb", 101, changesCaptured);
            DataRow actual = ((TestDataUtils)destDataUtils).testData.Tables["dbo.tblCTtest1_101", "RELAY.CT_testdb"].Rows[0];
            Assert.True(actual.Field<int>("column1") == 100
                && actual.Field<string>("column2") == "test"
                && actual.Field<DateTime>("RowCreationDate") == new DateTime(2012, 11, 1, 12, 0, 0)
                && actual.Field<string>("SYS_CHANGE_OPERATION") == "I"
                && actual.Field<Int64>("SYS_CHANGE_VERSION") == 1500
                );

            Assert.False(((TestDataUtils)destDataUtils).testData.Tables.Contains("dbo.tblCTtest2_101", "RELAY.CT_testdb"));
        }

        [Fact]
        public void TestPublishTableInfo() {
            //undo changes
            ((TestDataUtils)destDataUtils).ReloadData("test1");
            ctb = new ChangeTrackingBatch(101, 1000, 2000, 0);
            PublishTableInfo(Config.tables, "CT_testdb", changesCaptured, ctb.CTID);
            DataRow actual = ((TestDataUtils)destDataUtils).testData.Tables["dbo.tblCTTableInfo_101", "RELAY.CT_testdb"].Rows[0];
            Assert.True(actual.Field<string>("CtiTableName") == "test1"
                && actual.Field<string>("CtiSchemaName") == "dbo"
                && actual.Field<string>("CtiPKList") == "column1"
                && actual.Field<int>("CtiExpectedRows") == 1);

            actual = ((TestDataUtils)destDataUtils).testData.Tables["dbo.tblCTTableInfo_101", "RELAY.CT_testdb"].Rows[1];
            Assert.True(actual.Field<string>("CtiTableName") == "test2"
                && actual.Field<string>("CtiSchemaName") == "dbo"
                && actual.Field<string>("CtiPKList") == "column1"
                && actual.Field<int>("CtiExpectedRows") == 0);
        }

        [Fact]
        public void TestGetRowCounts_NonZero() {
            var rowCounts = GetRowCounts(Config.tables, "CT_testdb", 101);
            Assert.Equal(1, rowCounts["dbo.test1"]);
        }

        [Fact]
        public void TestGetRowCounts_Zero() {
            var rowCounts = GetRowCounts(Config.tables, "CT_testdb", 101);
            Assert.Equal(0, rowCounts["dbo.test2"]);
        }

        [Fact]
        public void TestResizeBatch_MaxBatchZero() {
            //test that it doesn't mess with batch size when maxBatchSize is 0
            Assert.Equal(1000, ResizeBatch(500, 1000, 1000, 0, null, null, new DateTime(2000, 1, 1, 12, 0, 0)));
        }

        [Fact]
        public void TestResizeBatch_NoThreshold() {
            //test the basic case with threshold times not set
            Assert.Equal(1000, ResizeBatch(500, 1500, 1500, 500, null, null, new DateTime(2000, 1, 1, 12, 0, 0)));
        }

        [Fact]
        public void TestResizeBatch_ThresholdNoMidnight_NotInThreshold() {
            //same case with threshold times set (not wrapping around midnight), when we are not in the ignore window
            Assert.Equal(1000, ResizeBatch(500, 1500, 1500, 500, new TimeSpan(1, 0, 0), new TimeSpan(3, 0, 0), new DateTime(2000, 1, 1, 12, 0, 0)));
        }

        [Fact]
        public void TestResizeBatch_ThresholdNoMidnight_InThreshold() {
            //threshold times set (not wrapping around midnight) and we are currently in the ignore window
            Assert.Equal(1500, ResizeBatch(500, 1500, 1500, 500, new TimeSpan(1, 0, 0), new TimeSpan(3, 0, 0), new DateTime(2000, 1, 1, 2, 0, 0)));
        }

        [Fact]
        public void TestResizeBatch_ThresholdMidnight_NotInThreshold() {
            //threshold time wraps around midnight and we are not in the ignore window
            Assert.Equal(1000, ResizeBatch(500, 1500, 1500, 500, new TimeSpan(23, 45, 0), new TimeSpan(1, 30, 0), new DateTime(2000, 1, 1, 12, 0, 0)));
        }

        [Fact]
        public void TestResizeBatch_ThresholdMidnight_InThresholdBeforeMidnight() {
            //threshold time wraps around midnight and we are in the ignore window (before midnight)
            Assert.Equal(1500, ResizeBatch(500, 1500, 1500, 500, new TimeSpan(23, 45, 0), new TimeSpan(1, 30, 0), new DateTime(2000, 1, 1, 23, 55, 0)));
        }

        [Fact]
        public void TestResizeBatch_ThresholdMidnight_InThresholdAfterMidnight() {
            //threshold time wraps around midnight and we are in the ignore window (after midnight)
            Assert.Equal(1500, ResizeBatch(500, 1500, 1500, 500, new TimeSpan(23, 45, 0), new TimeSpan(1, 30, 0), new DateTime(2000, 1, 1, 0, 30, 0)));
        }
    }

    public class MasterTestFixture {
        public TestDataUtils sourceDataUtils;
        public TestDataUtils destDataUtils;

        public MasterTestFixture() {
            var tables = new TableConf[2];
            tables[0] = new TableConf();
            tables[0].Name = "test1";
            tables[0].schemaName = "dbo";

            tables[1] = new TableConf();
            tables[1].Name = "test2";
            tables[1].schemaName = "dbo";

            sourceDataUtils = new TestDataUtils(TServer.MASTER);
            sourceDataUtils.testData = new DataSet();
            destDataUtils = new TestDataUtils(TServer.RELAY);
            destDataUtils.testData = new DataSet();

            Config.masterDB = "testdb";
            Config.masterCTDB = "CT_testdb";
            Config.relayDB = "CT_testdb";
            Config.logLevel = LogLevel.Critical;
            Config.tables = tables;
            Config.masterType = SqlFlavor.MSSQL;
            Config.relayType = SqlFlavor.MSSQL;
        }

        //TODO move this somewhere else
        public void TestData() {
            //this will resolve to something like "C:\tesla\TeslaSQL\bin\Debug"
            string baseDir = AppDomain.CurrentDomain.BaseDirectory;
            //relative path to the tests folder will be four directories up from that
            string filePath = baseDir + @"..\..\..\..\Tests\test1\input_data.xml";
            DataSet ds = new DataSet();
            ds.ReadXml(filePath, XmlReadMode.ReadSchema);
            //ds.AcceptChanges();
            filePath = baseDir + @"..\..\..\..\Tests\test1\expected_data.xml";
            DataSet expected = new DataSet();
            expected.ReadXml(filePath, XmlReadMode.ReadSchema);

            //expected.AcceptChanges();
            Console.WriteLine(TestDataUtils.CompareDataSets(expected, ds));
            Console.WriteLine("one hop this time");
            DataRow row = ds.Tables["tblCTSlaveVersion"].NewRow();
            row["CTID"] = 500;
            row["slaveIdentifier"] = "TESTSLAVE";
            row["syncStartVersion"] = 1000;
            row["syncStopVersion"] = 2000;
            row["syncStartTime"] = new DateTime(2012, 1, 1, 12, 0, 0);
            row["syncBitWise"] = 0;
            ds.Tables["tblCTSlaveVersion"].Rows.Add(row);

            Console.WriteLine(TestDataUtils.CompareDataSets(expected, ds));
            Console.ReadLine();
        }
    }
}
