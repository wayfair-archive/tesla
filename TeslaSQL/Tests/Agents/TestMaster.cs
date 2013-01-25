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
using Moq;

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
            SetFieldLists("testdb", Config.Tables, sourceDataUtils);
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
            PublishSchemaChanges(Config.Tables, "testdb", "CT_testdb", 101, new DateTime(2000, 1, 1));
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
            IDictionary<string, Int64> result = CreateChangeTables("testdb", "CT_testdb", new ChangeTrackingBatch(101, 1000, 2000, 0));
            Assert.Equal(1, result["dbo.test1"]);
            Assert.Equal(0, result["dbo.test2"]);
        }

        [Fact]
        public void TestOverrideZeroStartVersion() {
            var table = new TableConf();
            table.Name = "tableName";
            table.SchemaName = "schema";
            string db = "db";
            string ctdb = "ctdb";
            long minValidVersion = 5;
            var batch = new ChangeTrackingBatch(1, 0, 10, 0);

            var sourceUtils = new Mock<IDataUtils>();
            this.sourceDataUtils = sourceUtils.Object;
            sourceUtils.Setup((ut) => ut.GetMinValidVersion(db, table.Name, table.SchemaName))
                       .Returns(minValidVersion).Verifiable();
            sourceUtils.Setup((ut) => ut.CheckTableExists(db, table.Name, It.IsAny<string>()))
                        .Returns(true);
            sourceUtils.Setup((ut) => ut.HasPrimaryKey(db, table.Name, table.SchemaName))
                        .Returns(true);
            sourceUtils.Setup((ut) => ut.IsChangeTrackingEnabled(db, table.Name, table.SchemaName))
                        .Returns(true);

            CreateChangeTable(table, db, ctdb, batch);
            sourceUtils.Verify(
                (ut) => ut.SelectIntoCTTable(ctdb, It.IsAny<TableConf>(), db, It.IsAny<ChangeTrackingBatch>(), It.IsAny<int>(), minValidVersion)
                    );
        }
        [Fact]
        public void TestNoOverrideNonZeroStartVersion() {
            var table = new TableConf();
            table.Name = "tableName";
            table.SchemaName = "schmea";
            string db = "db";
            string ctdb = "ctdb";
            var batch = new ChangeTrackingBatch(1, 1, 10, 0);

            var sourceUtils = new Mock<IDataUtils>();
            this.sourceDataUtils = sourceUtils.Object;
            sourceUtils.Setup((ut) => ut.GetMinValidVersion(db, table.Name, table.SchemaName))
                       .Returns(5);
            sourceUtils.Setup((ut) => ut.CheckTableExists(db, table.Name, It.IsAny<string>()))
                        .Returns(true);
            sourceUtils.Setup((ut) => ut.HasPrimaryKey(db, table.Name, table.SchemaName))
                        .Returns(true);
            sourceUtils.Setup((ut) => ut.IsChangeTrackingEnabled(db, table.Name, table.SchemaName))
                        .Returns(true);
            sourceUtils.Setup((ut) => ut.IsBeingInitialized(ctdb, It.IsAny<TableConf>()))
                        .Returns(false);
            sourceUtils.Setup((ut) => ut.GetInitializeStartVersion(ctdb, It.IsAny<TableConf>()))
                        .Returns(new long?());

            CreateChangeTable(table, db, ctdb, batch);
            //sourceUtils.Verify(
            //    (ut) => ut.SelectIntoCTTable(It.IsAny<string>(), It.IsAny<TableConf>(), It.IsAny<string>(
            sourceUtils.Verify(
                (ut) => ut.SelectIntoCTTable(ctdb, It.IsAny<TableConf>(), db, It.IsAny<ChangeTrackingBatch>(), It.IsAny<int>(), batch.SyncStartVersion),
                Times.Never(),
                "Should not select into CT table when the start version is less than minValidVersion");
        }

        [Fact]
        public void TestPublishChangeTables() {
            PublishChangeTables("CT_testdb", "CT_testdb", 101, changesCaptured);
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
            PublishTableInfo(Config.Tables, "CT_testdb", changesCaptured, ctb.CTID);
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
            var rowCounts = GetRowCounts(Config.Tables, "CT_testdb", 101);
            Assert.Equal(1, rowCounts["dbo.test1"]);
        }

        [Fact]
        public void TestGetRowCounts_Zero() {
            var rowCounts = GetRowCounts(Config.Tables, "CT_testdb", 101);
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
            tables[0].SchemaName = "dbo";

            tables[1] = new TableConf();
            tables[1].Name = "test2";
            tables[1].SchemaName = "dbo";

            sourceDataUtils = new TestDataUtils(TServer.MASTER);
            sourceDataUtils.testData = new DataSet();
            destDataUtils = new TestDataUtils(TServer.RELAY);
            destDataUtils.testData = new DataSet();

            Config.MasterDB = "testdb";
            Config.MasterCTDB = "CT_testdb";
            Config.RelayDB = "CT_testdb";
            Config.Tables = tables.ToList();
            Config.MasterType = SqlFlavor.MSSQL;
            Config.RelayType = SqlFlavor.MSSQL;
        }
    }
}
