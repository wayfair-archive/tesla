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
        public void SetFixture(MasterTestFixture fixture) {
            this.sourceDataUtils = fixture.sourceDataUtils;
            this.destDataUtils = fixture.destDataUtils;
            this.config = fixture.config;
            //svar table = ((TestDataUtils)sourceDataUtils).testData.Tables[1];
        }

        [Fact]
        public void TestGetRowCounts_NonZero() {
            var rowCounts = GetRowCounts(config.tables, "CT_testdb", 1);
            Assert.Equal(1, rowCounts["dbo.test1"]);
        }

        [Fact]
        public void TestGetRowCounts_Zero() {
            var rowCounts = GetRowCounts(config.tables, "CT_testdb", 1);
            Assert.Equal(0, rowCounts["dbo.test2"]);
        }

        //unit tests for ResizeBatch method
        [Fact]
        public void TestResizeBatch() {
            //test that it doesn't mess with batch size when maxBatchSize is 0
            Assert.Equal(1000, ResizeBatch(500, 1000, 1000, 0, null, null, new DateTime(2000, 1, 1, 12, 0, 0)));

            //test the basic case with threshold times not set
            Assert.Equal(1000, ResizeBatch(500, 1500, 1500, 500, null, null, new DateTime(2000, 1, 1, 12, 0, 0)));

            //same case with threshold times set (not wrapping around midnight), when we are not in the ignore window
            Assert.Equal(1000, ResizeBatch(500, 1500, 1500, 500, new TimeSpan(1, 0, 0), new TimeSpan(3, 0, 0), new DateTime(2000, 1, 1, 12, 0, 0)));

            //threshold times set (not wrapping around midnight) and we are currently in the ignore window
            Assert.Equal(1500, ResizeBatch(500, 1500, 1500, 500, new TimeSpan(1, 0, 0), new TimeSpan(3, 0, 0), new DateTime(2000, 1, 1, 2, 0, 0)));

            //threshold time wraps around midnight and we are not in the ignore window
            Assert.Equal(1000, ResizeBatch(500, 1500, 1500, 500, new TimeSpan(23, 45, 0), new TimeSpan(1, 30, 0), new DateTime(2000, 1, 1, 12, 0, 0)));

            //threshold time wraps around midnight and we are in the ignore window (before midnight)
            Assert.Equal(1500, ResizeBatch(500, 1500, 1500, 500, new TimeSpan(23, 45, 0), new TimeSpan(1, 30, 0), new DateTime(2000, 1, 1, 23, 55, 0)));

            //threshold time wraps around midnight and we are in the ignore window (after midnight)
            Assert.Equal(1500, ResizeBatch(500, 1500, 1500, 500, new TimeSpan(23, 45, 0), new TimeSpan(1, 30, 0), new DateTime(2000, 1, 1, 0, 30, 0)));
        }        
    }

    public class MasterTestFixture {
        public TestDataUtils sourceDataUtils;
        public TestDataUtils destDataUtils;
        public Config config;

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

            config = new Config();
            config.masterDB = "testdb";
            config.logLevel = LogLevel.Critical;
            config.tables = tables;

            sourceDataUtils.DropTableIfExists("CT_testdb", "tblCTtest1_1", "dbo");
            DataTable test1 = new DataTable("dbo.tblCTtest1_1", "MASTER.CT_testdb");
            test1.Columns.Add("column1", typeof(Int32));
            DataRow row = test1.NewRow();
            row["column1"] = 1;
            test1.Rows.Add(row);

            sourceDataUtils.DropTableIfExists("CT_testdb", "tblCTtest2_1", "dbo");
            DataTable test2 = new DataTable("dbo.tblCTtest2_1", "MASTER.CT_testdb");
            test2.Columns.Add("column1", typeof(Int32));

            sourceDataUtils.testData.Tables.Add(test1);
            sourceDataUtils.testData.Tables.Add(test2);
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
