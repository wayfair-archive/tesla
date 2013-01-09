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
    public class TestMasterMaintenance : MasterMaintenance {
        [Fact]
        public void TestTableDeletions() {
            Config.masterCTDB = "alksdjf";
            Config.changeRetentionHours = 10;
            var dataUtils = new Mock<IDataUtils>();
            var intsToDel = new List<int> { 3, 4, 5 };
            var tables = new List<TTable>{
                new TTable("tblCTtblTest_3", "dbo"),
                new TTable("tblCTtblTest_4", "dbo"),
                new TTable("tblCTtblTest_5", "dbo"),
                new TTable("tblCTtblTest_6", "dbo"),
                new TTable("tblCTTableInfo_3", "dbo"),
                new TTable("tblCTVersion", "dbo"),
            };
            var tablesToDel = tables.Where(
                t => t.name.Contains('_') && intsToDel.Contains(int.Parse(t.name.Substring(t.name.LastIndexOf('_') + 1)))
                    );
            dataUtils.Setup(
                du => du.GetOldCTIDs(It.IsAny<string>(), It.IsAny<DateTime>(), It.IsAny<AgentType>()))
                .Returns(intsToDel);
            dataUtils.Setup(
                du => du.GetTables(It.IsAny<string>()))
                .Returns(tables);

            this.sourceDataUtils = dataUtils.Object;
            Run();
            foreach (var otherT in tablesToDel) {
                dataUtils.Verify(
                    du => du.DropTableIfExists(
                        Config.masterCTDB, otherT.name, otherT.schema)
                        );
            }
            
        }
    }
}
