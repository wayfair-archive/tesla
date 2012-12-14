using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Xunit;
using TeslaSQL.Agents;
namespace TeslaSQL.Tests.Agents {
    public class TestAgent : TeslaSQL.Agents.Agent {

        public override void Run() {
            throw new NotImplementedException();
        }

        public override void ValidateConfig() {
            throw new NotImplementedException();
        }

        public TestAgent() {
            this.logger = new Logger(LogLevel.Critical, null, null, null);
        }

        [Fact]
        public void TestSetFieldList() {             
            TableConf t = new TableConf();
            t.columnList = new string[] { "col1", "col2", "col3", "col4" };
            var fields = new Dictionary<string, bool>{
                {"col1", true},
                {"col2", false},
                {"col3", false},

                {"col4", true}
            };
            var cm = new ColumnModifier();
            cm.type = "ShortenField";
            cm.length = 100;
            cm.columnName = "col1";
            t.columnModifiers = new ColumnModifier[] { cm };
            SetFieldList(t, fields);
            Assert.Equal("LEFT(CAST(P.col1 AS NVARCHAR(MAX)),100) as col1,P.col2,P.col3,CT.col4", t.masterColumnList);
            Assert.Equal("col1,col2,col3,col4", t.slaveColumnList);
            Assert.Equal("P.col1 = CT.col1 AND P.col4 = CT.col4", t.pkList);
            Assert.Equal("P.col1 IS NOT NULL AND P.col4 IS NOT NULL", t.notNullPKList);
            Assert.Equal("P.col2=CT.col2,P.col3=CT.col3", t.mergeUpdateList);

        }
    }
}
