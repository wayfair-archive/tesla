using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using TeslaSQL;
using Xunit;

namespace TeslaSQL.Tests {
    public class TestSchemaChange {
        [Fact]
        public void TestEquals() {
            SchemaChange one = new SchemaChange(1, SchemaChangeType.Add, "sname", "tname", "cname", "ncname");
            SchemaChange two = new SchemaChange(1, SchemaChangeType.Add, "sname", "tname", "cname", "ncname");

            two.EventType = SchemaChangeType.Drop;
            Assert.False(one.Equals(two));
            two.EventType = SchemaChangeType.Add;
            two.SchemaName = "asdf";

            Assert.False(one.Equals(two));
            two.SchemaName = "sname";

            Assert.True(one.Equals(two));
            two.ColumnName = "asdf";
            Assert.False(one.Equals(two));
            two.ColumnName = "cname";
        }
    }
}
