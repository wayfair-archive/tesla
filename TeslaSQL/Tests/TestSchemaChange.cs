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
            Assert.True(one.Equals(two));

            two = new SchemaChange(1, SchemaChangeType.Drop, "sname", "tname", "cname", "ncname");

            Assert.False(one.Equals(two));
            two = new SchemaChange(1, SchemaChangeType.Add, "asdf", "tname", "cname", "ncname");

            Assert.False(one.Equals(two));
            two = new SchemaChange(1, SchemaChangeType.Add, "sname", "tname", "asdf", "ncname");
            Assert.False(one.Equals(two));
        }
    }
}
